from dataclasses import dataclass, field
import datetime
import io
import itertools as it
import json
from typing import Dict, Union, List

import pandas as pd
import requests
from redfin import Redfin
from dask import delayed
from dask.distributed import Client, as_completed


REDFIN_ENDPOINT='https://www.redfin.com/stingray/api/gis-csv?'


def gen_headers():
    """ Request headers"""
    headers = {
        'authority': 'www.redfin.com',
        'content-length': '0',
        'sec-ch-ua': '"Chromium";v="92", " Not A;Brand";v="99", "Google Chrome";v="92"',
        'sec-ch-ua-mobile': '?0',
        'user-agent': 'Mozilla/5.0 (X11; CrOS x86_64 13982.82.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.157 Safari/537.36',
        'content-type': 'text/plain;charset=UTF-8',
        'accept': '*/*',
        'origin': 'https://www.redfin.com',
        'sec-fetch-site': 'same-origin',
        'sec-fetch-mode': 'no-cors',
        'sec-fetch-dest': 'empty',
        'referer': 'https://www.redfin.com/city/12839/DC/Washington-DC',
        'accept-language': 'en-US,en;q=0.9',
    }
    return headers 


def gen_params():
    """ Redfin search parameters"""
    params = {
        'al': 1,
        'hoa': 150,
        'market': 'dc',
        'max_listing_approx_size': 3000,
        'min_listing_approx_size': 1700,
        'max_num_beds': 4,
        'max_price': 800_000,
        'num_baths': 2,
        'num_beds': 2,
        'num_homes': 450,
        'page_number': 1,
        'region_id': 2965,
        'region_type': 5,
        'sf': '1,2,3,5,6,7',
        'status': 9,
        'uipt': '1,2,3,4,5,6,7,8',
        'v': 8
    }
    return params


def gen_cols():
    """ Columns to keep from Redfin listings"""
    relevant_columns = [
        'ADDRESS', 'CITY', 'STATE OR PROVINCE', 'ZIP OR POSTAL CODE', "PRICE", 
    ]
    return relevant_columns


def gen_final_cols():
    final_columns = [
        'ADDRESS', 'CITY', 'STATE OR PROVINCE', 'ZIP OR POSTAL CODE', "PRICE", 
        'tax_assessed_value', 'date'
    ]
    return final_columns


@dataclass
class Agent:
    """ Stores Redfin query parameters, runs Redfin
    query, digests output"""
    request_headers: Dict[str, str] = field(default_factory=gen_headers)
    redfin_query_params: Dict[str, Union[int, str]] = field(default_factory=gen_params)
    keep_cols: List[str] = field(default_factory=gen_cols)
    final_cols: List[str] = field(default_factory=gen_final_cols)
        
        
    def pull_listings(self):
        """ Query redfin for listings"""
        with requests.Session() as session:
            session.headers.update(self.request_headers)
            download = session.get(
                REDFIN_ENDPOINT, 
                params=self.redfin_query_params,
            )
        return download
            
        
    def digest_listings(self, download: requests.Response):
        """ Convert get request into dataframe"""
        if download.status_code != 200:
            raise RuntimeError(
                "Error making listings request: " +
                f"{download.content.decode('UTF-8')}"
            )
        df = pd.read_csv(
            io.StringIO(download.content.decode("utf-8")),
            low_memory=False, error_bad_lines=False
        )
        
        missing_cols = [c for c in self.keep_cols if c not in df.columns]
        if len(missing_cols) > 0:
            raise RuntimeError(
                f"Redfin listings missing {len(missing_cols)} " +
                f"columns: {missing_cols}"
            )
        
        return df[self.keep_cols].assign(
            full_address=lambda x: x['ADDRESS'] + ', ' + x['CITY'] 
                + ' ' + x['STATE OR PROVINCE']
        )

    
    def pull_details(self, list_of_addresses: List[str]):
        """ Query for below-the-fold information"""
        daskclient = Client()
        redfinclient = Redfin()
        with requests.Session() as session:
            session.headers.update(self.request_headers)
            session_scattered = daskclient.scatter(session)
            redfinclient_scattered = daskclient.scatter(redfinclient)
            futures = [
                delayed(self.query_redfin_dask)(session_scattered, 
                    redfinclient_scattered, address)
                for address in list_of_addresses
            ]
            futures = daskclient.compute(futures)
            completed_results = [
                result for a, result in 
                as_completed(futures, raise_errors=False, with_results=True)
            ]
        daskclient.close() 
        
        return completed_results
    
    
    def digest_details(
        self, 
        df: pd.DataFrame, 
        completed_results: List[Dict[str, float]]
    ):
        """ Process below-the-fold responses into dataframe"""
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d")
        processed_df = (
            df.merge(
                self.compile_results(completed_results), 
                left_on='full_address', right_index=True
            ).assign(date=timestamp) 
        )
        
        return processed_df[self.final_cols]
        
        
    def query_redfin_dask(
        self, 
        session: requests.Session, 
        redfinclient: Redfin, 
        address: str, 
        **kwargs
    ):
        """ For a given address, query redfin and identify tax-assessed value

        This is the function we submit to the dask client
        """
        response = session.get(
            'https://redfin.com/stingray/do/location-autocomplete',
            params={
                'location': address,
                'v': 2,
                **kwargs
            },
        )
        return {address: self.process_redfin_response(response, redfinclient)}
    
    
    def process_redfin_response(
        self, 
        response: requests.Response, 
        redfinclient: Redfin
    ):
        """ Given a response from redfin API, return the tax-assessed value

        Notes
        -----
        This can get messy because this response is deeply-nested JSON, and
        there are many chances for us to fail at pulling tax values.
        In all the places where things can go wrong, I do a very sloppy check and
        then return -1 if something broke
        """
        if response.status_code != 200:
            return -1
        else:
            resp_dict = json.loads(response.text[4:])
            if (
                (resp_dict.get('errorMessage', None) == 'Success') &
                ('exactMatch' in resp_dict['payload'])
            ):
                # Pull property metadata
                url = resp_dict['payload']['exactMatch']['url']

                data = redfinclient.initial_info(url)['payload']
                if data['responseCode'] != 200:
                    return -1
                property_id = data['propertyId']
                listing_id = data['listingId']
                info = redfinclient.below_the_fold(property_id)
                # Pull latest tax-assessed value
                if len(info['payload']['publicRecordsInfo']['allTaxInfo']) > 0:
                    tax_assessment = (
                        pd.DataFrame(
                            info['payload']['publicRecordsInfo']['allTaxInfo']
                        ) .sort_values("rollYear", ascending=False)
                    ).iloc[0]
                    return (
                        tax_assessment.get('taxableLandValue', 0) + 
                        tax_assessment.get('taxableImprovementValue', 0)
                    )
                else:
                    return -1
            else:
                return -1

            
    def compile_results(self, results: List[Dict[str, float]]):
        """ Aggregate the results from all the redfin requests into a single series

        Take a list of dictionaries (from the dask future objects), 
        flatten them into one dictionary, then turn into a pandas series
        """
        compiled = pd.Series(
            dict(it.chain.from_iterable(a.items() for a in results)), 
            name='tax_assessed_value'
        )

        return compiled
