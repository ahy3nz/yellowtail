import json
from pathlib import Path
import itertools as it
import io
import datetime
import logging
import time

import requests
import pandas as pd
from dask import delayed
from dask.distributed import Client, as_completed, wait, LocalCluster
from redfin import Redfin

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
streamhandler = logging.StreamHandler()
streamhandler.setLevel(logging.DEBUG)
logger.addHandler(streamhandler)

request_headers = {
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng.*/*;q=0.8',
    'accept-encoding':'gzip, deflate, br',
    'accept-language': 'en-US,en;q=0.8',
    'upgrade-insecure-requests': '1',
    'user-agent': 'Chrome/92.0.4515.130'
}

relevant_columns = [
    'ADDRESS', 'CITY', 'STATE OR PROVINCE', 'ZIP OR POSTAL CODE', "PRICE", 
    "tax_assessed_value", 'overpriced', 'date'
]

def main():
    start = time.time()
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d")
    logger.info(f"Initializing on {timestamp}...")

    logger.info("Initializing dask and redfin clients...")
    daskclient = Client()
    redfinclient = Redfin()

    with requests.Session() as session:
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
        logger.info("Downloading list of redfin properties within parameters...")
        download = session.get(
            'https://www.redfin.com/stingray/api/gis-csv?', params=params,
            headers=request_headers
        )
    logger.info("Transforming into table of listings ...")
    df = (
        pd.read_csv(io.StringIO(download.content.decode("utf-8")))
        .assign(full_address=lambda x: x.apply(stitch_full_address, axis=1))
    )

    with requests.Session() as session:
        logger.info("Accessing below-the-fold listing information...")
        session_scattered = daskclient.scatter(session)
        redfinclient_scattered = daskclient.scatter(redfinclient)
        futures = [
            delayed(query_redfin_dask)(session_scattered, 
                redfinclient_scattered, address, headers=request_headers)
            for address in df['full_address'].to_list()
        ]
        futures = daskclient.compute(futures)
        completed_results = [
                result for a, result in 
                as_completed(futures, raise_errors=False, with_results=True)
        ]
    logger.info("Compiling all listing information...")
    processed_df = (
        df
        .merge(
            compile_results(completed_results), 
            left_on='full_address', right_index=True
        ).assign(
            overpriced=lambda x: x['PRICE'] - x['tax_assessed_value'],
            date=timestamp
        )
    )

    logger.info("Filtering for positive tax-assessed values...")
    final_df = (
        processed_df
        .query('tax_assessed_value > 0')
        [relevant_columns]
    )

    logger.info("Dumping data to disk ...")
    path_to_output = Path(__file__).parent / Path("../output/listings.csv")
    final_df.to_csv(
            path_to_output, index=False, mode='a',
            header = not path_to_output.exists()
    )

    duration = time.time() - start
    daskclient.close()
    logger.info(f"Scraping finished in {duration:.3f} seconds")

def stitch_full_address(row):
    """ Given a house record from redfin, generate a full address for future querying """
    return row['ADDRESS'] + ', ' + row['CITY'] + " " + row['STATE OR PROVINCE']


def process_redfin_response(response, redfinclient):
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
                    pd.DataFrame(info['payload']['publicRecordsInfo']['allTaxInfo'])
                    .sort_values("rollYear", ascending=False)
                ).iloc[0]
                return tax_assessment.get('taxableLandValue', 0) + tax_assessment.get('taxableImprovementValue', 0)
            else:
                return -1
        else:
            return -1


def query_redfin_dask(session, redfinclient, address, headers=None, **kwargs):
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
        headers=headers
    )
    return {address: process_redfin_response(response, redfinclient)}


def compile_results(results):
    """ Aggregate the results from all the redfin requests into a single series

    Take a list of dictionaries (from the dask future objects), flatten them into one
    dictionary, then turn into a pandas series
    """
    compiled = pd.Series(dict(it.chain.from_iterable(a.items() for a in results)), name='tax_assessed_value')

    return compiled

if __name__ == "__main__":
    main()

