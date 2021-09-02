from unittest.mock import patch

from requests import Response
import pandas as pd

from yellowtail import Agent
from yellowtail.agent import gen_cols

@patch('yellowtail.Agent.pull_listings')
def test_pull_listings(mock_pull):
    agent = Agent()
    
    mockresponse = Response()
    mockresponse.status_code = 200
    mock_pull.return_value = mockresponse
    
    response = agent.pull_listings()
    assert response.status_code == 200
    

@patch('yellowtail.Agent.pull_listings')
def test_digest_listings(mock_pull_listings):
    agent = Agent()
    
    columns_to_mock = gen_cols()
    mock_csv = pd.DataFrame({c: ['foo'] for c in columns_to_mock}).to_csv(index=False)
    mock_response = Response()
    mock_response._content = str.encode(mock_csv)
    mock_response.status_code = 200
    mock_pull_listings.return_value = mock_response
    
    download = agent.pull_listings()
    df = agent.digest_listings(download)
    
    assert all(c in df.columns for c in columns_to_mock)
    assert 'full_address' in df.columns
    
# Best ways to mock these functions?
# test agent.pull_details
# test query_redfin_dask
# test agent.process_redfin_response

def test_digest_details():
    agent = Agent()
    df = pd.DataFrame({
        'ADDRESS': ['a', 'b'],
        'CITY': ['a', 'b'],
        'STATE OR PROVINCE': ['a', 'b'],
        'ZIP OR POSTAL CODE': ['a', 'b'],
        'PRICE': ['a', 'b'],
        'full_address': ['a', 'b']
    })
    prices = pd.Series([
        {'a': 100},
        {'b': 42}
    ], name='tax_assessed_value')
    digest = agent.digest_details(df, prices)
    
    assert isinstance(digest, pd.DataFrame)
    assert 'tax_assessed_value' in digest.columns
    assert 'date' in digest.columns
    
    
def test_compile_results():
    agent = Agent()
    
    compiled = agent.compile_results([
        {'a': 100},
        {'b': 42}
    ])
    assert isinstance(compiled, pd.Series)
    
def test_full_runthrough():
    agent = Agent()

    download = agent.pull_listings()

    digest = agent.digest_listings(download)

    details = agent.pull_details(digest['full_address'].to_list())

    processed_df = agent.digest_details(digest, details)