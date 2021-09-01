from pathlib import Path
import logging
import datetime
import time

import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
streamhandler = logging.StreamHandler()
streamhandler.setLevel(logging.DEBUG)
logger.addHandler(streamhandler)


def main():
    """ Take entire raw listing information and summarize by day """
    start = time.time()
    filepath = Path(__file__).parent / Path("../output/listings.csv.gz")
    output = Path(__file__).parent / Path("../output/per_day_summary.csv")
    today = datetime.datetime.now().strftime("%Y-%m-%d")
    
    logger.info(f"Summarizing on {today}...")
    logger.info("Loading all Redfin listings...")
    df = (
        pd.read_csv(filepath, parse_dates=['date'])
        .query('tax_assessed_value > 0')
        .assign(overpriced=lambda x: x['PRICE'] - x['tax_assessed_value'])
    )
    logger.info("Summarizing listings...")
    summary_df = (
        df
        .groupby('date')
        .agg({
            'ADDRESS': 'count',
            'PRICE': ['mean', 'median'],
            'tax_assessed_value': ['mean', 'median'],
            'overpriced': [adjusted_mean, adjusted_median]
        })
    )
    logger.info("Flattening columns...")
    summary_df.columns = ['{}_{}'.format(c[0], c[1]) for c in summary_df.columns]
    
    logger.info("Writing summary to disk...")
    summary_df.to_csv(output)
    
    duration = time.time() - start
    logger.info(f"Completed in {duration} seconds")
    
    
def adjusted_mean(grouped, threshold=200_000):
    """ Compute mean overpriced amount excluding new-builds,
    where new-builds are extremely overpriced relative to 
    out-of-date tax-assessed values
    """
    overpriced_amounts = [val for val in grouped if val < threshold]
    return np.mean(overpriced_amounts)


def adjusted_median(grouped, threshold=200_000):
    """ Compute median overpriced amount excluding new-builds,
    where new-builds are extremely overpriced relative to 
    out-of-date tax-assessed values
    """
    overpriced_amounts = [val for val in grouped if val < threshold]
    return np.median(overpriced_amounts)


    
if __name__ == "__main__":
    main()