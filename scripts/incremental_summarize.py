from pathlib import Path
import logging
import datetime
import time

import pandas as pd

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
streamhandler = logging.StreamHandler()
streamhandler.setLevel(logging.DEBUG)
logger.addHandler(streamhandler)


def main():
    """ Summarize today's listings """
    start = time.time()
    filepath = Path(__file__).parent / Path("../output/listings.csv.gz")
    output = Path(__file__).parent / Path("../output/per_day_summary.csv")
    today = datetime.datetime.now().strftime("%Y-%m-%d")
    
    logger.info(f"Summarizing on {today}...")
    logger.info("Loading Redfin listings for today...")
    df = (
        pd.read_csv(filepath, parse_dates=['date'])
        .query("date == @today")
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
            'overpriced': ['mean', 'median']
        })
    )
    logger.info("Flattening columns...")
    summary_df.columns = ['{}_{}'.format(c[0], c[1]) for c in summary_df.columns]
    
    logger.info("Appending summary to disk...")
    summary_df.to_csv(output, mode='a', header=not output.exists())
    
    duration = time.time() - start
    logger.info(f"Completed in {duration} seconds")

    
if __name__ == "__main__":
    main()
