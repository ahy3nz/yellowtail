from pathlib import Path
import datetime
import logging
import time


from yellowtail import Agent

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
streamhandler = logging.StreamHandler()
streamhandler.setLevel(logging.DEBUG)
logger.addHandler(streamhandler)


def main():
    """ Take a snapshot of Redfin listings """
    start = time.time()
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d")
    logger.info(f"Initializing on {timestamp}...")

    logger.info("Initializing yellowtail agent...")
    agent = Agent()

    logger.info("Downloading list of redfin properties within parameters...")
    download = agent.pull_listings()

    logger.info("Transforming into table of listings ...")
    digest = agent.digest_listings(download)

    logger.info("Accessing below-the-fold listing information...")
    details = agent.pull_details(digest['full_address'].to_list())

    logger.info("Compiling all listing information...")
    processed_df = agent.digest_details(digest, details)

    logger.info("Filtering for positive tax-assessed values...")
    final_df = (
        processed_df
        .query('tax_assessed_value > 0')
        [agent.keep_cols]
    )

    logger.info("Dumping data to disk ...")
    path_to_output = Path(__file__).parent / Path("../output/listings.csv.gz")
    final_df.to_csv(
        path_to_output, index=False, mode='a',
        header=not path_to_output.exists(),
        compression='gzip'
    )

    duration = time.time() - start
    daskclient.close()
    logger.info(f"Scraping finished in {duration:.3f} seconds")


if __name__ == "__main__":
    main()

