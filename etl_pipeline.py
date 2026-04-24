import logging
import os
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')


if __name__ == "__main__":
    load_dotenv()

    API_URL = "https://www.cisa.gov/sites/default/files/feeds/known_exploited_vulnerabilities.json"

    DATABASE_URL = os.getenv("DATABASE_URL")
    TABLE_NAME = "active_vulnerabilities"

    if not DATABASE_URL:
        logging.error("DATABASE_URL not found. Verify '.env' file.")
    else:
        try:
            raw_data = extract_data(API_URL)
            clean_data = transform_data(raw_data)
            load_data(clean_data, DATABASE_URL, TABLE_NAME)

            logging.info("Pipeline ETL sucessfully completed!")

        except Exception as e:
            logging.error(f"Error in pipeline execution: {e}")