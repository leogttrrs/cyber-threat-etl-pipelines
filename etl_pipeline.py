import requests
import pandas as pd
import logging
import os
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

def extract_data(url: str) -> dict:
    logging.info("Starting data extraction (Extract)...")
    response = requests.get(url)
    response.raise_for_status()
    return response.json()

def transform_data(json_data: dict) -> pd.DataFrame:
    logging.info("Starting data transformation (Transform)...")

    vulnerabilities = json_data.get("vulnerabilities", [])
    df = pd.DataFrame(vulnerabilities)

    important_columns = [
        'cveID',
        'vendorProject',
        'product',
        'vulnerabilityName',
        'dateAdded',
        'shortDescription'
    ]

    df = df[important_columns]

    df['dateAdded'] = pd.to_datetime(df['dateAdded'])

    df.fillna("Information not available", inplace=True)
    logging.info(f"Transformation completed. Total processed registers: {len(df)}")
    return df

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