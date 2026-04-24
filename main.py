import logging
from ETLs.malware_etl import run_malware_pipeline
from ETLs.vulnerabilities_etl import run_vulnerabilities_pipeline

logging.basicConfig(level=logging.INFO, format='%(asctime)s - [MAIN] - %(message)s')

if __name__ == "__main__":
    logging.info("Starting security data platform execution...")

    logging.info("--- Checking CISA Vulnerabilities ---")
    run_vulnerabilities_pipeline()

    logging.info("--- Checking URLhaus Malware Tracker ---")
    run_malware_pipeline()

    logging.info("Pipelines executed successfully")
