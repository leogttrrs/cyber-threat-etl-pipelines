# CISA KEV ETL Pipeline 🛡️

An end-to-end Data Pipeline (ETL) developed in Python to extract, transform, and store data regarding actively exploited cybersecurity vulnerabilities worldwide.

## About the Project
This project was built to practice Data Engineering skills. The pipeline consumes real-time data from the official Known Exploited Vulnerabilities (KEV) catalog provided by the U.S. Cybersecurity and Infrastructure Security Agency (CISA).

## Architecture (Data Flow)
1. **Extract:** HTTP request (via `requests` library) to CISA's public API, extracting the complete catalog of CVEs in JSON format.
2. **Transform:** Unstructured data processing using `pandas`. Transformations include:
   - Filtering and selecting relevant analytical columns (CVE ID, Vendor/Project, Description).
   - Data type conversion and standardization (`datetime` parsing).
   - Handling null/missing values to ensure data integrity.
3. **Load:** Loading the cleaned *DataFrame* into a **PostgreSQL** cloud database (Neon), automatically generating the table schema for analytical SQL queries.

## Tech Stack
* **Language:** Python 3
* **Data Manipulation:** Pandas
* **Database:** PostgreSQL (Neon) & SQLAlchemy
* **REST Requests:** Requests
* **Environment Management:** python-dotenv

## How to Run
Clone the repository and install the dependencies:

```bash
git clone https://github.com/leogttrrs/CISA-KEV-ETL.git
cd cisa-kev-etl
pip install -r requirements.txt