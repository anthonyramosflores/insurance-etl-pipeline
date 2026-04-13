# Insurance Claims ETL Pipeline

An end-to-end data engineering pipeline that extracts raw insurance claims data,
transforms and cleans it, and loads it into Google BigQuery for analysis.

## Architecture
Raw CSV → Extract (Python/pandas) → Transform (cleaning, type casting) → Load (BigQuery)

## Technologies
- Python 3.9
- pandas
- Google BigQuery
- google-cloud-bigquery
- python-dotenv

## Dataset
Download the dataset from Kaggle: https://www.kaggle.com/datasets/buntyshah/auto-insurance-claims-data
Place the CSV in the `data/` folder as `claims_raw.csv`.

## Setup

1. Clone the repo
git clone https://github.com/anthonyramosflores/insurance-etl-pipeline.git
cd insurance-etl-pipeline

2. Install dependencies
pip3 install -r requirements.txt

3. Set up Google Cloud
- Create a Google Cloud project
- Enable the BigQuery API
- Create a service account with BigQuery Admin role
- Download the credentials JSON and place it in the project root as `credentials.json`

4. Configure environment variables
Create a `.env` file in the project root:
GOOGLE_PROJECT_ID=your-project-id
GOOGLE_CREDENTIALS_PATH=credentials.json
BQ_DATASET=insurance_data
BQ_TABLE=claims

5. Run the pipeline
python3 src/load.py

## Project Structure
insurance-etl-pipeline/
├── data/                  # Raw data (not committed)
├── src/
│   ├── extract.py         # Reads CSV into pandas DataFrame
│   ├── transform.py       # Cleans and transforms data
│   └── load.py            # Loads data into BigQuery
├── sql/
│   └── create_tables.sql  # BigQuery schema definition
├── .env                   # Credentials (not committed)
├── requirements.txt
└── README.md