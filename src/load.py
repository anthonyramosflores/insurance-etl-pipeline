import os
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
from dotenv import load_dotenv

load_dotenv()

PROJECT_ID = os.getenv("GOOGLE_PROJECT_ID")
CREDENTIALS_PATH = os.getenv("GOOGLE_CREDENTIALS_PATH")
DATASET = os.getenv("BQ_DATASET")
TABLE = os.getenv("BQ_TABLE")


def get_client():
    credentials = service_account.Credentials.from_service_account_file(
        CREDENTIALS_PATH,
        scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )
    return bigquery.Client(project=PROJECT_ID, credentials=credentials)


def create_dataset_if_not_exists(client):
    dataset_ref = bigquery.Dataset(f"{PROJECT_ID}.{DATASET}")
    dataset_ref.location = "US"
    try:
        client.get_dataset(dataset_ref)
        print(f"Dataset {DATASET} already exists.")
    except Exception:
        client.create_dataset(dataset_ref)
        print(f"Created dataset {DATASET}.")


def load_to_bigquery(df: pd.DataFrame):
    client = get_client()
    create_dataset_if_not_exists(client)

    destination = f"{PROJECT_ID}.{DATASET}.{TABLE}"

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        autodetect=True,
    )

    job = client.load_table_from_dataframe(df, destination, job_config=job_config)
    job.result()

    table = client.get_table(destination)
    print(f"Loaded {table.num_rows} rows into {destination}.")


if __name__ == "__main__":
    from extract import extract_data
    from transform import transform_data

    df_raw = extract_data()
    df_clean = transform_data(df_raw)
    load_to_bigquery(df_clean)