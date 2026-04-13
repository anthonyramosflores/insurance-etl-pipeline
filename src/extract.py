import pandas as pd
import os

RAW_DATA_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'claims_raw.csv')

def extract_data() -> pd.DataFrame:
    print("Extracting data from CSV...")

    df = pd.read_csv(RAW_DATA_PATH)

    print(f"Extracted {len(df)} rows and {len(df.columns)} columns.")
    print(f"Columns: {list(df.columns)}")

    return df
if __name__ == "__main__":
    df = extract_data()
    print(df.head())