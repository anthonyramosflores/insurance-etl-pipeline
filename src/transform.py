import pandas as pd
from extract import extract_data

def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    print("Transforming data...")

    # drop junk column
    df = df.drop(columns=['_c39'])

    # rename columns with hyphens
    df = df.rename(columns={
        'capital-gains' : 'capital_gains',
        'capital-loss' : 'capital_loss'
    })

    # convert data strings to actual dates
    df['policy_bind_date'] = pd.to_datetime(df['policy_bind_date'])
    df['incident_date'] = pd.to_datetime(df['incident_date'])

    # convert fraud_reported Y/N to boolean
    df['fraud_reported'] = df['fraud_reported'].map({'Y': True, 'N': False})

    # strip whitespace from all string columns
    str_cols = df.select_dtypes(include='object').columns
    df[str_cols] = df[str_cols].apply(lambda x: x.str.strip())

    # drop duplicates
    df = df.drop_duplicates()

    # reset index
    df = df.reset_index(drop=True)

    print(f"Transformed data: {len(df)} rows, {len(df.columns)} columns.")
    print(f"Null counts:\n{df.isnull().sum()[df.isnull().sum() > 0]}")

    return df

if __name__ == "__main__":
    df = extract_data()
    df_clean = transform_data(df)
    print(df_clean.head())
    print(df_clean.dtypes)
