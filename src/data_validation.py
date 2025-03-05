import pandas as pd

def validate_data():
    df = pd.read_csv('/opt/airflow/src/raw_data.csv')
    if df.isnull().sum().sum() > 0:
        raise ValueError("Data contains missing values!")
    print("Data validated successfully!")
