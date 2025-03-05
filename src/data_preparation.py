import pandas as pd

def prepare_data():
    df = pd.read_csv('/opt/airflow/src/raw_data.csv')
    # Remove rows with missing essential columns
    df = df.dropna(subset=['customerID', 'Churn'])
    df['TotalCharges'] = pd.to_numeric(df['TotalCharges'], errors='coerce')
    df.to_csv('/opt/airflow/src/cleaned_data.csv', index=False)
    print("Data prepared successfully!")
