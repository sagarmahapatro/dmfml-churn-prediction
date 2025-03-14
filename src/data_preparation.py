import pandas as pd

def prepare_data():
    df_traning = pd.read_csv('/opt/airflow/storage/processed/staging/customer_churn_dataset_traning.csv')
    # Remove rows with missing essential columns
    df_traning = df_traning.dropna(subset=['CustomerID', 'Churn'])
    ##df['TotalCharges'] = pd.to_numeric(df['TotalCharges'], errors='coerce')
    df_traning.to_csv('/opt/airflow/storage/processed/staging/customer_churn_dataset_traning_cleaned_data.csv', index=False)
    
    df_testing = pd.read_csv('/opt/airflow/storage/processed/staging/customer_churn_dataset_testing.csv')
    # Remove rows with missing essential columns
    df_testing = df_testing.dropna(subset=['CustomerID', 'Churn'])
    ##df['TotalCharges'] = pd.to_numeric(df['TotalCharges'], errors='coerce')
    df_testing.to_csv('/opt/airflow/storage/processed/staging/customer_churn_dataset_testing_cleaned_data.csv', index=False)
    print("Data prepared successfully!")
