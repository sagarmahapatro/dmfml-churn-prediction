import pandas as pd

def transform_data():
    df = pd.read_csv('/opt/airflow/src/cleaned_data.csv')
    # Transform categorical columns (e.g., gender, Churn)
    df['gender'] = df['gender'].map({'Male': 1, 'Female': 0})
    df['Churn'] = df['Churn'].map({'Yes': 1, 'No': 0})
    df.to_csv('/opt/airflow/src/transformed_data.csv', index=False)
    print("Data transformed successfully!")
