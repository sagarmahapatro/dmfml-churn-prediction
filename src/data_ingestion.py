import pandas as pd

def download_data():
    url = "https://raw.githubusercontent.com/datasciencedojo/datasets/master/telco-churn-data/telco_churn.csv"
    df = pd.read_csv(url)
    df.to_csv('/opt/airflow/src/raw_data.csv', index=False)
    print("Data downloaded successfully!")

