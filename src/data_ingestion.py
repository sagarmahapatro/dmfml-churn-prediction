import pandas as pd
import os

def load_data_to_stagging():
    #url = "https://raw.githubusercontent.com/datasciencedojo/datasets/master/telco-churn-data/telco_churn.csv"
    raw_data_path = "/opt/airflow/storage/raw_data/"
    stagging_data_path = "/opt/airflow/storage/processed/staging/"
    os.makedirs(os.path.dirname(stagging_data_path), exist_ok=True)
    raw_data_path_traing =  raw_data_path + "customer_churn_dataset-training-master.csv"
    stagging_data_path_traing = stagging_data_path+"customer_churn_dataset_traning.csv"
    df = pd.read_csv(raw_data_path_traing)
    df.to_csv(stagging_data_path_traing, index=False)

    raw_data_path_testing =  raw_data_path + "customer_churn_dataset-testing-master.csv"
    stagging_data_path_testing = stagging_data_path+"customer_churn_dataset_testing.csv"
    df = pd.read_csv(raw_data_path_traing)
    df.to_csv(stagging_data_path_testing, index=False)
    print("Data downloaded successfully!")

