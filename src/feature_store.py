import pandas as pd
from datetime import datetime
import pytz
from sklearn.preprocessing import StandardScaler, LabelEncoder, OneHotEncoder
import numpy as np

def feature_engineering(df):
    """Create new aggregated and derived features."""
    if 'total_spend' in df.columns and 'num_purchases' in df.columns:
        df['avg_spend_per_purchase'] = df['total_spend'] / df['num_purchases'].replace(0, np.nan)

    if 'signup_date' in df.columns:
        df['signup_date'] = pd.to_datetime(df['signup_date'])
        df['customer_tenure_days'] = (datetime.now(pytz.timezone('Asia/Kolkata')) - df['signup_date']).dt.days

    if 'last_activity_date' in df.columns:
        df['last_activity_date'] = pd.to_datetime(df['last_activity_date'])
        df['days_since_last_activity'] = (datetime.now(pytz.timezone('Asia/Kolkata')) - df['last_activity_date']).dt.days

    #logging.info("Feature engineering applied.")
    return df


def select_feature(df):
    threshold = 0.03
    correlation_matrix = df.corr()
    high_corr_features = correlation_matrix.index[abs(correlation_matrix["Churn"]) > threshold].tolist()
    return df[high_corr_features]    

def create_feature_store():
    df_traning = pd.read_csv('/opt/airflow/storage/processed/staging/customer_churn_dataset_traning_transformed_data.csv')
    df_traning = feature_engineering(df_traning)
    df_traning = select_feature(df_traning)
    df_traning.to_csv('/opt/airflow/storage/processed/staging/customer_churn_dataset_traning_feature_data.csv', index=False)
    
    df_testing = pd.read_csv('/opt/airflow/storage/processed/staging/customer_churn_dataset_testing_transformed_data.csv')
    df_testing = feature_engineering(df_testing)
    df_testing = select_feature(df_testing)
    df_testing.to_csv('/opt/airflow/storage/processed/staging/customer_churn_dataset_testing_feature_data.csv', index=False)
    print("Feature store created successfully!")

