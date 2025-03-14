import pandas as pd
from sklearn.preprocessing import StandardScaler, LabelEncoder 
import numpy as np


def handle_missing_values(df):
    """Handle missing values by filling with mean (numeric) or mode (categorical)."""
    for col in df.columns:
        if df[col].isnull().sum() > 0:
            if df[col].dtype == 'object':
                df[col].fillna(df[col].mode()[0], inplace=True)
            else:
                df[col].fillna(df[col].mean(), inplace=True)
    #logging.info("Handled missing values.")
    return df

def normalize_numeric_features(df):
    """Standardize numerical features."""
    numerical_cols = ['Age', 'Tenure', 'Usage Frequency', 'Support Calls', 'Total Spend', 'Last Interaction']
    scaler = StandardScaler()
    df[numerical_cols] = scaler.fit_transform(df[numerical_cols])
    return df

def encode_categorical_features(df):
    """Encode categorical variables using Label Encoding and One-Hot Encoding."""
    categorical_cols = ['Gender', 'Payment Delay', 'Subscription Type', 'Contract Length']
    # One-hot encode categorical features
    df = pd.get_dummies(df, columns=categorical_cols, drop_first=True)

    #logging.info("Encoded categorical attributes.")
    return df

def encoded_data(df):
    gender_map = {'Male': 0, 'Female': 1}
    subscription_map = {'Basic': 0, 'Premium': 1, 'Pro': 2}
    Contract_Length = {'Annual': 0, 'Quarterly': 1, 'Monthly' : 2}

    df['Gender'] = df['Gender'].map(gender_map)
    df['Subscription Type'] = df['Subscription Type'].map(subscription_map)
    df['Contract Length'] = df['Contract Length'].map(Contract_Length)

def transform_data():
    df_traning = pd.read_csv('/opt/airflow/storage/processed/staging/customer_churn_dataset_traning_cleaned_data.csv')
    df_traning = handle_missing_values(df_traning)
    df_traning = normalize_numeric_features(df_traning)
    df_traning = encode_categorical_features(df_traning)
    df_traning.to_csv('/opt/airflow/storage/processed/staging/customer_churn_dataset_traning_transformed_data.csv', index=False)
    
    df_testing = pd.read_csv('/opt/airflow/storage/processed/staging/customer_churn_dataset_testing_cleaned_data.csv')
    df_testing = handle_missing_values(df_testing)
    df_testing = normalize_numeric_features(df_testing)
    df_testing = encode_categorical_features(df_testing)
    df_testing.to_csv('/opt/airflow/storage/processed/staging/customer_churn_dataset_testing_transformed_data.csv', index=False)
    print("Data transformed successfully!")
