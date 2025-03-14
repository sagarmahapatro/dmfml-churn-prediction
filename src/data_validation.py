import pandas as pd

def validate_data_df(df):
    """Perform data validation checks."""
    report = {}

    # Check for missing values
    missing_values = df.isnull().sum()
    report['Missing Values'] = missing_values[missing_values > 0].to_dict()

    # Validate data types
    expected_types = {col: str(df[col].dtype) for col in df.columns}
    report['Data Types'] = expected_types

    # Identify duplicates
    duplicate_count = df.duplicated().sum()
    report['Duplicate Rows'] = duplicate_count

    # Identify anomalies (outliers using IQR method)
    outliers = {}
    for col in df.select_dtypes(include=['number']).columns:
        q1, q3 = df[col].quantile([0.25, 0.75])
        iqr = q3 - q1
        lower_bound, upper_bound = q1 - 1.5 * iqr, q3 + 1.5 * iqr
        outliers[col] = df[(df[col] < lower_bound) | (df[col] > upper_bound)].shape[0]
    report['Outliers'] = {k: v for k, v in outliers.items() if v > 0}

    return report

def validate_data():
    df_traning = pd.read_csv('/opt/airflow/storage/processed/staging/customer_churn_dataset_traning_cleaned_data.csv')
    validate_data_df(df_traning)
    df_testing = pd.read_csv('/opt/airflow/storage/processed/staging/customer_churn_dataset_testing_cleaned_data.csv')
    validate_data_df(df_testing)
    print("Data validated successfully!")
