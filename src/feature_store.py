import pandas as pd

def create_feature_store():
    df = pd.read_csv('/opt/airflow/src/transformed_data.csv')
    # Example feature engineering: Normalize tenure
    df['tenure'] = df['tenure'] / df['tenure'].max()
    df.to_csv('/opt/airflow/src/feature_store.csv', index=False)
    print("Feature store created successfully!")
