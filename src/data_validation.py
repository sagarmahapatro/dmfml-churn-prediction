import pandas as pd

def validate_data(file_path):
    data = pd.read_csv(file_path)
    if data.isnull().sum().sum() > 0:
        raise ValueError("Data contains missing values.")
    return data
