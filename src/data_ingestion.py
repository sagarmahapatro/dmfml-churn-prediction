import pandas as pd

def ingest_data(url, file_path):
    data = pd.read_csv(url)
    data.to_csv(file_path, index=False)
