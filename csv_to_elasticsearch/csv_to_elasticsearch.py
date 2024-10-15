# pip install pandas
# pip install elasticsearch

import os
import pandas as pd
from elasticsearch import Elasticsearch

CSV_FILE = "countries.csv"
ES_HOST = os.getenv("ES_HOST", "http://localhost")
ES_PORT = int(os.getenv("ES_PORT", 9200))
ES_USER = os.getenv("ES_USER", "elastic")
ES_PASSWORD = os.getenv("ES_PASSWORD", "my-password")
ES_INDEX = "country"

def connect_to_elastic():
    return Elasticsearch(
        hosts=[f"{ES_HOST}:{ES_PORT}"],
        basic_auth=(ES_USER, ES_PASSWORD)
    )

def csv_to_elasticsearch():
    data_frame = pd.read_csv(CSV_FILE, na_filter=False)
    es = connect_to_elastic()
    for _, row in data_frame.iterrows():
        response = es.index(index=ES_INDEX, body=row.to_dict())
        print(f"{tuple(row.values)} indexed with _id={response['_id']}")

def execute_all():
    try:
        csv_to_elasticsearch()
        print("Done!")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    execute_all()