# pip install pandas
# pip install elasticsearch

import os
import pandas as pd
from elasticsearch import Elasticsearch

ES_HOST = os.getenv("ES_HOST", "http://localhost")
ES_PORT = int(os.getenv("ES_PORT", 9200))
ES_USER = os.getenv("ES_USER", "elastic")
ES_PASSWORD = os.getenv("ES_PASSWORD", "my-password")
ES_INDEX = "country"
CSV_FILE = "countries_exported.csv"

def connect_to_elastic():
    return Elasticsearch(
        hosts=[f"{ES_HOST}:{ES_PORT}"],
        basic_auth=(ES_USER, ES_PASSWORD)
    )

def fetch_documents(es):
    response = es.search(index=ES_INDEX, body={"query": {"match_all": {}}}, size=1000)
    return [hit['_source'] for hit in response['hits']['hits']]

def save_to_csv(documents, filename):
    data_frame = pd.DataFrame(documents)
    data_frame.to_csv(filename, index=False)
    print(f"Exported {len(documents)} records to {filename}")

def export_countries_to_csv():
    es = connect_to_elastic()
    documents = fetch_documents(es)
    save_to_csv(documents, CSV_FILE)

def execute_all():
    try:
        export_countries_to_csv()
        print("Done!")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    execute_all()
