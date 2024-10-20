#  pip install requests

import requests
from requests.auth import HTTPBasicAuth
import os

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "http://localhost")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", 15672))
RABBITMQ_USER = os.getenv("RABBITMQ_USER", "guest")
RABBITMQ_PASSWORD = os.getenv("RABBITMQ_PASSWORD", "guest")

PREFIX = "my-"
URL = f"{RABBITMQ_HOST}:{RABBITMQ_PORT}/api/queues"

def list_queues():
    try:
        response = requests.get(URL, auth=HTTPBasicAuth(RABBITMQ_USER, RABBITMQ_PASSWORD))
        response.raise_for_status()
        queues = filter_queues(response.json())
        print_queues(queues)
    except requests.exceptions.RequestException as e:
        print(f"Error accessing the RabbitMQ API: {e}")

def filter_queues(queues):
    return [queue['name'] for queue in queues if queue['name'].startswith(PREFIX)]

def print_queues(queues):
    if queues:
        for queue in queues:
            print(queue)
    else:
        print(f"No queue found with the prefix '{PREFIX}'.")

if __name__ == "__main__":
    list_queues()
