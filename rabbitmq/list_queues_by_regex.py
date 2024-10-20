#  pip install requests

import requests
from requests.auth import HTTPBasicAuth
import os

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "http://localhost")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", 15672))
RABBITMQ_VHOST = os.getenv("RABBITMQ_VHOST", "/")
RABBITMQ_USER = os.getenv("RABBITMQ_USER", "guest")
RABBITMQ_PASSWORD = os.getenv("RABBITMQ_PASSWORD", "guest")

PAGE = 1
PAGE_SIZE = 50
REGEX = "^my.*person$"

def build_list_queues_url(regex: str):
    return (f"{RABBITMQ_HOST}:{RABBITMQ_PORT}/api/queues?vhost={RABBITMQ_VHOST}"
            f"&pagination=true&page={PAGE}&page_size={PAGE_SIZE}"
            f"&name={regex}&use_regex=true")

def print_queues(queues):
    if queues:
        for queue in queues['items']:
            name = queue['name']
            consumers = queue['consumers']
            print(f"{name} - consumers: {consumers}")
    else:
        print(f"No queue found!")

def list_queues_by_regex(regex: str):
    try:
        url = build_list_queues_url(regex)
        response = requests.get(url, auth=HTTPBasicAuth(RABBITMQ_USER, RABBITMQ_PASSWORD))
        response.raise_for_status()
        queues = response.json()
        print_queues(queues)
    except requests.exceptions.RequestException as e:
        print(f"Error accessing the RabbitMQ API: {e}")

def input_regex():
    return input("Enter regex: ")

def execute_all():
    regex = input_regex()
    if regex:
        list_queues_by_regex(regex)
    else:
        print("Regex is required.")

if __name__ == "__main__":
    execute_all()
