# pip install requests
# pip install colorama

import os
import requests
from requests.auth import HTTPBasicAuth
from colorama import Fore, init as init_colorama

init_colorama()

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "http://localhost")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", 15672))
RABBITMQ_VHOST = os.getenv("RABBITMQ_VHOST", "/")
RABBITMQ_USER = os.getenv("RABBITMQ_USER", "guest")
RABBITMQ_PASSWORD = os.getenv("RABBITMQ_PASSWORD", "guest")

PAGE = 1
PAGE_SIZE = 50

def build_list_queues_url(regex: str) -> str:
    return (f"{RABBITMQ_HOST}:{RABBITMQ_PORT}/api/queues?vhost={RABBITMQ_VHOST}"
            f"&pagination=true&page={PAGE}&page_size={PAGE_SIZE}"
            f"&name={regex}&use_regex=true")

def print_message(message: str | Exception, color: str):
    print(f"{color}{message}{Fore.RESET}")

def print_queues(queues):
    if queues:
        for queue in queues['items']:
            print_queue(queue)
    else:
        print_message("No queue found!", Fore.RED)

def print_queue(queue):
    name = queue['name']
    consumer_count = str(queue['consumers']).ljust(3)
    message_count = str(queue['messages']).ljust(7)
    print(f"consumers: {consumer_count} messages: {message_count} {name}")

def list_queues_by_regex(regex: str):
    try:
        url = build_list_queues_url(regex)
        response = requests.get(url, auth=HTTPBasicAuth(RABBITMQ_USER, RABBITMQ_PASSWORD))
        response.raise_for_status()
        queues = response.json()
        print_queues(queues)
    except requests.exceptions.RequestException as e:
        print_message(f"Error accessing the RabbitMQ API: {e}", Fore.RED)

def input_regex() -> str:
    return input("Enter regex: ")

def execute_all():
    regex = input_regex()
    if regex:
        list_queues_by_regex(regex)
    else:
        print_message("Regex is required.", Fore.RED)

if __name__ == "__main__":
    execute_all()
