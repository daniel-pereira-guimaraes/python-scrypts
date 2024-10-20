# pip install requests
# pip install colorama

import os
import requests
import json
from requests import Response
from requests.auth import HTTPBasicAuth
from urllib.parse import quote
from colorama import Fore, init as init_colorama

init_colorama()

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", 15672))
RABBITMQ_VHOST = os.getenv("RABBITMQ_VHOST", "/")
RABBITMQ_USER = os.getenv("RABBITMQ_USER", "guest")
RABBITMQ_PASSWORD = os.getenv("RABBITMQ_PASSWORD", "guest")
RABBITMQ_AUTH = HTTPBasicAuth(RABBITMQ_USER, RABBITMQ_PASSWORD)

class GetQueueError(Exception):
    pass

class PurgeQueueError(Exception):
    pass

def queue_url(queue_name: str) -> str:
    encoded_vhost = quote(RABBITMQ_VHOST, safe='')
    return (f"http://{RABBITMQ_HOST}:{RABBITMQ_PORT}"
            f"/api/queues/{encoded_vhost}/{queue_name}")

def queue_contents_url(queue_name: str) -> str:
    encoded_vhost = quote(RABBITMQ_VHOST, safe='')
    return (f"http://{RABBITMQ_HOST}:{RABBITMQ_PORT}"
            f"/api/queues/{encoded_vhost}/{queue_name}/contents")

def response_error_message(response: Response) -> str:
    try:
        error_info = json.loads(response.text)
        message = error_info.get("error")
        return message if message is not None else response.text
    except json.JSONDecodeError:
        return response.text

def error_message_for(message: str, queue_name: str,
                      response: Response) -> str:
    response_message = response_error_message(response)
    return (f"{message} '{queue_name}'"
            f": {response.status_code} - {response_message}")

def get_error_message(queue: str, response: Response) -> str:
    message = "Failed to retrieve information for queue"
    return error_message_for(message, queue, response)

def purge_error_message(queue: str, response: Response) -> str:
    message = "Failed to purge queue"
    return error_message_for(message, queue, response)

def print_message(message: str | Exception, color: str):
    print(f"{color}{message}{Fore.RESET}")

def get_queue_info(queue_name: str):
    url = queue_url(queue_name)
    response = requests.get(url, auth=RABBITMQ_AUTH)
    if response.status_code == 200:
        return response.json()
    else:
        raise GetQueueError(get_error_message(queue_name, response))

def purge_queue(queue_name: str):
    url = queue_contents_url(queue_name)
    response = requests.delete(url, auth=RABBITMQ_AUTH)
    if response.status_code == 204:
        print_message(f"Queue '{queue_name}' purged successfully.", Fore.GREEN)
    else:
        raise PurgeQueueError(purge_error_message(queue_name, response))

def input_queue_name() -> str:
    return input("Queue name for purge: ")

def confirm_purge() -> bool:
    return input("Confirm purge queue? (yes|no): ").lower() == "yes"

def execute_all():
    queue_name = input_queue_name()
    queue_info = get_queue_info(queue_name)
    if queue_info.get('messages', 0) == 0:
        raise PurgeQueueError("The queue is empty!")
    if confirm_purge():
        purge_queue(queue_name)

if __name__ == "__main__":
    try:
        execute_all()
    except Exception as e:
        print_message(e, Fore.RED)
