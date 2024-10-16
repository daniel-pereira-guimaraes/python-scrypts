# pip install pika

import os
import pika

RABBITMQ_HOST = os.getenv('RABBITMQ_HOST', 'localhost')
RABBITMQ_PORT = int(os.getenv('RABBITMQ_PORT', 5672))
RABBITMQ_USER = os.getenv('RABBITMQ_USER', 'guest')
RABBITMQ_PASSWORD = os.getenv('RABBITMQ_PASSWORD', 'guest')
RABBITMQ_VHOST = os.getenv('RABBITMQ_VHOST', '/')

def send_test_message():
    queue = 'my-queue'
    message = "Hello, RabbitMQ!"
    headers = {
        'header1': 'value1',
        'header2': 'value2'
    }
    send_message(queue, message, headers)
    print(f"Message sent to the queue '{queue}'")

def send_message(queue: str, message: str, headers: dict):
    with create_connection() as connection:
        channel = connection.channel()
        declare_queue(channel, queue)
        publish_message(channel, queue, message, headers)

def create_connection():
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
    parameters = pika.ConnectionParameters(
        host=RABBITMQ_HOST,
        port=RABBITMQ_PORT,
        virtual_host=RABBITMQ_VHOST,
        credentials=credentials
    )
    return pika.BlockingConnection(parameters)

def declare_queue(channel, queue_name: str):
    channel.queue_declare(queue=queue_name, durable=True)

def publish_message(channel, queue_name: str, message: str, headers: dict):
    properties = pika.BasicProperties(headers=headers)
    channel.basic_publish(
        exchange='',
        routing_key=queue_name,
        body=message,
        properties=properties
    )

if __name__ == "__main__":
    send_test_message()