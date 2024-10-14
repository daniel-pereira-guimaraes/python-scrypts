# pip install pika

import os
import pika

RABBITMQ_HOST = os.getenv('RABBITMQ_HOST', 'localhost')
RABBITMQ_PORT = int(os.getenv('RABBITMQ_PORT', 5672))
RABBITMQ_USER = os.getenv('RABBITMQ_USER', 'guest')
RABBITMQ_PASSWORD = os.getenv('RABBITMQ_PASSWORD', 'guest')
RABBITMQ_VHOST = os.getenv('RABBITMQ_VHOST', '/')

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

def callback(ch, method, properties, body):
    print(f"Received message:")
    print(f"\tHeaders: {properties.headers}")
    print(f"\tBody: {body.decode()}")
    ch.basic_ack(delivery_tag=method.delivery_tag)

def consume_messages(queue: str):
    with create_connection() as connection:
        channel = connection.channel()
        declare_queue(channel, queue)
        channel.basic_consume(queue=queue, on_message_callback=callback)
        print(f"Waiting for messages in '{queue}'. To exit press CTRL+C")
        channel.start_consuming()

if __name__ == "__main__":
    consume_messages('my-queue')