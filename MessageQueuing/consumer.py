import pika
import json
from crawler import crawl_website_from_request

def callback(ch, method, properties, body):
    crawl_request = json.loads(body.decode('utf-8'))
    crawl_website_from_request(crawl_request)

connection = pika.BlockingConnection(pika.ConnectionParameters('127.0.0.1'))
channel = connection.channel()
channel.queue_declare(queue='crawler_queue')

channel.basic_consume(queue='crawler_queue', on_message_callback=callback, auto_ack=True)

print(" [*] Waiting for crawl requests. To exit, press CTRL+C")
channel.start_consuming()
