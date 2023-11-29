import pika
import json

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

channel.queue_declare(queue='crawl_requests', durable=True)

url = input("Enter URL to crawl: ")
depth = int(input("Enter depth of crawl: "))
keywords = input("Enter keywords separated by commas: ").split(',')

message = json.dumps({"url": url, "depth": depth, "keywords": keywords})

channel.basic_publish(exchange='',
                      routing_key='crawl_requests',
                      body=message,
                      properties=pika.BasicProperties(
                         delivery_mode=2,
                      ))
print(" [x] Sent crawl request")

connection.close()
