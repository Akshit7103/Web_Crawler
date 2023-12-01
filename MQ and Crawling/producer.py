import pika
import json

# Establish connection with RabbitMQ server
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

# Declare a queue as durable
channel.queue_declare(queue='crawl_requests', durable=True)

# Input details for the crawl
url = input("Enter URL to crawl: ")
depth = int(input("Enter depth of crawl: "))

# Create a message
# Keywords are not included as they are fetched from the database in the consumer script
message = json.dumps({"url": url, "depth": depth})

# Publish the message
channel.basic_publish(exchange='',
                      routing_key='crawl_requests',
                      body=message,
                      properties=pika.BasicProperties(
                         delivery_mode=2,  # make message persistent
                      ))
print(" [x] Sent crawl request")

# Close the connection
connection.close()
