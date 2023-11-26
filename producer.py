import pika
import json
import uuid
from db_utils import create_database_connection
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def fetch_keywords():
    connection = create_database_connection()
    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT word FROM words")
            return [row['word'] for row in cursor.fetchall()]
    except Exception as e:
        logging.error(f"Error fetching keywords: {e}")
        return []
    finally:
        connection.close()


connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()
channel.queue_declare(queue='crawl_requests', durable=True)

def send_crawl_request(request):
    channel.basic_publish(
        exchange='',
        routing_key='crawl_requests',
        body=json.dumps(request)
    )
    logging.info(f"Sent Crawl Request: {request['request_id']}")

urls = input("Enter URLs separated by commas: ").split(',')
depth = int(input("Enter crawl depth: "))
keywords = fetch_keywords()

if not keywords:
    logging.error("No keywords fetched from the database. Please check the database and the words table.")
else:
    crawl_request = {
        'request_id': str(uuid.uuid4()),
        'sites': [{'url': url.strip()} for url in urls],
        'depth': depth,
        'keywords': keywords
    }

    send_crawl_request(crawl_request)

connection.close()
