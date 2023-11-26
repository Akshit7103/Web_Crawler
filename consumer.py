import pika
import json
import requests
from bs4 import BeautifulSoup
import logging
from db_utils import create_database_connection

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

def store_hit(crawl_request_id, url, keyword, hit_type, context):
    connection = create_database_connection()
    try:
        with connection.cursor() as cursor:
            sql = "INSERT INTO hits (crawl_request_id, url, keyword, hit_type, context) VALUES (%s, %s, %s, %s, %s)"
            cursor.execute(sql, (crawl_request_id, url, keyword, hit_type, context))
        connection.commit()
    except Exception as e:
        logging.error(f"Error while storing hit: {e}")
    finally:
        connection.close()

def crawl_url(url, keywords):
    logging.info(f"Starting crawl of URL: {url}")
    try:
        response = requests.get(url, timeout=10)
        logging.info(f"URL response received: {url}")
        if response.status_code == 200:
            soup = BeautifulSoup(response.content, 'html.parser')
            text = soup.get_text()
            for keyword in keywords:
                if keyword in text:
                    logging.info(f"Keyword '{keyword}' found in URL: {url}")
                    store_hit('crawl_request_id', url, keyword, 'text', text)
                    break
        else:
            logging.warning(f"URL responded with status code: {response.status_code}")
    except requests.exceptions.Timeout:
        logging.error(f"Timeout occurred while crawling URL: {url}")
    except Exception as e:
        logging.error(f"Error crawling URL {url}: {e}")

def start_consuming():
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    channel.queue_declare(queue='crawl_requests', durable=True)

    def callback(ch, method, properties, body):
        logging.info("Received a message")
        try:
            message = json.loads(body)
            logging.info(f"Processing message: {message}")
            keywords = fetch_keywords()
            for site in message['sites']:
                crawl_url(site['url'], keywords)
        except Exception as e:
            logging.error(f"Error processing message: {e}")

    channel.basic_consume(queue='crawl_requests', on_message_callback=callback, auto_ack=True)
    logging.info('Starting to consume messages. To exit press CTRL+C')
    channel.start_consuming()

if __name__ == '__main__':
    start_consuming()
