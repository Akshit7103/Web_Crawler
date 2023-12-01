import pika
import requests
from bs4 import BeautifulSoup
import json
from urllib.parse import urljoin
import pandas as pd
import mysql.connector
import threading
import uuid  # Import the uuid module

def get_keywords_from_db():
    try:
        connection = mysql.connector.connect(
            host='localhost',
            user='root',  # Replace with your MySQL username
            password='82461937Cr7@',  # Replace with your MySQL password
            database='web_crawler'
        )
        cursor = connection.cursor()
        cursor.execute("SELECT keyword FROM keywords")
        keywords = [item[0] for item in cursor.fetchall()]
        cursor.close()
        connection.close()
        return keywords
    except mysql.connector.Error as err:
        print("Error connecting to MySQL:", err)
        return []

def crawl(url, depth, keywords, hits, visited_urls, lock):
    if depth == 0 or url in visited_urls:
        return

    with lock:
        if url in visited_urls:
            return
        visited_urls.add(url)

    try:
        response = requests.get(url)
        soup = BeautifulSoup(response.text, 'html.parser')
        text = soup.get_text().lower()

        for keyword in keywords:
            lower_keyword = keyword.lower()
            count = text.count(lower_keyword)
            if count > 0:
                with lock:
                    print(f"Keyword '{keyword}' found {count} times at {url}")
                    hits.append({'Keyword': keyword, 'URL': url, 'Count': count})

        for link in soup.find_all('a'):
            href = link.get('href')
            if href and not href.startswith('#'):
                absolute_url = urljoin(url, href)
                if absolute_url not in visited_urls:
                    threading.Thread(target=crawl, args=(absolute_url, depth - 1, keywords, hits, visited_urls, lock)).start()

    except requests.RequestException as e:
        with lock:
            print(f"Failed to crawl {url}: {str(e)}")

def on_request(ch, method, properties, body):
    data = json.loads(body)
    crawl_id = str(uuid.uuid4())  # Generate a unique ID for the crawl

    print(f" [x] Received crawl request (ID: {crawl_id}) for", data['url'])

    keywords = get_keywords_from_db()
    hits = []
    visited_urls = set()
    lock = threading.Lock()

    crawl(data['url'], data['depth'], keywords, hits, visited_urls, lock)

    # Wait for all threads to finish
    main_thread = threading.current_thread()
    for t in threading.enumerate():
        if t is main_thread:
            continue
        t.join()

    # Save hits to an Excel file with the crawl ID in the name
    if hits:
        excel_file_name = f'keyword_hits_{crawl_id}.xlsx'
        df = pd.DataFrame(hits)
        df.to_excel(excel_file_name, index=False)

    print(f"Crawling completed for request (ID: {crawl_id}):", data['url'])
    ch.basic_ack(delivery_tag=method.delivery_tag)

# Establish connection with RabbitMQ server
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

# Declare the queue as durable
channel.queue_declare(queue='crawl_requests', durable=True)

# Set up subscription on the queue
channel.basic_consume(queue='crawl_requests', on_message_callback=on_request)

print(' [*] Waiting for crawl requests. To exit, press CTRL+C')
channel.start_consuming()
