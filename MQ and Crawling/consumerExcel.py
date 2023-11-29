import pika
import requests
from bs4 import BeautifulSoup
import json
from urllib.parse import urljoin
import pandas as pd

def crawl(url, depth, keywords, hits, hit_counts, visited_urls):
    if depth == 0 or url in visited_urls:
        return

    visited_urls.add(url)

    try:
        response = requests.get(url)
        soup = BeautifulSoup(response.text, 'html.parser')

        # Convert page text to lower case for case-insensitive search
        text = soup.get_text().lower()
        for keyword in keywords:
            lower_keyword = keyword.lower()
            count = text.count(lower_keyword)
            if count > 0:
                print(f"Keyword '{keyword}' found {count} times at {url}")
                hits.append({'Keyword': keyword, 'URL': url, 'Count': count})
                hit_counts[keyword] += count

        # Crawl to the next depth
        for link in soup.find_all('a'):
            href = link.get('href')
            if href and not href.startswith('#'):
                absolute_url = urljoin(url, href)
                crawl(absolute_url, depth - 1, keywords, hits, hit_counts, visited_urls)

    except requests.RequestException as e:
        print(f"Failed to crawl {url}: {str(e)}")

def on_request(ch, method, properties, body):
    data = json.loads(body)
    print(" [x] Received crawl request for", data['url'])

    hits = []
    hit_counts = {keyword.lower(): 0 for keyword in data['keywords']}
    visited_urls = set()

    crawl(data['url'], data['depth'], data['keywords'], hits, hit_counts, visited_urls)

    # Convert hits to a DataFrame and save to Excel
    if hits:
        df = pd.DataFrame(hits)
        df.to_excel('keyword_hits.xlsx', index=False)

    print("Crawling completed for request:", data['url'])

    ch.basic_ack(delivery_tag=method.delivery_tag)

# Establish connection with RabbitMQ server
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

# Declare the queue as durable
channel.queue_declare(queue='crawl_requests', durable=True)

# Set up subscription on the queue
channel.basic_consume(queue='crawl_requests', on_message_callback=on_request)

print(' [*] Waiting for crawl requests. To exit press CTRL+C')
channel.start_consuming()
