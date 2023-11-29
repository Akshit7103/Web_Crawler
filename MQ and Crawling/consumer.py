import pika
import requests
from bs4 import BeautifulSoup
import json
from urllib.parse import urljoin

def crawl(url, depth, keywords, hits, hit_counts, visited_urls):
    if depth == 0 or url in visited_urls:
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
                print(f"Keyword '{keyword}' found {count} times at {url}")
                hits.append({'keyword': keyword, 'url': url, 'count': count})
                hit_counts[keyword] += count

        for link in soup.find_all('a'):
            href = link.get('href')
            if href and not href.startswith('#'):
                absolute_url = urljoin(url, href)
                crawl(absolute_url, depth - 1, keywords, hits, hit_counts, visited_urls)

    except requests.RequestException as e:
        print(f"Failed to crawl {url}: {str(e)}")

def on_request(ch, method, properties, body):
    data = json.loads(body)
    hits = []
    hit_counts = {keyword.lower(): 0 for keyword in data['keywords']}
    visited_urls = set()

    crawl(data['url'], data['depth'], data['keywords'], hits, hit_counts, visited_urls)

    with open('keyword_hits.txt', 'a') as file:
        for hit in hits:
            file.write(f"Keyword: {hit['keyword']} found {hit['count']} times at URL: {hit['url']}\n")
        file.write(f"\nTotal keyword hits:\n")
        for keyword, count in hit_counts.items():
            file.write(f"{keyword}: {count}\n")

    ch.basic_ack(delivery_tag=method.delivery_tag)

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

channel.queue_declare(queue='crawl_requests', durable=True)

channel.basic_consume(queue='crawl_requests', on_message_callback=on_request)

print(' [*] Waiting for crawl requests. To exit press CTRL+C')
channel.start_consuming()
