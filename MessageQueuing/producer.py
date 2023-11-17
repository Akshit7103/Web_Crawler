import pika
import json

def publish_crawl_request(start_url, crawl_depth, search_keywords):
    connection = pika.BlockingConnection(pika.ConnectionParameters('127.0.0.1'))
    channel = connection.channel()
    channel.queue_declare(queue='crawler_queue')

    crawl_request = {
        'start_url': start_url,
        'crawl_depth': crawl_depth,
        'search_keywords': search_keywords,
    }

    channel.basic_publish(
        exchange='',
        routing_key='crawler_queue',
        body=json.dumps(crawl_request)
    )

    print(f"Crawl request published: {crawl_request}")

    connection.close()

if __name__ == "__main__":  # Corrected this line
    start_url = input("Enter the start URL: ")
    crawl_depth = int(input("Enter the crawl depth: "))
    search_keywords = input("Enter search keywords (comma-separated): ").split(',')

    publish_crawl_request(start_url, crawl_depth, search_keywords) 
