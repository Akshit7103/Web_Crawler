import pika
import json
from django.core.management.base import BaseCommand

class Command(BaseCommand):
    help = 'Publish a crawl request to RabbitMQ'

    def add_arguments(self, parser):
        parser.add_argument('start_url', type=str, help='The start URL for crawling')
        parser.add_argument('crawl_depth', type=int, help='The crawl depth')
        parser.add_argument('search_keywords', type=str, help='Search keywords (comma-separated)')

    def handle(self, *args, **options):
        start_url = options['start_url']
        crawl_depth = options['crawl_depth']
        search_keywords = options['search_keywords'].split(',')

        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
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

            self.stdout.write(self.style.SUCCESS(f"Crawl request published: {crawl_request}"))

        except Exception as e:
            self.stderr.write(self.style.ERROR(f"An error occurred: {e}"))
        finally:
            connection.close()
