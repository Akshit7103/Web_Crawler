# crawler/views.py
from django.shortcuts import render
from .models import CrawlRequest, CrawlResult
from django.http import HttpResponse

def crawl_website_from_request(crawl_request):
    # Implement your crawling logic here
    # Example: Create a CrawlRequest and save it to the database
    new_crawl_request = CrawlRequest(
        start_url=crawl_request['start_url'],
        crawl_depth=crawl_request['crawl_depth'],
        search_keywords=crawl_request['search_keywords']
    )
    new_crawl_request.save()

    # Perform actual crawling logic here...

    # Example: Create a CrawlResult and save it to the database
    new_crawl_result = CrawlResult(
        crawl_request=new_crawl_request,
        result_data="Crawling result goes here..."
    )
    new_crawl_result.save()

    return HttpResponse("Crawling in progress...")


def index(request):
    crawl_requests = CrawlRequest.objects.all()
    crawl_results = CrawlResult.objects.all()
    return render(request, 'crawler/index.html', {'crawl_requests': crawl_requests, 'crawl_results': crawl_results})
