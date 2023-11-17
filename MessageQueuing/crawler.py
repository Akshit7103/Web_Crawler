import requests
from bs4 import BeautifulSoup

def crawl_website_from_request(crawl_request):
    start_url = crawl_request['start_url']
    crawl_depth = crawl_request['crawl_depth']
    search_keywords = crawl_request['search_keywords']

    # Function to perform the crawl
    def crawl(url, depth):
        if depth > crawl_depth:
            return

        try:
            response = requests.get(url)
            soup = BeautifulSoup(response.text, 'html.parser')

            # Process the page here (e.g., search for keywords, extract information)
            # This is a placeholder for whatever processing you want to do
            print(f"Processing {url} at depth {depth}")

            # Find all links on the page and crawl them
            for link in soup.find_all('a', href=True):
                next_page = link['href']
                crawl(next_page, depth + 1)

        except requests.RequestException as e:
            print(f"Failed to retrieve {url}: {e}")

    # Start the crawl process
    crawl(start_url, 0)