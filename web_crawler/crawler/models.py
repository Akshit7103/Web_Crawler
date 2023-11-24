# crawler/models.py
from django.db import models

class CrawlRequest(models.Model):
    start_url = models.URLField()
    crawl_depth = models.IntegerField()
    search_keywords = models.TextField()
    start_date = models.DateTimeField(auto_now_add=True)
    progress = models.IntegerField(default=0)
    class Meta:
        app_label = 'crawler'

class CrawlResult(models.Model):
    crawl_request = models.ForeignKey(CrawlRequest, on_delete=models.CASCADE)
    url = models.URLField()
    is_good_hit = models.BooleanField()
