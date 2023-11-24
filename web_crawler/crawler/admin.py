# crawler/admin.py
from django.contrib import admin
from .models import CrawlRequest, CrawlResult

admin.site.register(CrawlRequest)
admin.site.register(CrawlResult)
