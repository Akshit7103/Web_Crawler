# web_crawler/urls.py
from django.contrib import admin
from django.urls import path, include

urlpatterns = [
    path('admin/', admin.site.urls),
    path('', include('crawler.urls')),
    path('crawler/', include('crawler.urls'))
    # Add other URL patterns as needed
]
