from urllib.parse import urljoin
import scrapy


class RucSpiderSpider(scrapy.Spider):
    name = "ruc_spider"
    allowed_domains = ["ruc.edu.cn"]
    start_urls = ["https://www.ruc.edu.cn"]

    custom_settings = {
        'DOWNLOAD_DELAY': 0.1,
        'CONCURRENT_REQUESTS': 32,
        'ROBOTSTXT_OBEY': True,
        'DEPTH_LIMIT': 50,
        'DNS_RESOLVER': 'scrapy_aiohttp.AiohttpDNSResolver',
        'COMPRESSION_ENABLED': True
    }

    def parse(self, response):
        content_type = response.headers.get('Content-Type', b'').decode('utf-8')
        if 'text/html' not in content_type and 'text/plain' not in content_type and 'text/css' not in content_type:
            return
        page = response.url
        yield{
            'url':page
        }
        for link in response.css('a::attr(href)').getall():
            full_link = urljoin(response.url, link)
            if full_link.startswith("http") or full_link.startswith("https"):
                yield response.follow(full_link, callback=self.parse)
