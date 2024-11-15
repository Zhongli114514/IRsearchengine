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
        'COMPRESSION_ENABLED': True,
        'DOWNLOAD_TIMEOUT': 5,
        'RETRY_ENABLED': True,
        'RETRY_TIMES': 2,
        'JOBDIR': 'jobdir',
        'DUPEFILTER_CLASS' : 'scrapy.dupefilters.RFPDupeFilter',
        'SCHEDULER_PERSIST' : True,
        'SCHEDULER' : 'scrapy.core.scheduler.Scheduler',
        'SCHEDULER_DISK_QUEUE' : 'scrapy.squeues.PickleFifoDiskQueue',
        'SCHEDULER_MEMORY_QUEUE' : 'scrapy.squeues.FifoMemoryQueue'
    }

    def parse(self, response):
        content_type = response.headers.get('Content-Type', b'').decode('utf-8')
        if 'text/html' not in content_type and 'text/plain' not in content_type and 'text/css' not in content_type:
            return

        page_url = response.url

        # 获取网页内容
        page_content = response.xpath('//body//text()').getall()
        page_content = ' '.join([text.strip() for text in page_content if text.strip()])

        # 获取所有链接及其锚文本
        links = response.css('a::attr(href)').getall()
        anchor_texts = response.css('a::text').getall()

        # 正确处理链接的完整路径
        full_links = [urljoin(response.url, link) for link in links]

        # 记录出链数
        out_links_count = len(full_links)

        # 生成链接和锚文本的键值对
        link_anchor_pairs = [{'link': full_link, 'anchor_text': anchor_text.strip()} 
                             for full_link, anchor_text in zip(full_links, anchor_texts) 
                             if full_link.startswith("http") or full_link.startswith("https")]

        # 保存当前页面的信息
        yield {
            'page_url': page_url,
            'page_content': page_content,
            'out_links_count': out_links_count,
            'link_anchor_pairs': link_anchor_pairs
        }

        # 继续抓取新的链接
        for full_link in full_links:
            if full_link.startswith("http") or full_link.startswith("https"):
                yield response.follow(full_link, callback=self.parse)