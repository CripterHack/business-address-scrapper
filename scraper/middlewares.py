import random
from fake_useragent import UserAgent
from scrapy import signals
from scrapy.exceptions import NotConfigured
from scraper.settings import PROXY_LIST

class RandomUserAgentMiddleware:
    def __init__(self):
        self.ua = UserAgent()

    @classmethod
    def from_crawler(cls, crawler):
        middleware = cls()
        crawler.signals.connect(middleware.spider_opened, signal=signals.spider_opened)
        return middleware

    def spider_opened(self, spider):
        spider.logger.info('Spider opened: %s' % spider.name)

    def process_request(self, request, spider):
        request.headers['User-Agent'] = self.ua.random

class ProxyMiddleware:
    def __init__(self):
        if not PROXY_LIST:
            raise NotConfigured('No proxy list configured')
        self.proxies = PROXY_LIST

    @classmethod
    def from_crawler(cls, crawler):
        return cls()

    def process_request(self, request, spider):
        if self.proxies:
            request.meta['proxy'] = random.choice(self.proxies)

class HumanBehaviorMiddleware:
    def __init__(self):
        self.min_delay = 1
        self.max_delay = 5

    @classmethod
    def from_crawler(cls, crawler):
        return cls()

    def process_request(self, request, spider):
        # Simulate random delays between requests
        delay = random.uniform(self.min_delay, self.max_delay)
        spider.logger.info(f'Waiting {delay} seconds before next request')
        return None 