import time
from scrapy import signals
from scrapy.exceptions import IgnoreRequest


class HandleMiddleware:
    def __init__(self, crawler, config_data):
        self.crawler = crawler
        self.delay = config_data.get('PAUSE_TIME', 300)  # Default delay
        self.openai_api_url = 'https://api.openai.com/'

    @classmethod
    def from_crawler(cls, crawler):
        # Retrieve config data from crawler's settings
        config_data = crawler.settings.getdict('CUSTOM_CONFIG_DATA')
        middleware = cls(crawler, config_data)
        crawler.signals.connect(middleware.spider_opened, signal=signals.spider_opened)
        return middleware

    def spider_opened(self, spider):
        self.logger = spider.logger

    def process_response(self, request, response, spider):
        if response.status == 429 and self._is_openai_request(request):
            self.logger.warning(
                "Received 429 status code for OpenAI request. Pausing for {} minutes.".format(self.delay / 60))
            time.sleep(self.delay)
            self.logger.info("Resuming the request.")
            return request.copy()  # Resend the original request after the delay
        return response

    def _is_openai_request(self, request):
        return request.url.startswith(self.openai_api_url)

    def process_exception(self, request, exception, spider):
        if isinstance(exception, IgnoreRequest):
            return None
        return request
