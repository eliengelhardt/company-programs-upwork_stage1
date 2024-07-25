import logging
import random

from scrapy.downloadermiddlewares.retry import RetryMiddleware
from scrapy.utils.response import response_status_message


class ProxyRetryMiddleware(RetryMiddleware):
    def __init__(self, settings):
        super().__init__(settings)
        self.proxies = settings.getlist('PROXIES')
        self.config_data = settings.getdict('CUSTOM_CONFIG_DATA')
        if not self.proxies:
            raise ValueError("No proxies found in settings")

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler.settings)

    def process_request(self, request, spider):
        # Assign a random proxy to the request
        request.meta['proxy'] = random.choice(self.proxies)
        return None

    def process_response(self, request, response, spider):
        if request.meta.get('dont_retry', False):
            return response

        if response.status in self.retry_http_codes:
            reason = response_status_message(response.status)
            return self._retry(request, reason, spider) or response

        # Continue processing in the same callback method
        return response

    def process_exception(self, request, exception, spider):
        if isinstance(exception, self.EXCEPTIONS_TO_RETRY) and not request.meta.get('dont_retry', False):
            return self._retry(request, exception, spider)
        return None

    def _retry(self, request, reason, spider):
        retries = request.meta.get('retry_times', 0) + 1

        if retries <= self.config_data.get('RETRY_TIME'):
            logging.info(f"Retrying {request} (failed {retries} times): {reason}")
            retryreq = request.copy()
            retryreq.meta['retry_times'] = retries
            retryreq.dont_filter = True
            # Rotate the proxy
            retryreq.meta['proxy'] = random.choice(self.proxies)
            return retryreq
        else:
            status_code = int(reason.split(' ')[0])

            logging.info(f"Processing response with status code {status_code} despite retry limit exceeded")

            return None
