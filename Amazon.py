import re
import time
import json
import scrapy
import random
import logging
import requests
from helper import *
from scrapy import signals
from urllib.parse import urljoin
from scrapy.crawler import CrawlerProcess
from sales_rank_to_sales import run_class
from scrapy.exceptions import IgnoreRequest
from scrapy.utils.project import get_project_settings
from scrapy.utils.response import response_status_message
from scrapy.downloadermiddlewares.retry import RetryMiddleware
from minimum_sales_required import minimum_total_sales_of_search_group_for_results

class_for_sales_rank = run_class()

with open('config.json', 'r') as json_file:
    config_data = json.load(json_file)

HEADERS = {
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "accept-language": "en-US,en;q=0.9",
    "cache-control": "max-age=0",
    "device-memory": "8",
    "downlink": "6.85",
    "dpr": "1.5",
    "ect": "4g",
    # "referer": "https://www.amazon.com/s?k=stanley&crid=PVDMUJUCJK7N&sprefix=stanley%2Caps%2C334&ref=nb_sb_noss_1",
    "rtt": "200",
    "sec-ch-device-memory": "8",
    "sec-ch-dpr": "1.5",
    "sec-ch-ua": "\"Google Chrome\";v=\"123\", \"Not:A-Brand\";v=\"8\", \"Chromium\";v=\"123\"",
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": "\"Windows\"",
    "sec-ch-ua-platform-version": "\"10.0.0\"",
    "sec-ch-viewport-width": "1442",
    "sec-fetch-dest": "document",
    "sec-fetch-mode": "navigate",
    "sec-fetch-site": "same-origin",
    "sec-fetch-user": "?1",
    "upgrade-insecure-requests": "1",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "viewport-width": "1442"
}

GPT_HEADERS = {
    'Content-Type': 'application/json',
    'Authorization': f'Bearer {config_data.get("GPT_API_KEY")}'
}
RULES = config_data.get('RULES')
API_URL_GPT = config_data.get('API_URL_GPT')
BASE_URL = config_data.get('BASE_URL')


def get_proxies():
    proxy_path = config_data.get('PROXIES_FILE_PATH')
    file = open(proxy_path, mode='r')
    proxies = []
    for url in file.readlines():
        proxies.append(
            f'http://{url.strip()}'
        )
    # proxies = [
    #     "http://ufenldeh:lim9gprvbsew@38.154.227.167:5868",
    #     "http://ufenldeh:lim9gprvbsew@185.199.229.156:7492",
    #     "http://ufenldeh:lim9gprvbsew@185.199.228.220:7300",
    #     "http://ufenldeh:lim9gprvbsew@185.199.231.45:8382",
    #     "http://ufenldeh:lim9gprvbsew@188.74.210.207:6286",
    #     "http://ufenldeh:lim9gprvbsew@188.74.183.10:8279",
    #     "http://ufenldeh:lim9gprvbsew@188.74.210.21:6100",
    #     "http://ufenldeh:lim9gprvbsew@45.155.68.129:8133",
    #     "http://ufenldeh:lim9gprvbsew@154.95.36.199:6893",
    #     "http://ufenldeh:lim9gprvbsew@45.94.47.66:8110"
    # ]
    return proxies


PROXIES = get_proxies()


class AmazonSpider(scrapy.Spider):
    name = "amazon_Scope"

    skipped_list = set()
    custom_settings = {
        'LOG_LEVEL': 'INFO'
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.logs = {}

    def start_requests(self):
        for term in terms_list:
            term = term.lower().strip()
            self.skipped_list.add(term)
            prompt = RULES.get('RULE_5_6_9').format(term).strip()
            data = get_gpt_payload(prompt)
            yield scrapy.Request(
                url=API_URL_GPT,
                method='POST',
                headers=GPT_HEADERS,
                body=json.dumps(data),
                meta={'search_term': term, 'proxy': random.choice(PROXIES)}
            )

    def parse(self, response, **kwargs):
        """
            Implemented the 5,6 9 ruler here
            :param response:
            :param kwargs:
            :return:
        """
        term = response.meta.get('search_term')

        self.logs[term] = {}

        data = response.json()
        result = data.get('choices', [{}])[0].get('message', {}).get('content', '')
        if result == 'True':
            api_url = f"https://completion.amazon.com/api/2017/suggestions?limit=11&prefix={term}&suggestion-type=WIDGET&suggestion-type=KEYWORD&page-type=Search&alias=aps&site-variant=desktop&version=3&event=onkeypress&wc=&lop=en_US&last-prefix=ceramic%20fire%20balls&avg-ks-time=7481&fb=1&mid=ATVPDKIKX0DER&plain-mid=1&client-info=search-ui"
            yield scrapy.Request(api_url, meta={'term': term, 'proxy': random.choice(PROXIES)},
                                 callback=self.parse_rule_1)
        else:
            self.logs[term]['Rule 5-9'] = f"Failed: {result}"
            self.skipped_list.remove(term)
            yield {term: f"Rule 5-9 Failed: {result}"}
            self.save_logs()

    def parse_rule_1(self, response, **kwargs):
        term = response.meta.get('term')
        try:
            data = json.loads(response.body)['suggestions']
        except:
            logging.info("Couldn't get any suggestion.")
            self.logs[term]['Rule 1'] = "Failed: Couldn't get any suggestion."
            self.skipped_list.remove(term)
            yield {term: "Rule 1 Failed: Couldn't get any suggestion."}
            self.save_logs()
            return

        suggestions = []
        if len(data) >= 3:
            matching_suggestions = []
            for suggestion in data:
                value = suggestion.get('value', '').lower()
                suggestions.append(value)
                if value and term in value:
                    matching_suggestions.append(value)

            if len(matching_suggestions) >= 2:
                logging.info(f"Rule 1 passed for term {term}.")
                self.logs[term]['Rule 1'] = 'Passed'
                url = f"https://www.amazon.com/s?k={term}"
                meta = {'term': term, 'suggestions': suggestions, 'proxy': random.choice(PROXIES)}
                yield scrapy.Request(url, meta=meta, callback=self.parse_rule_2, headers=HEADERS)

            else:
                logging.info(f"Term {term} failed in Rule 1. {term} contains less than 2 in a suggestion.")
                self.logs[term]['Rule 1'] = f'Failed: {term} contains less than 2 in a suggestion.'
                self.skipped_list.remove(term)
                yield {term: f'Rule 1 Failed: {term} contains less than 2 in a suggestion.'}
                self.save_logs()
                return

        else:
            logging.info(f"Term {term} failed in Rule 1. Suggestions are less than 3.")
            self.logs[term]['Rule 1'] = f'Failed: {term} suggestions are less than 3.'
            self.skipped_list.remove(term)
            yield {term: f'Rule 1 Failed: {term} suggestions are less than 3.'}
            self.save_logs()

    def parse_rule_2(self, response, **kwargs):
        print('RULE 2 ==============')
        term = response.meta.get('term')

        if response.status != 200:
            logging.info(
                f"Rule 2 failed for term {term}. Status code are {response.status}.")
            self.logs[term]['Rule 2'] = f'Rule 2 failed for term {term}. Status code are {response.status}.'
            yield {term: f'Rule 2 failed for term {term}. Status code are {response.status}.'}
            self.save_logs()
            return
        suggestions = response.meta.get('suggestions')
        total_results = response.css('div[class="a-section a-spacing-small a-spacing-top-small"] > span::text').get('')
        total_results = total_results.replace(',', '')
        total_results = re.findall(r'\d+', total_results)
        if total_results:
            total_results = int(total_results[-1])
            if total_results <= 400:
                min_sale = minimum_total_sales_of_search_group_for_results(total_results)
                meta = {'term': term, 'total_results': total_results, 'min_sale': min_sale,
                        'searched_term': suggestions, 'proxy': random.choice(PROXIES)}
                self.logs[term]['Rule 2'] = 'Passed'
                self.save_logs()

                unique_urls = set()
                product_listing = []
                for product in response.css(
                        'div[class="a-section a-spacing-small puis-padding-left-small puis-padding-right-small"],  div[data-csa-c-type="item"]'):
                    if product.css('span:contains("Sponsored")'):
                        continue

                    bought_items = product.css("span.a-size-base:contains('bought')::text").get('')
                    bought_value = ''
                    if bought_items:
                        bought_value = bought_items.split()[0].rstrip('+')
                        bought_value = convert_abbreviated_number(bought_value)

                    try:
                        price = float(product.css(
                            "span.a-price > span.a-offscreen::text, span:contains('No featured offers available') + br +span.a-color-base::text").get(
                            '').lstrip('$'))
                    except:
                        price = 0

                    url = product.css("h2 > a::attr(href)").get()
                    if url in unique_urls:
                        continue
                    unique_urls.add(url)
                    product_listing.append({
                        "url": response.urljoin(url),
                        'name': product.css("h2 > a > span::text").get('').strip().lower(),
                        'bought_values': bought_value if bought_value else '',
                        'price': price,
                        'monthly_sale': bought_value * price if bought_value and price else 0
                    })

                if product_listing:
                    data = {
                        'main_url': response.url,
                        'data': product_listing,
                        'next_page_url': response.css('a.s-pagination-next::attr(href)').get()
                    }

                    meta['data'] = data

                    prompt = RULES.get('RULE_3').format(term, suggestions).strip()
                    data = get_gpt_payload(prompt)
                    yield scrapy.Request(
                        url=API_URL_GPT,
                        method='POST',
                        headers=GPT_HEADERS,
                        body=json.dumps(data),
                        meta=meta,
                        callback=self.parse_rule_3
                    )
            else:
                logging.info(
                    f"Rule 2 failed for term {term}. Total Items Results are more than 400.")
                self.logs[term]['Rule 2'] = f'Failed: {term} has more than 400 items'
                self.skipped_list.remove(term)
                yield {term: f'Rule 2 Failed: {term} has more than 400 items'}
                self.save_logs()
                return
        else:
            logging.info(
                f"Rule 2 failed for term {term}. No Total Results are found.")
            self.logs[term]['Rule 2'] = f'Failed: {term}, No Total Results are found'
            self.skipped_list.remove(term)
            yield {term: f'Rule 2 Failed: {term}, No Total Results are found'}
            self.save_logs()
            return

    def parse_rule_3(self, response, **kwargs):
        print('RULE 3 1 ==============')
        try:
            gpt_data = response.json()
            group_term = gpt_data.get('choices', [{}])[0].get('message', {}).get('content', '')
        except Exception as e:
            print(f"Error parsing GPT response: {e}")
            return

        meta = response.meta.copy()
        data = response.meta.get('data')
        product_listing = data['data']
        
        try:
            product_names = [product['name'] for product in product_listing]
            
            # Joining all names into a single string
            all_product_names = ", ".join(product_names)
            
            # Formatting the RULE_3_1 with the joined string
            prompt = RULES.get('RULE_3_1').format(group_term, all_product_names).strip()
            data = get_gpt_payload(prompt)
            response = requests.post(API_URL_GPT, headers=GPT_HEADERS, data=json.dumps(data))
            status = response.raise_for_status()
            gpt_response = response.json()
            gpt_response = gpt_response.get('choices', [{}])[0].get('message', {}).get('content', '')
            
            print(gpt_response)
            
            if gpt_response == 'True':
                logging.info(
                        f"Rule 3 1 success for term {group_term}..")
            else:
                logging.info(
                        f"Rule 3 1 failed for term {group_term}. Total Items Results are more than 400.")
                self.logs[group_term]['Rule 2'] = f'Failed: {group_term} has more than 400 items'
                self.skipped_list.remove(group_term)
                yield {group_term: f'Rule 2 Failed: {group_term} has more than 400 items'}
                self.save_logs()
                return
        
        except requests.RequestException as e:
            print(f"Request failed: {e}")
        
        
        index = 0
        meta['name_index'] = index
        meta['group_term'] = group_term
        meta['proxy'] = random.choice(PROXIES)
        
        prompt = RULES.get('RULE_3_2').format(group_term, product_listing[index]['name']).strip()
        data = get_gpt_payload(prompt)
        
        yield scrapy.Request(
            url=API_URL_GPT,
            method='POST',
            headers=GPT_HEADERS,
            body=json.dumps(data),
            meta=meta,
            callback=self.parse_rule_3_2
        )

    def parse_rule_3_2(self, response, **kwargs):

        try:
            gpt_data = response.json()
            result = gpt_data.get('choices', [{}])[0].get('message', {}).get('content', '')
        except:
            return

        term = response.meta.get('term')
        data = response.meta.get('data')
        group_term = response.meta.get('group_term')
        product_listing = data['data']
        index = response.meta.get('name_index') + 1

        if (result == "False") or (len(product_listing) < 15):
            term = response.meta.get('term')
            total_results = response.meta.get('total_results')
            min_sale = response.meta.get('min_sale')
            data = response.meta.get('data')
            product_listing = data['data']
            # SEARCH GROUP COMPLETED
            updated_data = [d for d in product_listing if d['price'] < 400]

            if len(updated_data) == 0:
                next_page_url = data.get('next_page_url')
                if not next_page_url:
                    self.logs[term]['Rule 3'] = f'Failed: {term} have no item < 400 price'
                    self.skipped_list.remove(term)
                    yield {term: f"f'Rule 3 Failed: {term} have no item < 400 price'"}
                    self.save_logs()
                    return
                else:
                    next_page_url = urljoin(BASE_URL, next_page_url)
                    listing_meta = {'term': term, 'total_results': total_results, 'min_sale': min_sale,
                                    'group_term': group_term, 'total_monthly_sale': 0, 'proxy': random.choice(PROXIES)}
                    yield scrapy.Request(next_page_url, meta=listing_meta, callback=self.parse_product_listing)
                    return

            total_monthly_sale = sum([d['monthly_sale'] for d in updated_data if d['monthly_sale'] > 0])

            updated_data_dict = {
                'main_url': data.get('main_url'),
                'data': updated_data,
                'next_page_url': data.get('next_page_url')

            }

            url = updated_data[0]['url']
            meta = {'term': term, 'total_results': total_results, 'min_sale': min_sale,
                    'group_term': group_term, 'data': updated_data_dict, 'index': 0,
                    'total_monthly_sale': total_monthly_sale, 'proxy': random.choice(PROXIES)}
            yield scrapy.Request(url, callback=self.parse_product_details_page, meta=meta)

            return
        elif index < len(product_listing[:15]):
            meta = response.meta.copy()
            meta['name_index'] = index
            meta['proxy'] = random.choice(PROXIES)
            prompt = RULES.get('RULE_3_2').format(group_term, product_listing[index]['name']).strip()
            data = get_gpt_payload(prompt)
            yield scrapy.Request(
                url=API_URL_GPT,
                method='POST',
                headers=GPT_HEADERS,
                body=json.dumps(data),
                meta=meta,
                callback=self.parse_rule_3_2
            )
        else:
            self.logs[term]['Rule 3'] = f'Failed: {term} does not match with consecutive 15 items.'
            self.skipped_list.remove(term)
            yield {term: f'Rule 3 Failed: {term} does not match with consecutive 15 items.'}
            self.save_logs()
            return

    def parse_product_details_page(self, response, **kwargs):
        if response.status != 200:
            pass
        # Extract data from response meta
        data = response.meta.get('data')
        index = response.meta.get('index')
        term = response.meta.get('term')
        total_results = response.meta.get('total_results')
        min_sale = response.meta.get('min_sale')
        group_term = response.meta.get('group_term')
        total_monthly_sale = response.meta.get('total_monthly_sale', 0)
        bundle_check = bool(response.css('div#bundleV2_feature_div > div.a-row * div.bundle-comp-title > a'))

        if bundle_check and not response.meta.get('bundle', False):
            # Process bundle URLs if present
            bundle_urls = response.css(
                'div#bundleV2_feature_div > div.a-row * div.bundle-comp-title > a::attr(href)').getall()
            for bundle_url in bundle_urls:
                meta = {'term': term, 'total_results': total_results, 'min_sale': min_sale,
                        'group_term': group_term, 'data': data, 'index': index,
                        'total_monthly_sale': total_monthly_sale, 'bundle': True, 'proxy': random.choice(PROXIES)}
                yield response.follow(bundle_url, meta=meta, callback=self.parse_product_details_page)
        else:
            # Extract product details
            product = data['data'][index]
            try:
                price = product['price'] or int(
                    response.css('span.aok-offscreen::text').get('').strip().lstrip('$').replace(
                        ',', '')) or 0
            except:
                price = 0
            if product['monthly_sale'] == 0:
                rank_subcate_1 = response.css("th:contains('Best Sellers Rank') ~ td span > span::text").get('').rstrip(
                    '(').split(' in ')

                if len(rank_subcate_1) < 2:
                    rank_subcate_1 = ''.join(
                        response.css('span.a-list-item:contains("Best Sellers Rank") ::text').getall()).replace(
                        'Best Sellers Rank:', '').strip().split('(')[0].strip().split(' in ')

                rank_subcate_2 = ''.join(response.css(
                    "th:contains('Best Sellers Rank') ~ td span > span + br + span ::text").getall()).rstrip('(').split(
                    ' in ')

                if len(rank_subcate_2) < 2:
                    rank_subcate_2 = ''.join(''.join(response.css(
                        'span.a-list-item:contains("Best Sellers Rank") > ul >li > span.a-list-item ::text').getall())).split(
                        ' in ')

                categories = [category.strip() for category in [rank_subcate_1[-1], rank_subcate_2[-1]]]
                ranks = [get_rank(rank.replace(',', '').strip('#')) for rank in
                         [rank_subcate_1[0].strip(), rank_subcate_2[0].strip()]]

                monthly_sales = []
                for category, rank in zip(categories, ranks):
                    if class_for_sales_rank.check_if_cat_exits(category):
                        rank_data = class_for_sales_rank.get_sales_cat(rank, category)
                        monthly_sales.append(rank_data["sales"] * int(price))

                if monthly_sales:
                    data['data'][index]['monthly_sale'] = monthly_sale = min(monthly_sales) if len(
                        monthly_sales) > 1 else \
                        monthly_sales[0]
                else:
                    data['data'][index]['monthly_sale'] = monthly_sale = 0

                total_monthly_sale += monthly_sale

            data['data'][index]['brand'] = response.css('td:contains("Brand") + td > span::text').get(
                '').strip() or response.css(
                'a#bylineInfo::text').get('').replace('Visit the', '').replace('Store', '').strip()

            if response.css('span:contains("a-color-price")'):
                data['data'].pop(index)

            # Move to the next product or process results if all products have been processed

            if index < len(data['data']) - 1:
                index += 1
                url = data['data'][index]['url']
                meta = {'term': term, 'total_results': total_results, 'min_sale': min_sale,
                        'group_term': group_term, 'data': data, 'index': index,
                        'total_monthly_sale': total_monthly_sale, 'proxy': random.choice(PROXIES)}

                yield response.follow(url, callback=self.parse_product_details_page, meta=meta)

            elif total_monthly_sale >= min_sale:
                self.logs[term]['Rule 3'] = f"Passed"
                self.save_logs()
                prompt = RULES.get('RULE_4').format(term).strip()
                payload_data = get_gpt_payload(prompt)
                yield scrapy.Request(
                    url=API_URL_GPT,
                    method='POST',
                    headers=GPT_HEADERS,
                    body=json.dumps(payload_data),
                    meta={'term': term, 'data': data, 'proxy': random.choice(PROXIES)},
                    callback=self.parse_rule_4
                )

            else:
                # Process the next page or finish processing if all products have been processed
                index += 1
                if index >= len(data['data']):
                    next_page_url = data.get('next_page_url')
                    if next_page_url:
                        meta = {'term': term, 'total_results': total_results, 'min_sale': min_sale,
                                'group_term': group_term, 'detail': True, 'total_monthly_sale': total_monthly_sale,
                                'data': data, 'proxy': random.choice(PROXIES)}
                        yield response.follow(next_page_url, callback=self.parse_product_listing, meta=meta)
                    else:
                        self.logs[term]['Rule 3'] = f"Failed: {term} doesn't match with min monthly sale."
                        self.skipped_list.remove(term)
                        yield {term: f"Rule 3 Failed: {term} doesn't match with min monthly sale."}
                        self.save_logs()
                        return False

    def parse_product_listing(self, response, **kwargs):
        if response.status != 200:
            pass
        term = response.meta.get('term')
        data = response.meta.get('data', {})
        group_term = response.meta.get('group_term')
        min_sale = response.meta.get('min_sale')
        total_results = response.meta.get('total_results')
        total_monthly_sale = response.meta.get('total_monthly_sale')

        unique_urls = set()
        product_listing_1 = data.get('data', [])
        product_listing_2 = []
        for product in response.css(
                'div[class="a-section a-spacing-small puis-padding-left-small puis-padding-right-small"],  div[data-csa-c-type="item"]'):
            if product.css('span:contains("Sponsored")'):
                continue

            bought_items = product.css("span.a-size-base:contains('bought')::text").get('')
            bought_value = ''
            if bought_items:
                bought_value = bought_items.split()[0].rstrip('+')
                bought_value = convert_abbreviated_number(bought_value)

            try:
                price = float(product.css(
                    "span.a-price > span.a-offscreen::text, span:contains('No featured offers available') + br +span.a-color-base::text").get(
                    '').lstrip('$'))
            except:
                price = 0

            url = product.css("h2 > a::attr(href)").get()
            if url in unique_urls:
                continue

            unique_urls.add(url)
            product_listing_2.append({
                "url": response.urljoin(url),
                'name': product.css("h2 > a > span::text").get('').strip().lower(),
                'bought_values': bought_value if bought_value else '',
                'price': price,
                'monthly_sale': bought_value * price if bought_value and price else 0
            })

        updated_data = [d for d in product_listing_2 if d['price'] < 400]

        if len(updated_data) == 0:
            next_page_url = response.css('a.s-pagination-next::attr(href)').get()
            if not next_page_url:
                self.logs[term]['Rule 3'] = f'Failed: {term} have no item < 400 price'
                self.skipped_list.remove(term)
                yield {term: f"f'Rule 3 Failed: {term} have no item < 400 price'"}

                self.save_logs()
                return
            else:
                next_page_url = urljoin(BASE_URL, next_page_url)

                listing_meta = {'term': term, 'total_results': total_results, 'min_sale': min_sale,
                                'group_term': group_term, 'total_monthly_sale': 0, 'data': data,
                                'proxy': random.choice(PROXIES)}
                yield scrapy.Request(next_page_url, meta=listing_meta, callback=self.parse_product_listing)
                return

        new_total_monthly_sale = sum([d for d in updated_data if d['monthly_sale']])
        total_monthly_sale += new_total_monthly_sale
        product_listing = product_listing_1 + updated_data
        index = len(product_listing_1)
        if product_listing:
            updated_data_dict = {
                'main_url': response.url,
                'data': product_listing,
                'next_page_url': response.css('a.s-pagination-next::attr(href)').get()
            }
            url = updated_data[0]['url']
            meta = {'term': term, 'total_results': total_results, 'min_sale': min_sale,
                    'group_term': group_term, 'data': updated_data_dict, 'index': index,
                    'detail': True, 'total_monthly_sale': total_monthly_sale, 'proxy': random.choice(PROXIES)
                    }
            yield response.follow(url, callback=self.parse_product_details_page, meta=meta)

    def parse_rule_4(self, response, **kwargs):
        term = response.meta.get('term')
        try:
            gpt_data = response.json()
            result = gpt_data.get('choices', [{}])[0].get('message', {}).get('content', '')
        except:
            return

        if result == 'True':
            data = response.meta.get('data')
            products = data['data']
            brands = {}
            for product in products:
                brand = product.get('brand', '')
                if not brand:
                    continue
                if brand not in brands.keys():
                    brands[brand] = product['monthly_sale']
                else:
                    brands[brand] += product['monthly_sale']

            len_brands = len(brands.keys())
            if len_brands > 1:
                for brand, monthly_sale in brands.items():
                    if monthly_sale >= 100:
                        self.logs[term]['Rule 4'] = f"Passed."
                        self.skipped_list.remove(term)
                        yield {term: f"Passed."}
                        self.save_logs()
                        return
                    else:
                        self.logs[term]['Rule 4'] = f"Failed: {term} has no brands > $100 sale."
                        self.skipped_list.remove(term)
                        yield {term: f"Rule 4 Failed: {term} has no brands > $100 sale."}
                        self.save_logs()
                        return
            else:
                self.logs[term]['Rule 4'] = f"Failed: {term} has {len_brands} brands."
                self.skipped_list.remove(term)
                yield {term: f"Rule 4 Failed: {term} has {len_brands} brands."}
                self.save_logs()
                return
        else:
            self.logs[term]['Rule 4'] = f"Passed."
            self.skipped_list.remove(term)
            yield {term: f"Passed."}
            self.save_logs()
            return

    def save_logs(self):
        with open('rule_logs.json', 'w') as f:
            json.dump(self.logs, f, indent=4)

    def close(spider, reason):
        with open('skipped.json', 'w') as f:
            json.dump(list(spider.skipped_list), f, indent=4)


class HandleMiddleware:
    def __init__(self, crawler):
        self.crawler = crawler
        self.delay = config_data.get('PAUSE_TIME')
        self.openai_api_url = 'https://api.openai.com/'

    @classmethod
    def from_crawler(cls, crawler):
        middleware = cls(crawler)
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


class ProxyRetryMiddleware(RetryMiddleware):
    def __init__(self, settings):
        super().__init__(settings)
        self.proxies = PROXIES
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

        if retries <= config_data.get('RETRY_TIME'):
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


if __name__ == "__main__":
    terms = open(config_data.get('INPUT_FILE_PATH'), mode='r')
    terms_list = terms.readlines()
    local_setting = get_project_settings()
    file_name = f"final_logs.json"
    local_setting['FEED_FORMAT'] = 'json'
    local_setting['FEED_URI'] = file_name
    local_setting['ROBOTSTXT_OBEY'] = False
    # local_setting['RETRY_TIMES'] = config_data.get('RETRY_TIME')
    # local_setting['RETRY_HTTP_CODES'] = [500, 502, 503, 504, 400, 403, 404, 408]
    # local_setting['ROTATING_PROXY_LIST_PATH'] = config_data.get('PROXIES_FILE_PATH')
    local_setting['DOWNLOADER_MIDDLEWARES'] = {
        #     'rotating_proxies.middlewares.RotatingProxyMiddleware': 610,
        #     'rotating_proxies.middlewares.BanDetectionMiddleware': 620,
        'Amazon.HandleMiddleware': 543,
        'Amazon.ProxyRetryMiddleware': 543
    }
    local_setting['HTTPERROR_ALLOW_ALL'] = True
    local_setting['DOWNLOAD_TIMEOUT'] = 1800
    local_setting[
        'USER_AGENT'] = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.82 Safari/537.36'
    local_setting['LOG_LEVEL'] = 'DEBUG'
    local_setting['CONCURRENT_REQUESTS'] = config_data.get('CONCURRENT_REQUESTS')
    process = CrawlerProcess(local_setting)
    crawler = process.create_crawler(AmazonSpider)
    process.crawl(crawler)
    process.start()