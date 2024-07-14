# -*- coding: utf-8 -*-
import scrapy
import re
import pytz
import logging
from scrapy.http import Request, FormRequest
from datetime import datetime
from AmazonScraper.items import AmazonscraperItem


headers = {
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
    'accept-encoding': 'gzip, deflate',
    'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8,ml;q=0.7',
    'upgrade-insecure-requests': '1',
    'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.88 Safari/537.36',
}


class DataParserSpider(scrapy.Spider):
    name = 'data_parser'
    allowed_domains = ['amazon.com']

    # The category variable will have the input URL.
    def __init__(self, product_url='', **kwargs):
        self.product_url = 'https://www.amazon.com/dp/' + \
            product_url if 'https://' not in product_url else product_url
        super().__init__(**kwargs)

    custom_settings = {
        'FEED_URI': 'AmazonScraper/outputfile.json', 'CLOSESPIDER_TIMEOUT': 15}

    def start_requests(self):
        yield Request(url=self.product_url, callback=self.parse, headers=headers)

    def parse(self, response):
        ASIN_XPATH = '//div/@data-asin'
        NAME_XPATH = '//h1[@id="title"]/span/text()'
        SALE_PRICE_XPATH = '//div[@id="price"]//tr[td[text()="Sale:"]]/td[2]/span[@id="priceblock_saleprice"]/text()'
        SALE_PRICE_XPATH_1 = '//div[@id="price"]//tr[td[text()="Price:"]]/td[2]/span[@id="priceblock_ourprice"]/text()'
        SALE_PRICE_XPATH_2 = '//div[@id="price"]//tr[td[text()="With Deal:"]]/td[2]/span[@id="priceblock_dealprice"]/text()'
        LIST_PRICE_XPATH = '//div[@id="price"]//tr[td[text()="List Price:"]]/td[2]//text()'
        IMAGES_XPATH = '//script[contains(., \'register("ImageBlockATF"\')]/text()'
        BRAND_XPATH = '//a[@id="brand"]/text() | //div[@data-feature-name="bylineInfo"]//a[@id="bylineInfo"]/text()'
        RATING_XPATH = '//span[@class="a-size-medium a-color-base"]/text()'
        REVIEW_COUNT_XPATH = '//span[@class="a-size-base a-color-secondary"]/text()'
        STOCK_AVAILABILITY_XPATH = '//div[@id="availability"]/span//text()'
        BREADCRUMB_PATH = '//div[contains(@id,"breadcrumb")]/ul/li/span[@class="a-list-item"]/a/text()'
        ATTRIBUTE_TABLE_XPATH = '//table[@id="productDetails_techSpec_section_1"]//tr | //table[@id="productDetails_techSpec_section_2"]//tr'
        ATTRIBUTE_TABLE_XPATH1 = '//table[@id="productDetails_techSpec_section_2"]//tr'
        ATTRIBUTE_TABLE_XPATH2 = '//td[h2/text()="Product details"]/div[@class="content"]/ul/li'
        ATTRIBUTE_TABLE_XPATH3 = '//table[@id="productDetails_detailBullets_sections1"]//tr'
        ATTRIBUTE_TABLE_XPATH4 = '//table[@id="product-specification-table"]//tr'
        ATTRIBUTE_TABLE_XPATH5 = '//table[@id="technical-details-table"]//tr'
        ATTRIBUTE_TABLE_XPATH6 = '//table[@id="technicalSpecifications_section_1"]//tr'
        SELLER_XPATH = '//div[@id="merchant-info"]//text()'
        DESCRIPTION_XPATH = '//div[@id="productDescription"]//p/text()'
        DESCRIPTION_XPATH2 = '//h2[text()="Product Description"]/following-sibling::div//p//text() |//h2[text()="From the manufacturer"]/following-sibling::div//p//text()'
        RANK1_XPATH_1 = '//li[@id="SalesRank"]/text()[contains(self::text(), "in")]'
        RANK1_XPATH_2 = '//tr[@id="SalesRank"]/td/text()[contains(self::text(), "in")]'
        RANK1_XPATH_3 = '//tr/th[contains(text(),"Best Sellers Rank")]/following-sibling::td/span/span[1]/text()[contains(self::text(), "in")]'
        RANK1_CATEGORY_XPATH = '//tr/th[contains(text(),"Best Sellers Rank")]/following-sibling::td/span/span[1]//text()'
        RANK2_XPATH = '//li[@class="zg_hrsr_item"][1]/span[@class="zg_hrsr_rank"]/text()|//tr/th[contains(text(),"Best Sellers Rank")]/following-sibling::td/span/span[2]/text()[contains(self::text(), "in")]'
        RANK2_CATEGORY_XPATH = '//li[@class="zg_hrsr_item"][1]/span[@class="zg_hrsr_ladder"]//a/text()|//tr/th[contains(text(),"Best Sellers Rank")]/following-sibling::td/span/span[2]//text()'
        RANK3_XPATH = '//li[@class="zg_hrsr_item"][2]/span[@class="zg_hrsr_rank"]/text()|//tr/th[contains(text(),"Best Sellers Rank")]/following-sibling::td/span/span[3]/text()[contains(self::text(), "in")]'
        RANK3_CATEGORY_XPATH = '//li[@class="zg_hrsr_item"][2]/span[@class="zg_hrsr_ladder"]//a/text()|//tr/th[contains(text(),"Best Sellers Rank")]/following-sibling::td/span/span[3]//text()'
        RANK4_XPATH = '//li[@class="zg_hrsr_item"][3]/span[@class="zg_hrsr_rank"]/text()|//tr/th[contains(text(),"Best Sellers Rank")]/following-sibling::td/span/span[4]/text()[contains(self::text(), "in")]'
        RANK4_CATEGORY_XPATH = '//li[@class="zg_hrsr_item"][3]/span[@class="zg_hrsr_ladder"]//a/text()|//tr/th[contains(text(),"Best Sellers Rank")]/following-sibling::td/span/span[4]//text()'
        DATE_FIRST_AVAILABLE_XPATH = '//tr[@class="date-first-available"]/td[@class="value"]/text() | //li[b[contains(translate(text(), "ABCDEFGHIJKLMNOPQRSTUVWXYZ", "abcdefghijklmnopqrstuvwxyz"), "date first available")]]/text() | //li/span[span[contains(translate(text(), "ABCDEFGHIJKLMNOPQRSTUVWXYZ", "abcdefghijklmnopqrstuvwxyz"),"date first available")]]/span[2]/text() | //tr/th[contains(translate(text(), "ABCDEFGHIJKLMNOPQRSTUVWXYZ", "abcdefghijklmnopqrstuvwxyz"),"date first available")]/following-sibling::td/text()'
        PRODUCT_DESCRIPTION_XPATH = '//div[@id="productDescription"]//text() | //div[@class="a-section launchpad-module launchpad-module-brand-description-left"]//text()'
        DATE_FIRST_AVAILABLE_XPATH = '//tr[@class="date-first-available"]/td[@class="value"]/text() | //li[b[contains(translate(text(), "ABCDEFGHIJKLMNOPQRSTUVWXYZ", "abcdefghijklmnopqrstuvwxyz"), "date first available")]]/text() | //li/span[span[contains(translate(text(), "ABCDEFGHIJKLMNOPQRSTUVWXYZ", "abcdefghijklmnopqrstuvwxyz"),"date first available")]]/span[2]/text() | //tr/th[contains(translate(text(), "ABCDEFGHIJKLMNOPQRSTUVWXYZ", "abcdefghijklmnopqrstuvwxyz"),"date first available")]/following-sibling::td/text()'

        asin = response.xpath(ASIN_XPATH).extract()
        asin = asin[0].strip() if asin else ''
        product_name = response.xpath(NAME_XPATH).extract()
        product_name = product_name[0].strip() if product_name else ''
        sale_price = response.xpath(SALE_PRICE_XPATH).extract()
        sale_price_1 = response.xpath(SALE_PRICE_XPATH_1).extract()
        sale_price_2 = response.xpath(SALE_PRICE_XPATH_2).extract()
        list_price = response.xpath(LIST_PRICE_XPATH).extract()
        if list_price:
            list_price = ''.join(list_price).strip() if list_price else ''
            list_price = re.findall(
                r'\$\d{0,10}\.\d{0,2}', list_price) if list_price else ''
            list_price = list_price[0] if list_price else ''
            if list_price:
                list_price = list_price.replace('$', '')
                l_price = list_price
        else:
            l_price = ''
        if 'Lower price available on select options' in sale_price:
            sale_price = ''
        if 'Lower price available on select options' in sale_price_1:
            sale_price_1 = ''
        if 'Lower price available on select options' in sale_price_2:
            sale_price_2 = ''
        sale_price = sale_price if sale_price else sale_price_1
        sale_price = sale_price if sale_price else sale_price_2
        if sale_price:
            sale_price = ''.join(sale_price).strip().replace('$', '')
        else:
            sale_price = ''

        images_text = response.xpath(IMAGES_XPATH).extract()
        images_list = []
        if images_text:
            images_text = images_text[0].strip() if images_text else ''
            images_text = ''.join(images_text.split('\n'))
            large_images = re.findall('large":".*?"', images_text)
            for large_image in large_images:
                images = large_image.replace(
                    'large":"', '').replace('"', '').strip()
                images_list.append(images)
        images_list = images_list if images_list else ''

        brand = response.xpath(BRAND_XPATH).extract()
        brand = brand[0].replace('Brand:', '').replace(
            'Visit the ', '').strip() if brand else ''
        rating = response.xpath(RATING_XPATH).extract()
        rating = rating[0].split()[0] if rating else ''
        review_count = response.xpath(REVIEW_COUNT_XPATH).extract()
        review_count = review_count[-1].split()[0].replace(
            ',', '') if review_count else ''

        

        breadcrumb = response.xpath(BREADCRUMB_PATH).extract()
        breadcrumb = [x.strip() for x in breadcrumb if x.strip()]
        breadcrumb = ' > '.join(breadcrumb) if breadcrumb else ''
        stock_availability_text = response.xpath(
            STOCK_AVAILABILITY_XPATH).extract()
        stock_availability_text = ' '.join(
            ' '.join(stock_availability_text).split()).strip() if stock_availability_text else ''
        stock_availability_text = stock_availability_text.lower()
        if 'in stock' in stock_availability_text:
            availability = 'Yes'
        elif 'out of stock' in stock_availability_text:
            availability = 'No'
        else:
            availability = ''

        description = response.xpath(DESCRIPTION_XPATH).extract()
        description = ' '.join(
            ''.join(description).split()) if description else ''
        if not description:
            description = response.xpath(DESCRIPTION_XPATH2).extract()
            description = ' '.join(
                ''.join(description).split()) if description else ''
        item_attribute = {}
        if response.xpath(ATTRIBUTE_TABLE_XPATH5):
            item_attribute = {}
            for i in response.xpath(ATTRIBUTE_TABLE_XPATH5):
                key = i.xpath('td[1]//text()').extract()
                value = i.xpath('td[2]//text()').extract()
                key = ''.join(key).strip(':').replace(
                    ':', '').replace('.', '_').strip() if key else 'N/A'
                value = ''.join(value).strip(':').replace(
                    ':', '').strip() if value else 'N/A'
                if key.strip():
                    item_attribute[key] = value if value else 'N/A'

        if not item_attribute:
            item_attribute = {}
            for i in response.xpath(ATTRIBUTE_TABLE_XPATH4):
                key = i.xpath('th//text()').extract()
                value = i.xpath('td//text()').extract()
                key = ''.join(key).strip(':').replace(
                    ':', '').replace('.', '_').strip() if key else 'N/A'
                value = ''.join(value).strip(':').replace(
                    ':', '').strip() if value else 'N/A'
                if key.strip():
                    item_attribute[key] = value if value else 'N/A'

        if not item_attribute:
            item_attribute = {}
            if response.xpath(ATTRIBUTE_TABLE_XPATH):
                for i in response.xpath(ATTRIBUTE_TABLE_XPATH):
                    key = i.xpath('th//text()').extract()
                    value = i.xpath('td//text()').extract()
                    key = ''.join(key).strip(':').replace(
                        ':', '').replace('.', '_').strip() if key else 'N/A'
                    value = ''.join(value).strip(':').replace(
                        ':', '').strip() if value else 'N/A'
                    if key.strip():
                        item_attribute[key] = value if value else 'N/A'

        if not item_attribute:
            if response.xpath(ATTRIBUTE_TABLE_XPATH2):
                item_attribute = {}
                for i in response.xpath(ATTRIBUTE_TABLE_XPATH2):
                    key = i.xpath('b/text()').extract()
                    value = i.xpath(
                        'b/following-sibling::text()').extract()
                    key = ''.join(key).strip(':').replace(
                        ':', '').replace('.', '_').strip() if key else 'N/A'
                    value = ''.join(value).strip(':').replace(
                        ':', '').strip() if value else 'N/A'
                    if key.strip():
                        item_attribute[key] = value if value else 'N/A'

        if not item_attribute:
            if response.xpath(ATTRIBUTE_TABLE_XPATH3):
                item_attribute = {}
                for i in response.xpath(ATTRIBUTE_TABLE_XPATH3):
                    key = i.xpath('th//text()').extract()
                    value = i.xpath('td//text()').extract()
                    key = ''.join(key).strip(':').replace(
                        ':', '').replace('.', '_').strip() if key else 'N/A'
                    value = ''.join(value).strip(':').replace(
                        ':', '').strip() if value else 'N/A'
                    if key.strip():
                        item_attribute[key] = value if value else 'N/A'

        if not item_attribute:
            if response.xpath(ATTRIBUTE_TABLE_XPATH6):
                item_attribute = {}
                for i in response.xpath(ATTRIBUTE_TABLE_XPATH6):
                    key = i.xpath('th//text()').extract()
                    value = i.xpath('td//text()').extract()
                    key = ''.join(key).strip(':').replace(
                        ':', '').replace('.', '_').strip() if key else 'N/A'
                    value = ''.join(value).strip(':').replace(
                        ':', '').strip() if value else 'N/A'
                    if key.strip():
                        item_attribute[key] = value if value else 'N/A'

        if not item_attribute:
            item_attribute = ''

        choices = response.xpath(
            '//span[@class="a-size-small aok-float-left ac-badge-rectangle"]/span/text() |//i[@class="a-icon a-icon-addon p13n-best-seller-badge"]/text() ').extract()
        choices = ''.join(choices).strip() if choices else ''
        if 'Best Seller' in choices or 'Amazon' in choices:
            choices = choices
        else:
            choices = ''
        rank1 = response.xpath(RANK1_XPATH_1).extract()
        _rank1 = response.xpath(RANK1_XPATH_2).extract()
        __rank1 = response.xpath(RANK1_XPATH_3).extract()
        rank2 = response.xpath(RANK2_XPATH).extract()
        rank2_category = response.xpath(RANK2_CATEGORY_XPATH).extract()
        rank3 = response.xpath(RANK3_XPATH).extract()
        rank3_category = response.xpath(RANK3_CATEGORY_XPATH).extract()
        rank4 = response.xpath(RANK4_XPATH).extract()
        rank4_category = response.xpath(RANK4_CATEGORY_XPATH).extract()
        rank1 = rank1 if rank1 else _rank1
        rank1 = rank1 if rank1 else __rank1
        rank1_category = rank1[0].split(
            ' in ')[-1].strip().strip('(').strip() if ' in ' in ''.join(rank1) else ''
        if not rank1_category:
            rank1_category = response.xpath(RANK1_CATEGORY_XPATH).extract()
            rank1_category = ' > '.join(
                rank1_category) if rank1_category else ''
            # rank1_category = ' > '.join(rank1_category).replace('(See top 100)','') if rank1_category else ''
            # rank1_category = re.sub(r'\#.*\sin',r'',''.join(rank1_category)).strip().replace('(See top 100)','').strip() if rank1_category else ''
        rank1 = rank1[0].split(' in ')[0].strip().strip(
            '#').lower().replace('paid', '').strip() if ' in ' in ''.join(rank1) else ''
        rank2 = rank2[0].strip('#').lower().replace(
            'paid', '').strip() if rank2 else ''
        # rank2_category = ' > '.join(rank2_category).replace('(See top 100)','') if rank2_category else ''
        rank2_category = re.sub(r'\#.*\sin', r'', ''.join(rank2_category)).strip(
        ).replace('(See top 100)', '').strip() if rank2_category else ''
        rank3 = rank3[0].strip('#').lower().replace(
            'paid', '').strip() if rank3 else ''
        # rank3_category = ' > '.join(rank3_category).replace('(See top 100)','') if rank3_category else ''
        rank3_category = re.sub(r'\#.*\sin', r'', ''.join(rank3_category)).strip(
        ).replace('(See top 100)', '').strip() if rank3_category else ''
        rank4 = rank4[0].strip('#').lower().replace(
            'paid', '').strip() if rank4 else ''
        # rank4_category = ' > '.join(rank4_category).replace('(See top 100)','') if rank4_category else ''
        rank4_category = re.sub(r'\#.*\sin', r'', ''.join(rank4_category)).strip(
        ).replace('(See top 100)', '').strip() if rank4_category else ''
        if not rank1:
            rank1 = rank2
            rank1_category = rank2_category
            rank2 = rank3
            rank2_category = rank3_category
            rank3 = rank4
            rank3_category = rank4_category
            rank4 = rank4_category = ''
        rank1 = re.sub(r'(\s*in\s*|\,)', r'', rank1, re.I)
        rank2 = re.sub(r'(\s*in\s*|\,)', r'', rank2, re.I)
        rank3 = re.sub(r'(\s*in\s*|\,)', r'', rank3, re.I)
        rank4 = re.sub(r'(\s*in\s*|\,)', r'', rank4, re.I)
        datefirstavailable = response.xpath(
            DATE_FIRST_AVAILABLE_XPATH).extract()
        datefirstavailable = datefirstavailable[0].strip(
        ) if datefirstavailable else ''
        ranks = {}
        country = ''
        ranks.update({'rank1': rank1, 'rank1_category': rank1_category}
                     ) if rank1 and rank1_category else ''
        ranks.update({'rank2': rank2, 'rank2_category': rank2_category}
                     ) if rank2 and rank2_category else ''
        ranks.update({'rank3': rank3, 'rank3_category': rank3_category}
                     ) if rank3 and rank3_category else ''
        ranks.update({'rank4': rank4, 'rank4_category': rank4_category}
                     ) if rank4 and rank4_category else ''
        if not ranks:
            item_attribute = {}
            rank_list = response.xpath(
                '//ul[@class="a-unordered-list a-nostyle a-vertical a-spacing-none detail-bullet-list"]/li')
            for ran in rank_list:
                list_key = ran.xpath(
                    'span[@class="a-list-item"]/span/text()').extract()
                list_key = ''.join(list_key).strip() if list_key else ''
                key_ = ran.xpath(
                    'span[@class="a-list-item"]/span[@class="a-text-bold"]/text()').extract()
                key_ = ''.join(key_).strip().replace('\n', '').replace(':', '')
                values = ran.xpath(
                    'span[@class="a-list-item"]/span[not(@class="a-text-bold")]/text()').extract()
                values = ''.join(values).strip().replace('\n', '')
                if key_ and values and not 'Date First Available' in key_:
                    item_attribute.update({str(key_): str(values)})

                if 'Best Sellers Rank' in list_key:
                    rank1 = ran.xpath(
                        'span[@class="a-list-item"]/text()').extract()
                    rank1 = [x.strip() for x in rank1 if x.strip()]
                    rank2 = ran.xpath(
                        'span[@class="a-list-item"]/ul[@class="a-unordered-list a-nostyle a-vertical zg_hrsr"]/li[1]/span//text()').extract()
                    rank2 = [y.strip() for y in rank2 if y.strip()]
                    rank3 = ran.xpath(
                        'span[@class="a-list-item"]/ul[@class="a-unordered-list a-nostyle a-vertical zg_hrsr"]/li[2]/span//text()').extract()
                    rank3 = [z.strip() for z in rank3 if z.strip()]
                    rank4 = ran.xpath(
                        'span[@class="a-list-item"]/ul[@class="a-unordered-list a-nostyle a-vertical zg_hrsr"]/li[3]/span//text()').extract()
                    rank4 = [m.strip() for m in rank4 if m.strip()]
                    rank2_category = rank2[1] if rank2 else ''
                    rank3_category = rank3[1] if rank3 else ''
                    rank4_category = rank4[1] if rank4 else ''

                    rank1_category = rank1[0].split(
                        ' in ')[-1].strip().strip('(').strip() if ' in ' in ''.join(rank1) else ''
                    if not rank1_category:
                        rank1_category = response.xpath(
                            RANK1_CATEGORY_XPATH).extract()
                        rank1_category = ' > '.join(
                            rank1_category) if rank1_category else ''
                    rank1 = rank1[0].split(' in ')[0].strip().strip(
                        '#').lower().replace('paid', '').strip() if ' in ' in ''.join(rank1) else ''
                    rank2 = rank2[0].strip('#').lower().replace(
                        'paid', '').strip() if rank2 else ''
                    rank2_category = re.sub(r'\#.*\sin', r'', ''.join(rank2_category)).strip(
                    ).replace('(See top 100)', '').strip() if rank2_category else ''
                    rank3 = rank3[0].strip('#').lower().replace(
                        'paid', '').strip() if rank3 else ''
                    rank3_category = re.sub(r'\#.*\sin', r'', ''.join(rank3_category)).strip(
                    ).replace('(See top 100)', '').strip() if rank3_category else ''
                    rank4 = rank4[0].strip('#').lower().replace(
                        'paid', '').strip() if rank4 else ''
                    rank4_category = re.sub(r'\#.*\sin', r'', ''.join(rank4_category)).strip(
                    ).replace('(See top 100)', '').strip() if rank4_category else ''
                    if not rank1:
                        rank1 = rank2
                        rank1_category = rank2_category
                        rank2 = rank3
                        rank2_category = rank3_category
                        rank3 = rank4
                        rank3_category = rank4_category
                        rank4 = rank4_category = ''
                    rank1 = re.sub(r'(\s*in\s*|\,)', r'', rank1, re.I)
                    rank2 = re.sub(r'(\s*in\s*|\,)', r'', rank2, re.I)
                    rank3 = re.sub(r'(\s*in\s*|\,)', r'', rank3, re.I)
                    rank4 = re.sub(r'(\s*in\s*|\,)', r'', rank4, re.I)
                    ranks = {}
                    ranks.update({'rank1': rank1, 'rank1_category': rank1_category}
                                 ) if rank1 and rank1_category else ''
                    ranks.update({'rank2': rank2, 'rank2_category': rank2_category}
                                 ) if rank2 and rank2_category else ''
                    ranks.update({'rank3': rank3, 'rank3_category': rank3_category}
                                 ) if rank3 and rank3_category else ''
                    ranks.update({'rank4': rank4, 'rank4_category': rank4_category}
                                 ) if rank4 and rank4_category else ''
        currency = 'USD'
        product_description = response.xpath(
            PRODUCT_DESCRIPTION_XPATH).extract()
        if product_description:
            Description_list = [i.strip().replace('\xa0', '').replace('|', '').replace('\n', '').strip()
                                for i in product_description if i.strip()]
            description = "\n".join(Description_list)
            description = description.replace('\n', ' ').replace(',', ' ')
            # description = ''.join(product_description).strip().replace('\xa0','').replace('\xa0x\xa0','')
        else:
            description = ''
        datefirstavailable = response.xpath(
            DATE_FIRST_AVAILABLE_XPATH).extract()
        datefirstavailable = datefirstavailable[0].strip(
        ) if datefirstavailable else ''
        product_url = response.url
        load_timestamp = str(datetime.now(
            pytz.timezone('America/Chicago'))).split('.')[0]

        items = AmazonscraperItem()
        items['product_url'] = product_url
        items['asin'] = asin
        items['product_name'] = product_name
        items['list_price'] = l_price
        items['sale_price'] = sale_price
        items['brand'] = brand
        items['rating'] = rating
        items['images'] = images_list
        items['currency'] = currency
        items['review_count'] = review_count
        items['availability'] = availability
        items['breadcrumb'] = breadcrumb
        items['attributes'] = item_attribute
        items['description'] = description
        items['ranks'] = ranks
        items['choices'] = choices
        items['date_first_available'] = datefirstavailable
        items['image_urls'] = images_list
        items['timestamp'] = load_timestamp
        yield items
