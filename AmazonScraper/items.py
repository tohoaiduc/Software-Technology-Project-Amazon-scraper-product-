# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

import scrapy
from scrapy.item import Item, Field


class AmazonscraperItem(scrapy.Item):
    product_url = scrapy.Field()
    asin = scrapy.Field()
    product_name = scrapy.Field()
    list_price = scrapy.Field()
    sale_price = scrapy.Field()
    brand = scrapy.Field()
    rating = scrapy.Field()
    images = scrapy.Field()
    currency = scrapy.Field()
    review_count = scrapy.Field()
    availability = scrapy.Field()
    breadcrumb = scrapy.Field()
    attributes = scrapy.Field()
    description = scrapy.Field()
    ranks = scrapy.Field()
    choices = scrapy.Field()
    date_first_available = scrapy.Field()
    image_urls = scrapy.Field()
    timestamp = scrapy.Field()
