#!/usr/bin/env python
# -*-coding:utf-8 -*-
import logging
import os
import shelve
import scrapy
from scrapy import Request, Selector

from ..items import FulijiItem
from ..utils.logger_config import SpiderLoggerMixin


class NungvlSpider(SpiderLoggerMixin, scrapy.Spider):
    name = "nungvl"
    allowed_domains = ["nungvl.net", "wp.com"]

    # 设置图片存储路径
    HOME_PATH = os.path.expanduser("~")
    custom_settings = {
        'IMAGES_STORE': os.path.join(HOME_PATH, "Documents/图片/nungvl.net")
    }

    def __init__(self, *args, **kwargs):
        super(NungvlSpider, self).__init__(*args, **kwargs)
        # 使用统一的日志配置
        self.setup_spider_logger()
        # 打开一个shelve数据库存储已经访问过的URL
        self.visited_urls_db = shelve.open("./temp/visited_nungvl_urls")

    def start_requests(self):
        for page in range(1, 2):
            yield Request(url=f'https://nungvl.net/?page={page}')

    def parse(self, response):
        sel = Selector(response)
        fuliji_titles = sel.xpath('//img[@class="xld"]/@alt').extract()
        fuliji_hrefs = sel.xpath('//a[@class="denomination"]/@href').extract()

        for title, href in zip(fuliji_titles, fuliji_hrefs):
            item = FulijiItem()
            item['title'] = title.strip()
            item['site'] = 'nungvl.net'

            complete_url = response.urljoin(href)  # Combining base url with href

            # 检查接口是否已经被访问过
            if complete_url in self.visited_urls_db:
                self.log(f"Skipping {complete_url}, already visited.", logging.INFO)
                continue
            self.visited_urls_db[complete_url] = True  # Mark the URL as visited

            self.log(f"Requesting URL: {complete_url}", logging.INFO)
            item['href'] = complete_url
            yield Request(url=complete_url, callback=self.parse_details, meta={'item': item})

    def parse_details(self, response):
        item = response.meta['item']
        if 'image_urls' not in item:
            item['image_urls'] = []

        src_links = response.xpath('//div[@class="contentme"]/a//img/@src').extract()
        if src_links:
            item['image_urls'].extend([response.urljoin(src) for src in src_links])  # 尝试获取当前页码
            current_page = response.meta.get('page', 1)
            next_page = current_page + 1

            if next_page <= 10:
                next_page_url = f"{item['href']}?page={next_page}"  # 这里假设页面URL结构支持此操作
                self.log(f"Requesting next page: {next_page_url}", logging.INFO)
                yield Request(url=next_page_url, callback=self.parse_details, meta={'item': item, 'page': next_page})
        else:
            yield item

    def closed(self, reason):
        # 当爬虫关闭时，确保我们也关闭了shelve数据库
        self.visited_urls_db.close()
        self.log(f"爬虫关闭，原因: {reason}", logging.INFO)
