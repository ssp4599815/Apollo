# !/usr/bin/env python
# -*-coding:utf-8 -*-
import logging
import os
import shelve

import scrapy
from scrapy import Selector, Request

from ..items import FulijiItem


class SfnmtSpider(scrapy.Spider):
    name = "sfnmt"
    # 自定义相关配置
    HOME_PATH = os.path.expanduser("~")
    custom_settings = {
        'IMAGES_STORE': os.path.join(HOME_PATH, "Documents/图片/sfnmt.com")
    }

    def __init__(self, *args, **kwargs):
        super(SfnmtSpider, self).__init__(*args, **kwargs)
        # 打开一个shelve数据库存储已经访问过的URL
        self.visited_urls_db = shelve.open("./temp/visited_sfnmt_urls")

    def start_requests(self):
        # 从数据库中读取已经访问过的URL
        for page in range(1, 5):
            yield Request("http://www.sfnmt.com/taotu/listhtnl/25_{}.html".format(page))

    def parse(self, response):
        # 获取当前页面的所有图片链接
        sel = Selector(response)
        sfnmt_titles = sel.xpath("//div[@class='Title']/a/text()").extract()
        sfnmt_hrefs = sel.xpath("//div[@class='Title']/a/@href").extract()

        for title, href in zip(sfnmt_titles, sfnmt_hrefs):
            item = FulijiItem()
            item['title'] = ' '.join(title.strip().split())
            item['site'] = 'sfnmt.com'

            complete_url = response.urljoin(href)

            # 如果URL不在数据库中，则发起请求
            if complete_url in self.visited_urls_db:
                logging.info(f"Skipping {complete_url}, already visited.")
                continue
            self.visited_urls_db[complete_url] = True

            logging.info("complete_url: %s" % complete_url)
            item['href'] = complete_url
            yield Request(url=complete_url, callback=self.parse_details, meta={'item': item})

    def parse_details(self, response):
        item = response.meta['item']
        if 'image_urls' not in item:
            item['image_urls'] = []

        src_links = response.xpath("//div[@id='picg']//a//img/@src").extract()
        # logging.info(f"src_links: {src_links}")
        if src_links:
            item['image_urls'].extend([response.urljoin(src) for src in src_links])
            # logging.info(f"image_urls: {item['image_urls']}")

            # 提取所有数字
            page_numbers = response.xpath("//div[@class='pagelist']/a/text()").extract()
            # 过滤出所有整数（您可以通过isdigit函数检查字符串是否只包含数字）
            page_numbers_int = [int(x) for x in page_numbers if x.isdigit()]
            # 获取最大整数
            max_page = max(page_numbers_int) if page_numbers_int else 2
            current_page = response.meta.get('page', 1)
            next_page = current_page + 1

            if next_page <= max_page:
                page_base_url = response.url.rsplit('.', 1)[0]
                # Remove any existing page number from the base URL if present
                if '_' in page_base_url:
                    page_base_url = page_base_url.rsplit('_', 1)[0]

                next_page_url = f"{page_base_url}_{next_page}.html"
                logging.info(f"Requesting next page: {next_page_url}")
                yield Request(url=next_page_url, callback=self.parse_details, meta={'item': item, 'page': next_page})
            else:
                logging.info(f"No more links found, stopping further requests.")
                yield item
        else:
            logging.info(f"No more links found, stopping further requests.")
            yield item

    def closed(self, reason):
        # 当爬虫关闭时，确保我们也关闭了shelve数据库
        self.visited_urls_db.close()
