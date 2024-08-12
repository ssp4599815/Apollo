import logging

import scrapy
from scrapy import Request, Selector

from ..items import FulijiItem


class NungvlSpider(scrapy.Spider):
    name = "nungvl"
    allowed_domains = ["nungvl.net", "wp.com"]

    def start_requests(self):
        for page in range(1, 2):
            yield Request(url=f'https://nungvl.net/?page={page}')

    def parse(self, response):
        sel = Selector(response)
        fuliji_titles = sel.xpath('//img[@class="xld"]/@alt').extract()
        fuliji_hrefs = sel.xpath('//a[@class="denomination"]/@href').extract()

        for title, href in zip(fuliji_titles, fuliji_hrefs):
            item = FulijiItem()
            item['title'] = title
            complete_url = response.urljoin(href)  # Combining base url with href
            logging.info("complete_url: %s" % complete_url)
            item['href'] = complete_url
            yield Request(url=complete_url, callback=self.parse_details, meta={'item': item})

    def parse_details(self, response):
        item = response.meta['item']
        src_links = response.xpath('//div[@class="contentme"]/a//img/@src').extract()
        if src_links:
            item['image_urls'] = [response.urljoin(src) for src in src_links]  # Generate absolute URLs
            logging.info(f"image_urls: {item['image_urls']}")
            yield item

            # 尝试获取当前页码
            current_page = response.meta.get('page', 1)
            next_page = current_page + 1

            if next_page <= 100:
                next_page_url = f"{item['href']}?page={next_page}"  # 这里假设页面URL结构支持此操作
                logging.info(f"Requesting next page: {next_page_url}")
                yield Request(url=next_page_url, callback=self.parse_details, meta={'item': item, 'page': next_page})
        else:
            logging.info(f"No more links found, stopping further requests.")
