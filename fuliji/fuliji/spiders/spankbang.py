import logging

import scrapy
from scrapy import Selector

from ..items import SpankBangItem


class SpankbangSpider(scrapy.Spider):
    name = "spankbang"
    allowed_domains = ["spankbang.com"]
    start_urls = ["https://spankbang.com/7tdxv/playlist/porn"]
    handle_httpstatus_list = [403]


    def parse(self, response):
        if response.status == 403:
            # 在这里记录额外的调试信息
            self.logger.error(f"Request got a forbidden response: {response.url}")
            self.logger.error(f"Response headers: {response.headers}")
            # 如有必要，可以尝试利用更多的响应内容进行调试
            self.logger.error(f"Response body: {response.text}")
            return


        sel = Selector(response)

        spankbakg_titles = sel.xpath('//div[@class="video-item"]/a/@title').extract()
        spankbakg_hrefs = sel.xpath('//div[@class="video-item"]/a/@href').extract()

        for title, href in zip(spankbakg_titles, spankbakg_hrefs):
            item = SpankBangItem()
            item['title'] = title

            complete_url = response.urljoin(href)
            logging.info("complete_url: %s" % complete_url)
            item['href'] = complete_url
            yield item
