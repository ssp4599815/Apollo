# !/usr/bin/env python
# -*-coding:utf-8 -*-
import logging
import os

import scrapy
from base_api.modules.quality import Quality
from scrapy import Selector

from ..items import VideoItem
from ..utils.spankbang import Client


class SpankbangSpider(scrapy.Spider):
    name = "spankbang"
    allowed_domains = ["spankbang.com"]

    custom_settings = {
        'ITEM_PIPELINES': {
        }
    }

    start_urls = ["https://spankbang.com/axxn0/playlist/porn"]

    def parse(self, response):
        sel = Selector(response)
        spankbakg_titles = sel.xpath('//div[@class="video-item"]/a/@title').extract()
        spankbakg_hrefs = sel.xpath('//div[@class="video-item"]/a/@href').extract()

        for title, href in zip(spankbakg_titles, spankbakg_hrefs):
            complete_url = response.urljoin(href)
            item = VideoItem()
            item['title'] = title.strip()
            item['href'] = complete_url
            logging.info("Requesting URL: %s" % complete_url)
            yield scrapy.Request(url=response.urljoin(href), callback=self.save_video, meta={'item': item})

    def save_video(self, response):
        item = response.meta['item']
        # Initialize a Client object
        client = Client()

        # Fetch a video
        video_object = client.get_video(item['href'])

        # Get information from videos
        print("Video title:", video_object.title)
        # print("Video description:", video_object.description)

        HOME_PATH = os.path.expanduser("~")
        video_object.download(quality=Quality.BEST, path=os.path.join(HOME_PATH, "Documents/视频/spankbang/"),
                              use_hls=False)
