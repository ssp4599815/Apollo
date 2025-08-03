#!/usr/bin/env python
# -*-coding:utf-8 -*-
import json
import logging
import re

import scrapy
from scrapy import Request, Selector

from ..items import VideoItem
from ..utils.logger_config import SpiderLoggerMixin


class ChiguaSpider(SpiderLoggerMixin, scrapy.Spider):
    name = "chigua"
    allowed_domains = []

    def __init__(self, *args, **kwargs):
        super(ChiguaSpider, self).__init__(*args, **kwargs)
        # 使用统一的日志配置
        self.setup_spider_logger()

    def start_requests(self):
        for page in range(2, 5):
            # for tag in ["萝莉", '一线天', '馒头逼', '福利姬', '自慰',
            #            '美乳', '粉嫩', '学生妹', '鲍鱼', '嫩穴', '小穴',
            #            '大学', '高中']:
            yield Request(url=f'https://cabinet.byoeyvro.club/search/大学/{page}/')

    def parse(self, response):
        sel = Selector(response)
        chigua_titles = sel.xpath("//h2[@class='post-card-title' and @itemprop='headline']/text()").extract()
        chigua_hrefs = sel.xpath("//div[@id='archive']//a/@href").extract()

        for title, href in zip(chigua_titles, chigua_hrefs):
            item = VideoItem()
            item['title'] = title.strip()
            item['site'] = 'chigua'

            if "search" in href:
                continue

            complete_url = response.urljoin(href)  # Combining base url with href

            self.log(f"Requesting URL: {complete_url}", logging.INFO)
            item['href'] = complete_url
            yield Request(url=complete_url, callback=self.parse_details, meta={'item': item})

    def parse_details(self, response):
        """
        解析详情页面，提取m3u8链接并进行预去重处理
        """
        item = response.meta['item']

        # 存储所有找到的视频链接
        all_video_urls = []
        m3u8_urls = []

        # 提取视频配置数据
        src_links = response.xpath("//div[@class='dplayer']/@data-config").extract()

        if src_links:
            for src in src_links:
                try:
                    config_data = json.loads(src)
                    video_url = config_data.get('video', {}).get('url', '')

                    if video_url:
                        all_video_urls.append(video_url)
                        # 检查是否为m3u8格式的视频
                        if video_url.endswith('.m3u8') or 'm3u8' in video_url:
                            m3u8_urls.append(video_url)
                            self.log(f"✅ 找到M3U8视频链接: {video_url}", logging.INFO)
                        else:
                            self.log(f"找到其他格式视频链接: {video_url}", logging.INFO)

                except json.JSONDecodeError as e:
                    self.log(f"解析视频配置JSON失败: {e}", logging.ERROR)
                    continue

        # 如果没找到配置数据，尝试其他方式
        if not all_video_urls:
            self.log("页面上未找到视频配置数据，尝试其他方式...", logging.INFO)

            # 尝试其他可能的视频链接提取方式
            # 查找可能的m3u8链接
            m3u8_links = response.xpath("//source[@src[contains(., '.m3u8')]]/@src").extract()
            if not m3u8_links:
                # 在脚本或其他地方查找m3u8链接
                m3u8_links = re.findall(r'["\']([^"\']*\.m3u8[^"\']*)["\']', response.text)

            for m3u8_link in m3u8_links:
                # 确保URL是完整的
                if m3u8_link.startswith('http'):
                    video_url = m3u8_link
                else:
                    video_url = response.urljoin(m3u8_link)

                m3u8_urls.append(video_url)
                self.log(f"✅ 通过其他方式找到M3U8链接: {video_url}", logging.INFO)

    def closed(self, reason):
        self.log(f"爬虫关闭，原因: {reason}", logging.INFO)
