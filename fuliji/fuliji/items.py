# !/usr/bin/env python
# -*-coding:utf-8 -*-
import scrapy


class FulijiItem(scrapy.Item):
    title = scrapy.Field()
    href = scrapy.Field()
    link = scrapy.Field()
    site = scrapy.Field()
    image_urls = scrapy.Field()  # 为 ImagesPipeline 添加
    images = scrapy.Field()  # 用于存储下载信息
    image_paths = scrapy.Field()  # 用于存储图片路径


class VideoItem(scrapy.Item):
    title = scrapy.Field()
    href = scrapy.Field()
    m3u8_url = scrapy.Field()  # M3U8视频链接
    site = scrapy.Field()
    video_path = scrapy.Field()  # 用于存储视频路径
