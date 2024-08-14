# !/usr/bin/env python
# -*-coding:utf-8 -*-
import logging
import os

import scrapy
from scrapy.exceptions import DropItem
from scrapy.pipelines.images import ImagesPipeline
from scrapy.utils.project import get_project_settings


class ImgPipeline(ImagesPipeline):

    def get_media_requests(self, item, info):
        # 获取将用于存储图片的目录路径
        dir_path = self.get_directory_path(item)
        logging.info("Checking if directory exists at: %s" % dir_path)
        if not os.path.exists(dir_path):
            logging.info("Directory does not exist, downloading images")
            for index, image_url in enumerate(item['image_urls']):
                yield scrapy.Request(image_url, meta={'item': item, 'index': index})
        else:
            logging.info("Directory already exists, skipping download for images in: %s" % dir_path)

    def item_completed(self, results, item, info):
        image_paths = [x['path'] for ok, x in results if ok]
        # logging.info("image_paths: %s" % image_paths)
        if not image_paths:
            raise DropItem("No images downloaded %s" % image_paths)
        item['image_paths'] = image_paths
        # 打印完结的日志
        logging.info("Download images completed: %s" % item['title'])

        return item

    def file_path(self, request, response=None, info=None):
        item = request.meta['item']
        index = request.meta['index']
        url = request.url
        # 获取图片格式
        image_format = url.split('.')[-1]
        file_name = f"{item['title']}-{index + 1}"  # 以图片URL的顺序命名文件
        # logging.info("file_name: %s" % file_name)
        return f"{item['title']}/{file_name}.{image_format}"

    def get_directory_path(self, item):
        """
        Returns the directory path for the given item.
        """
        settings = get_project_settings()
        images_store = settings.get('IMAGES_STORE', '')
        dir_path = os.path.join(images_store, item['site'], item['title'])
        return dir_path
