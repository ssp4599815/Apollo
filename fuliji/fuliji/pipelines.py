# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html
import logging

import scrapy
from scrapy.exceptions import DropItem
from scrapy.pipelines.images import ImagesPipeline


class FulijiPipeline():
    def process_item(self, item, spider):
        return item


class ImgPipeline(ImagesPipeline):

    def get_media_requests(self, item, info):
        for image_url in item['image_urls']:
            logging.info("image_url: %s" % image_url)
            print("image_url: %s" % image_url)
            yield scrapy.Request(image_url)

    def file_path(self, request, response=None, info=None):
        url = request.url
        file_name = url.split('/')[-1]  # 以图片链接最后一段xxx.jpg作为文件名
        logging.info("file_name: %s" % file_name)
        return file_name

    def item_completed(self, results, item, info):
        image_paths = [x['path'] for ok, x in results if ok]
        logging.info("image_paths: %s" % image_paths)
        if not image_paths:
            raise DropItem('Item contains no files')
        item['image_paths'] = image_paths

        print("iem: %s" % item)
        return item
