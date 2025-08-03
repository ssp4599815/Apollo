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
        for page in range(5, 15):
            for tag in ["萝莉", '一线天', '馒头逼', '福利姬', '自慰',
                        '美乳', '粉嫩', '学生妹', '鲍鱼', '嫩穴', '小穴',
                        '大学', '高中', '海角']:
                yield Request(url=f'https://cabinet.byoeyvro.club/search/{tag}/{page}/')

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

        # 对m3u8 URL列表进行预去重
        if m3u8_urls:
            unique_m3u8_urls = self._deduplicate_m3u8_urls(m3u8_urls)

            if unique_m3u8_urls:
                # 根据去重后的URL数量决定如何设置item
                if len(unique_m3u8_urls) == 1:
                    item['m3u8_url'] = unique_m3u8_urls[0]
                else:
                    item['m3u8_url'] = unique_m3u8_urls
                    self.log(f"🎯 准备发送item到pipeline: {item['title']}")
                self.log(f"📺 去重后M3U8链接数量: {len(unique_m3u8_urls)}")
                yield item
            else:
                self.log(f"❌ 所有M3U8链接都是重复的: {item['title']}", logging.WARNING)
        else:
            self.log(f"❌ 未找到任何M3U8视频链接: {item['title']}", logging.WARNING)

    def _deduplicate_m3u8_urls(self, m3u8_urls):
        """
        对m3u8 URL列表进行预去重
        """
        unique_urls = []
        seen_urls = set()
        seen_url_keys = set()

        for url in m3u8_urls:
            if not url or not isinstance(url, str):
                continue

            # URL标准化
            normalized_url = url.strip()

            # 完全相同的URL去重
            if normalized_url in seen_urls:
                self.log(f"🔄 发现重复URL，跳过: {normalized_url[:100]}...")
                continue

            # 提取URL关键特征进行相似性去重
            url_key = self._extract_url_key(normalized_url)
            if url_key in seen_url_keys:
                self.log(f"🔄 发现相似URL，跳过: {normalized_url[:100]}...")
                continue

            # 验证URL格式
            if not self._is_valid_m3u8_url(normalized_url):
                self.log(f"❌ 无效的M3U8 URL，跳过: {normalized_url[:100]}...")
                continue

            # 通过所有检查，添加到唯一列表
            unique_urls.append(normalized_url)
            seen_urls.add(normalized_url)
            seen_url_keys.add(url_key)

        self.log(f"📊 URL去重统计: 原始数量={len(m3u8_urls)}, 去重后数量={len(unique_urls)}")
        return unique_urls

    def _extract_url_key(self, url):
        """
        从URL中提取关键标识符用于重复检测
        """
        import re

        # 方法1：提取长的字母数字组合
        matches = re.findall(r'[a-zA-Z0-9]{8,}', url)
        if matches:
            longest_match = max(matches, key=len)
            if len(longest_match) >= 12:
                return longest_match

        # 方法2：提取路径中的关键部分
        path_matches = re.findall(r'/([a-zA-Z0-9]{6,})/', url)
        if path_matches:
            return path_matches[-1]

        # 方法3：提取文件名（不含扩展名）
        filename_match = re.search(r'/([^/]+)\.m3u8', url)
        if filename_match:
            filename = filename_match.group(1)
            if len(filename) >= 6:
                return filename

        # 方法4：返回URL的哈希值
        import hashlib
        return hashlib.md5(url.encode()).hexdigest()[:16]

    def _is_valid_m3u8_url(self, url):
        """
        验证是否为有效的m3u8 URL
        """
        if not url:
            return False

        # 基本格式检查
        if not (url.startswith('http://') or url.startswith('https://')):
            return False

        # 检查是否包含m3u8
        if not ('.m3u8' in url.lower() or 'm3u8' in url.lower()):
            return False

        # 检查URL长度（过短的URL可能无效）
        if len(url) < 20:
            return False

        # 检查是否包含明显的无效字符
        invalid_chars = ['<', '>', '"', "'", '\\']
        if any(char in url for char in invalid_chars):
            return False

        return True

    def closed(self, reason):
        self.log(f"爬虫关闭，原因: {reason}", logging.INFO)
