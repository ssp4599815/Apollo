# !/usr/bin/env python
# -*-coding:utf-8 -*-
import os

BOT_NAME = "fuliji"
SPIDER_MODULES = ["fuliji.spiders"]
NEWSPIDER_MODULE = "fuliji.spiders"

# user_agent
# USER_AGENT = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.159 Safari/537.36'
USER_AGENT = "PostmanRuntime/7.41.1"

# 是否遵循robot协议
ROBOTSTXT_OBEY = False

# 并发请求数
CONCURRENT_REQUESTS = 100
CONCURRENT_REQUESTS_PER_DOMAIN = 100
CONCURRENT_REQUESTS_PER_IP = 0

# 线程池大小
REACTOR_THREADPOOL_MAXSIZE = 20

# 视频下载并发
FFMPEG_MAX_THREADS = 20

# 下载并发
MAX_CONCURRENT_DOWNLOADS = 20

# 下载延迟
DOWNLOAD_DELAY = 0.25
# 随机延迟
RANDOMIZE_DOWNLOAD_DELAY = True

# 禁止重定向
REDIRECT_ENABLED = False

# 禁止cookies
COOKIES_ENABLED = False
# 设置超时时间
DOWNLOAD_TIMEOUT = 10
# 重试失败请求最多5次
RETRY_TIMES = 5

# 管道管理
ITEM_PIPELINES = {
    # "fuliji.pipelines.ImgPipeline": 2,
    "fuliji.pipelines.M3U8Pipeline": 1
}

# 图片存储路径
# 获取当前用户的家目录
HOME_PATH = os.path.expanduser("~")
IMAGES_STORE = os.path.join(HOME_PATH, "Downloads")
# 图片过滤器，最小高度和宽度
IMAGES_MIN_HEIGHT = 100
IMAGES_MIN_WIDTH = 100

# 视频存储路径
VIDEOS_STORE = os.path.join(HOME_PATH, "Downloads", "Videos")

# 指定request_fingerprint的实现
REQUEST_FINGERPRINTER_IMPLEMENTATION = "2.7"
TWISTED_REACTOR = "twisted.internet.asyncioreactor.AsyncioSelectorReactor"
FEED_EXPORT_ENCODING = "utf-8"

# 降低日志级别
LOG_LEVEL = 'DEBUG'

# 允许重复的请求（在这里设为False以避免重复下载）
DUPEFILTER_DEBUG = False
