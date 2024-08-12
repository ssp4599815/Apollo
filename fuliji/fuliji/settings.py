import os

BOT_NAME = "fuliji"

SPIDER_MODULES = ["fuliji.spiders"]
NEWSPIDER_MODULE = "fuliji.spiders"

# Crawl responsibly by identifying yourself (and your website) on the user-agent
USER_AGENT = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.159 Safari/537.36'

# Obey robots.txt rules
ROBOTSTXT_OBEY = False

# Configure maximum concurrent requests performed by Scrapy (default: 16)
CONCURRENT_REQUESTS = 16

# Configure a delay for requests for the same website (default: 0)
# See https://docs.scrapy.org/en/latest/topics/settings.html#download-delay
# See also autothrottle settings and docs
# 下载延迟
DOWNLOAD_DELAY = 0.25

# 随机延迟
RANDOMIZE_DOWNLOAD_DELAY = True

# The download delay setting will honor only one of:
# CONCURRENT_REQUESTS_PER_DOMAIN = 16
# CONCURRENT_REQUESTS_PER_IP = 16

# 禁止cookies
COOKIES_ENABLED = False

# 设置超时时间
DOWNLOAD_TIMEOUT = 10

# Disable Telnet Console (enabled by default)
# TELNETCONSOLE_ENABLED = False

# Override the default request headers:
# DEFAULT_REQUEST_HEADERS = {
#    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
#    "Accept-Language": "en",
# }

# Enable or disable spider middlewares
# See https://docs.scrapy.org/en/latest/topics/spider-middleware.html
# SPIDER_MIDDLEWARES = {
#    "fuliji.middlewares.FulijiSpiderMiddleware": 543,
# }

# Enable or disable downloader middlewares
# See https://docs.scrapy.org/en/latest/topics/downloader-middleware.html
# DOWNLOADER_MIDDLEWARES = {
#    "fuliji.middlewares.FulijiDownloaderMiddleware": 543,
# }

# Enable or disable extensions
# See https://docs.scrapy.org/en/latest/topics/extensions.html
# EXTENSIONS = {
#    "scrapy.extensions.telnet.TelnetConsole": None,
# }

# Configure item pipelines
# See https://docs.scrapy.org/en/latest/topics/item-pipeline.html
ITEM_PIPELINES = {
    "fuliji.pipelines.ImgPipeline": 1,
}

# Specifies the folder where the images will be stored
# 如果没有这个路径，会自动创建
os.makedirs('images', exist_ok=True)
IMAGES_STORE = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'images')

# Enable and configure the AutoThrottle extension (disabled by default)
# See https://docs.scrapy.org/en/latest/topics/autothrottle.html
# AUTOTHROTTLE_ENABLED = True
# The initial download delay
# AUTOTHROTTLE_START_DELAY = 5
# The maximum download delay to be set in case of high latencies
# AUTOTHROTTLE_MAX_DELAY = 60
# The average number of requests Scrapy should be sending in parallel to
# each remote server
# AUTOTHROTTLE_TARGET_CONCURRENCY = 1.0
# Enable showing throttling stats for every response received:
# AUTOTHROTTLE_DEBUG = False

# Enable and configure HTTP caching (disabled by default)
# See https://docs.scrapy.org/en/latest/topics/downloader-middleware.html#httpcache-middleware-settings
# HTTPCACHE_ENABLED = True
# HTTPCACHE_EXPIRATION_SECS = 0
# HTTPCACHE_DIR = "httpcache"
# HTTPCACHE_IGNORE_HTTP_CODES = []
# HTTPCACHE_STORAGE = "scrapy.extensions.httpcache.FilesystemCacheStorage"

# Set settings whose default value is deprecated to a future-proof value
REQUEST_FINGERPRINTER_IMPLEMENTATION = "2.7"
TWISTED_REACTOR = "twisted.internet.asyncioreactor.AsyncioSelectorReactor"
FEED_EXPORT_ENCODING = "utf-8"

# # 是否启用日志
# LOG_ENABLED = False
#
# # 日志使用的编码
# LOG_ENCODING = 'utf-8'
#
# # 日志文件(文件名)
# LOG_DIR = 'logs'
# LOG_FILE = os.path.join(LOG_DIR, 'scrapy.log')
#
# # 日志格式
# LOG_FORMAT = '%(asctime)s [%(name)s] %(levelname)s: %(message)s'
#
# # 日志时间格式
# LOG_DATEFORMAT = '%Y-%m-%d %H:%M:%S'
#
# # 日志级别 CRITICAL, ERROR, WARNING, INFO, DEBUG
# LOG_LEVEL = 'DEBUG'
#
# # 如果等于True，所有的标准输出（包括错误）都会重定向到日志，例如：print('hello')
# LOG_STDOUT = False
#
# # 如果等于True，日志仅仅包含根路径，False显示日志输出组件
# LOG_SHORT_NAMES = False


# 去重过滤器
DUPEFILTER_CLASS = 'scrapy.dupefilter.RFPDupeFilter'

