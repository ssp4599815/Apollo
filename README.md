# Apollo
数据爬虫采集


## 创建项目
```shell
# 创建一个项目
scrapy startproject mySpider

# 创建一个爬虫
cd mySpider
scrapy genspider itcast "itcast.cn"

# 启动项目
scrapy crawl itcast
```