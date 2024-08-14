# Apollo
数据爬虫采集


## 创建项目
```shell
#1 创建一个scrapy项目 
scrapy startproject mySpider 
 
#2 生成一个爬虫 
cd mySpider
scrapy genspider demo "demo.cn" 
 
#3 提取数据 
完善spider 使用xpath等 
 
#4 保存数据 
pipeline中保存数据 

#5 启动项目
scrapy crawl itcast

```