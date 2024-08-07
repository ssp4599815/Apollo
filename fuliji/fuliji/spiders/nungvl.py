import scrapy


class NungvlSpider(scrapy.Spider):
    name = "nungvl"
    allowed_domains = ["nungvl.net"]
    start_urls = ["https://nungvl.net"]

    def parse(self, response):
        filename = "teacher.html"
        open(filename, 'w').write(response.body)
