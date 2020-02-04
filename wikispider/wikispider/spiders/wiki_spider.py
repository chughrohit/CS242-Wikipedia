import scrapy
from urllib.parse import urlparse
from pybloom_live import ScalableBloomFilter
from scrapy.linkextractors import LinkExtractor
import re
import os
import json


class BloomDupeFilter():
    def __init__(self):
        self.fingerprints = ScalableBloomFilter(initial_capacity=2000000, 
                 error_rate=0.00001,
                 mode=ScalableBloomFilter.SMALL_SET_GROWTH)

    def request_seen(self, url):
        url = url.split("#")[0]
        if url in self.fingerprints:
            return True
        else:
            self.fingerprints.add(url)
            return False

class WikiSpider(scrapy.Spider):
    name = "wiki"
    count = 0
    dupfilter = BloomDupeFilter()
    allowed_domains = ['en.wikipedia.org']
    start_urls = ['https://en.wikipedia.org/wiki/Category:Sports',
    'https://en.wikipedia.org/wiki/Category:Computing',
    'https://en.wikipedia.org/wiki/Category:Electronics',
    'https://en.wikipedia.org/wiki/Category:Engineering',
    'https://en.wikipedia.org/wiki/Category:Comics',
    'https://en.wikipedia.org/wiki/Category:People',
    'https://en.wikipedia.org/wiki/Category:Education',
    'https://en.wikipedia.org/wiki/Category:Travel',
    'https://en.wikipedia.org/wiki/Category:Medicine',
    'https://en.wikipedia.org/wiki/Category:Nutrition',
    'https://en.wikipedia.org/wiki/Category:Earth',
    'https://en.wikipedia.org/wiki/Category:Arts',
    'https://en.wikipedia.org/wiki/Category:Films'] 

    extractor=LinkExtractor(
            allow=[r"https://en\.wikipedia\.org/wiki/.+"],
            deny=[
                r"https://en\.wikipedia\.org/wiki/Wikipedia.*",
                r"https://en\.wikipedia\.org/wiki/Main_Page",
                r"https://en\.wikipedia\.org/wiki/Free_Content",
                r"https://en\.wikipedia\.org/wiki/Talk:.*",
                r"https://en\.wikipedia\.org/wiki/Portal:.*",
                r"https://en\.wikipedia\.org/wiki/Special:.*",
                r"https://en\.wikipedia\.org/wiki/User:.*",
                r"https://en\.wikipedia\.org/wiki/Help:.*",
                r"https://en\.wikipedia\.org/wiki/Template:.*",
                r"https://en\.wikipedia\.org/wiki/Template_talk:.*",
                r"https://en\.wikipedia\.org/wiki/Category_talk:.*",
                r"https://en\.wikipedia\.org/wiki/File:.*"
            ],
            allow_domains=['en.wikipedia.org'],
            unique=True
        )
    def __init__(self,**kwargs):
        self.filename = None
        self.data_path = os.path.join(os.getcwd(), r'data')
        if not os.path.exists(self.data_path):
            try:
                os.mkdir(self.data_path)
            except OSError:
                print('Error creating data folder')

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url)

    def parse(self,response):
        for anchor in self.extractor.extract_links(response):
            if 'Category:' in anchor.url:
                yield response.follow(anchor, callback=self.parse)
            else:
                yield response.follow(anchor, callback=self.parse_page)

    def write_data(self, data):
        if self.count%20000 == 0:
            self.filename = 'wiki-%s.json' % (self.count//20000)
        file_path = os.path.join(self.data_path,self.filename)
        with open(file_path, 'a') as output_file:
            json.dump(data, output_file)
            output_file.write("\n")
        self.count += 1

    def strip_data(self, value):
        value = re.sub(r"[\n\t]*", "", value)
        value = value.strip()
        return value

    def parse_page(self, response):
        item = {}
        title = response.css("h1.firstHeading::text").get()
        if title:
            item['title'] = title
        else:
            item['title'] = response.css("h1.firstHeading > i::text").get()
        page_content = response.css('#mw-content-text p::text').getall()
        urls = response.css('a::attr(href)').getall()
        body = self.strip_data(''.join(page_content))
        item['text'] = body
        item['url'] = response.url
        self.write_data(item)

        #checking for duplicates
        for url in urls:
            parsed_uri = urlparse(response.urljoin(url))
            result = '{uri.scheme}://{uri.netloc}/'.format(uri=parsed_uri)
            if "https://en.wikipedia.org" in result:
                if not self.dupfilter.request_seen(response.urljoin(url)):
                    yield response.follow(url, callback=self.parse)

