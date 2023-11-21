import clickhouse_connect
from clickhouse_connect import common
from scrapy.spiders import SitemapSpider
import sys

MIN_PYTHON = (3, 10)
if sys.version_info < MIN_PYTHON:
    sys.exit("Python %s.%s or later is required.\n" % MIN_PYTHON)


class Spider(SitemapSpider):
    name = "google-analytics-spider"

    sitemap_urls = [
        "https://clickhouse.com/sitemap.xml",
        "https://clickhouse.com/docs/sitemap.xml"
    ]
    allowed_domains = ["clickhouse.com"]

    custom_settings = {
        # Filters out Requests for URLs outside the domains covered by the spider.
        "OffsiteMiddleware": True
    }

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        common.set_setting('autogenerate_session_id', False)
        self.client = clickhouse_connect.get_client(
            host=self.host,
            port=self.port,
            username=self.username,
            password=self.password,
            secure=True)

    def is_url_of_interest(self, url):
        if "https://clickhouse.com/" not in url:
            return False

        if (("https://clickhouse.com/docs/" in url) and
                ("https://clickhouse.com/docs/knowledgebase/" not in url) and
                ("https://clickhouse.com/docs/en/" not in url)):
            return False

        return True

    def get_content(self, url, response):
        raw_content = None

        if "https://clickhouse.com/docs/knowledgebase" in url:
            raw_content = response.xpath("*//*/div").css(".markdown").get()
        elif "https://clickhouse.com/docs" in url:
            raw_content = response.xpath("*//*/div").css(".theme-doc-markdown").get()
        elif "https://clickhouse.com/blog" in url:
            raw_content = response.xpath("*//*/div").css(".rich-text-content").get()
        else:
            raw_content = response.xpath("*//*/div").css(".readable-content").get()

        if raw_content is None:
            raw_content = response.body

        return raw_content

    def parse(self, response):

        url = response.url

        if not self.is_url_of_interest(url):
            return

        # ------- Get page's raw title (may include html tags) ---------------------------------------------------------
        raw_title = response.xpath("//head//title").get()
        if raw_title is None:
            raw_title = ''

        # ------- Get page's raw content (may include html tags) -------------------------------------------------------
        raw_content = self.get_content(url, response)

        # ------- Insert page's raw data into a ClickHouse table -------------------------------------------------------
        # ------- Note that we use ClickHouse built-in text function for extracting the pure text
        self.client.insert(
            database=self.database,
            table=self.table,
            data=[[url, raw_title, raw_content]],
            column_names=['url', 'raw_title', 'raw_content'])
