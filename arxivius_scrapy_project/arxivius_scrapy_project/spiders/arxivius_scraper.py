import scrapy
import requests
from bs4 import BeautifulSoup
import pymupdf
import tempfile
import os

class ArxivSpider(scrapy.Spider):
    name = "arxiv"
    allowed_domains = ["export.arxiv.org", "ui.adsabs.harvard.edu"]

    first_holders = ["cs.AI", "cs.CL", "cs.CV", "cs.LG", "cs.MA"]
    second_holders = [str(year) for year in range(2025, 2024, -1)]
    fourth_holder = 2000

    nasa_ads = "https://ui.adsabs.harvard.edu/abs/"

    def start_requests(self):
        for first in self.first_holders:
            for second in self.second_holders:
                skip = 0
                url = f"https://export.arxiv.org/list/{first}/{second}?skip={skip}&show={self.fourth_holder}"
                meta = {'first': first, 'second': second, 'skip': skip}
                yield scrapy.Request(url, callback=self.parse, meta=meta)

    def parse(self, response):
        if b"No updates for this time period." in response.body:
            self.logger.info(f"No updates for {response.meta['first']} {response.meta['second']}")
            return

        print(f"Parsing {response.url}...")

        for article_dt in response.xpath('//dl[@id="articles"]/dt'):
            arxiv_id = article_dt.xpath('.//a[@title="Abstract"]/text()').get()
            arxiv_main_url = article_dt.xpath('.//a[@title="Abstract"]/@href').get()
            article_dd = article_dt.xpath('following-sibling::dd[1]')
            title_parts = article_dd.xpath('.//div[@class="list-title mathjax"]/text()').getall()
            authors = article_dd.xpath('.//div[@class="list-authors"]/a/text()').getall()

            if arxiv_id and title_parts and authors:
                yield {
                    'arxiv_id': arxiv_id.strip(),
                    'title': ' '.join(t.strip() for t in title_parts).replace('\n', ' '),
                    'authors': [author.strip() for author in authors],
                    'arxiv_main_url': arxiv_main_url.strip(),
                }
        # arxiv_id = response.xpath('.//a[@title="Abstract"]/text()').getall()
        # title = response.xpath('.//div[@class="list-title mathjax"]/text()').getall()
        # authors = response.xpath('.//div[@class="list-authors"]/a/text()').getall()
        # arxiv_main_url = dt.xpath('.//a[@title="Abstract"]/@href').get()
        # citation_url = self.nasa_ads + arxiv_id if arxiv_id else None
        # citation_number = get_citation_number(citation_url) if citation_url else 0
        # html_url = response.url.replace('/abs/', '/html/')
        # html_text = fetch_html_or_pdf(html_url)
        # if not arxiv_id or not title or not authors:
        #     self.logger.warning(f"Missing data in {response.url} for {arxiv_id}")
        #     continue
        # print(f"Found article: {arxiv_id} - {title} by {authors}")

        # Pagination: try next page
        first = response.meta['first']
        second = response.meta['second']
        skip = response.meta['skip'] + self.fourth_holder
        next_url = f"https://export.arxiv.org/list/{first}/{second}?skip={skip}&show={self.fourth_holder}"
        next_meta = {'first': first, 'second': second, 'skip': skip}
        yield scrapy.Request(next_url, callback=self.parse, meta=next_meta)

def fetch_html_or_pdf(url):
    resp = requests.get(url)
    if resp.status_code == 404:
        # Try PDF
        pdf_url = url.replace('/html/', '/pdf/')
        pdf_resp = requests.get(pdf_url)
        if pdf_resp.status_code == 200:
            # Save PDF to temp file
            with tempfile.NamedTemporaryFile(delete=False, suffix='.pdf') as tmp:
                tmp.write(pdf_resp.content)
                tmp_path = tmp.name
            # Extract text
            text = extract_pdf_text(tmp_path)
            os.remove(tmp_path)
            return text
        else:
            raise Exception("Both HTML and PDF not found.")
    else:
        return resp.text

def extract_pdf_text(pdf_path):
    doc = pymupdf.open(pdf_path)
    text = ""
    for page in doc:
        text += page.get_text()
    doc.close()
    return text

def get_citation_number(citation_url):
    USER_AGENT = "Mozilla/5.0 (iPad; CPU OS 12_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148"
    headers = {'User-Agent': USER_AGENT}
    resp = requests.get(citation_url, headers=headers)
    soup = BeautifulSoup(resp.text, 'html.parser')
    num_span = soup.select_one('span.num-items')
    if num_span:
        return int(num_span.text.strip('()'))
    return 0
