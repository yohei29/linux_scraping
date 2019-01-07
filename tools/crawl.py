import time
import re
import sys

import requests
import lxml.html
from pymongo import MongoClient

from redis import Redis
from rq import Queue


def main():

    q = Queue(connection=Redis())

    client = MongoClient('localhost', 27017)
    collection = client.scraping.ebook_htmls
    # collection.create_index('Key', unique=True)
    collection.create_index('Key')

    session = requests.Session()
    response = session.get('https://gihyo.jp/dp')
    urls = scrape_list_page(response)

    for u in urls:
        key = extract_key(u)

        ebook_html = collection.find_one({'key':key})

        if not ebook_html:
            time.sleep(1)
            print('Fetching {0}'.format(u), file=sys.stderr)

            response = session.get(u)

            collection.insert_one({
                'url':u,
                'key':key,
                'html':response.content,
            })

            q.enqueue('scraper_tasks.scrape', key, result_ttl=0)


def scrape_list_page(response):
    root = lxml.html.fromstring(response.content)
    root.make_links_absolute(response.url)

    for v in root.cssselect('#listBook a[itemprop="url"]'):
        url = v.get('href')
        yield url

def extract_key(url):
    m = re.search(r'/([^/]+)$', url)
    return m.group(1)

if __name__ == '__main__':
    main()