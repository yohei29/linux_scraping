import re
import lxml.html
from pymongo import MongoClient


def scrape(key):
    client = MongoClient('localhost', 27017)
    html_collection = client.scraping.ebook_htmls

    ebook_html = html_collection.find_one({'key':key})
    ebook = scrape_detail_page(key, ebook_html['url'], ebook_html['html'])
    ebook_coll = client.scraping.ebooks
    ebook_coll.create_index('key', unique=True)
    ebook_coll.insert_one(ebook)

def scrape_detail_page(key, url, html):
    root = lxml.html.fromstring(html)

    ebook = {
        'url':url,
        'key':key,
        'title':root.cssselect('#bookTitle')[0].text_content(),
        'price':root.cssselect('.buy')[0].text.strip(),
        'content':[noemalize_spaces(h3.text_content()) for h3 in root.cssselect('#content > h3')],
    }

    return ebook

def noemalize_spaces(s):
    return re.sub(r'\s+', '', s).strip()


def debug_main():
    scrape('978-4-297-10359-0')


if __name__ == '__main__':
    debug_main()