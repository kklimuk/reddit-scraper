import dataset
import requests
import os

from jinja2 import Template
from urlparse import urlparse, parse_qs
from bs4 import BeautifulSoup
from time import time

def get_imgur_images(link):
    def process_item(item):
        description = item.find(class_='description')
        image = item.find('img')
        return {
            "image": "http:%s" % (image.attrs['data-src'] if 'data-src' in image.attrs else image.attrs['src']),
            "description": description.string if description else None
        }
    document = BeautifulSoup(requests.get(link).text)
    images = document.find_all(class_='image')
    return map(process_item, images)

def process(item):
    if 'youtube' in item['link']:
        item['embed'] = parse_qs(urlparse(item['link']).query)['v'][0]
    elif 'imgur' in item['link'] and ('.jpg' not in item['link'] and '.jpeg' not in item['link'] \
        and '.gif' not in item['link'] and '.png' not in item['link']):
        item['images'] = get_imgur_images(item['link'])
    elif item['link'][0] == '/':
        item['link'] = 'http://reddit.com%s' % item['link']
    return item


def main():
    db = dataset.connect('sqlite:///reddit.db')
    if not os.path.exists('./deploy'):
        os.mkdir('./deploy')

    def get_items_from_day(date):
        return db.query('SELECT title, link, min(rank) as rank, (upvotes - downvotes) as votes, subreddit FROM status ' + \
               ('JOIN entry ON status.eid=entry.id WHERE rank <= 10 %s GROUP BY eid ORDER BY rank, (upvotes - downvotes) DESC;' % date))

    all_items = {}
    def process_items_from_day(items):
        data = []
        for item in filter(lambda item: item['link'] is not None, [item for item in items]):
            item = process(item)
            if item['link'] not in all_items:
                all_items[item['link']] = True
                data.append(item)
        return data

    collections = ['AND observed > date("now", "start of day", "-1 day") AND observed < date("now", "start of day")',
                   'AND observed > date("now", "start of day", "-2 day") AND observed < date("now", "start of day", "-1 day")',
                   'AND observed > date("now", "start of day", "-3 day") AND observed < date("now", "start of day", "-2 day")']
    collections = [process_items_from_day(get_items_from_day(date)) for date in collections]

    with open('./templates/newsletter.html', 'r') as newspaper:
        template = Template(newspaper.read())
        html = template.render(title="Reddit News Agency", edition=len(os.listdir('./deploy')),
                               collections=collections).encode('utf-8')
        f = open('./deploy/' + str(int(time())) + '.html', 'w')
        f.write(html)
        requests.post('http://reddit-news-agency.herokuapp.com/', data=html, headers={
            'Authorization': '9f9fa431c64a86da8324bb370d05377bbf49dbf9'
        })


if __name__ == '__main__':
    main()
