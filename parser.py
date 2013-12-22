import dataset
import requests

from bs4 import BeautifulSoup
from time import sleep
from uuid import uuid4
from datetime import datetime

def get_item_from_link(link):
    title = link.find('a', 'title')
    if title is None:
        return False

    comments = link.find(class_='comments').string.split(' ')[0]
    return {
        "title": title.string,
        "link": title['href'],
        "rank": int(link.find(class_='rank').string) if link.find(class_='rank').string else False,
        "upvotes": int(link['data-ups']),
        "downvotes": int(link['data-downs']),
        "comments": int(comments) if comments != "comment" else False,
        "thumbnail": link.select('.thumbnail img')[0]['src'] if len(link.select('.thumbnail img')) > 0 else False,
        "subreddit": link.find(class_='subreddit').string if link.find(class_='subreddit') else False,
        "observed": datetime.now(),
        "reddit_id": filter(lambda x: 'id-' in x, link['class'])[0][3:]
    }

def get_dataset_from_document(document):
    return filter(
        lambda link: link and link['rank'],
        map(get_item_from_link, document.find_all(class_='thing'))
    )

def add_thumbnail_to_item(item):
    link = item['thumbnail']
    if not link:
        item['thumbnail'] = None
        return item

    thumbnail = requests.get(link)
    item['thumbnail'] = './downloads/%s.%s' % (uuid4(), thumbnail.headers['content-type'].split('/')[1])
    with open(item['thumbnail'], 'w+') as f:
        f.write(thumbnail.content)

    return item


def get_data_from_remote(count, after):
    document = BeautifulSoup(requests.get('http://www.reddit.com?limit=100&count=%d&after=%s' % (count, after)).text)
    return get_dataset_from_document(document)


def mine():
    db = dataset.connect('sqlite:///reddit.db')
    entries = db['entry']
    statuses = db['status']

    while True:
        last_id = "";
        for x in xrange(0, 1000, 100):
            data = get_data_from_remote(x, last_id)
            for item in data:
                if item == data[-1]:
                    last_id = item['reddit_id']

                saved = entries.find_one(reddit_id=item['reddit_id'])
                if saved is None:
                    add_thumbnail_to_item(item)
                    eid = entries.insert({
                        "reddit_id": item['reddit_id'],
                        "thumbnail": item['thumbnail'],
                        "subreddit": item['subreddit'],
                        "title": item['title'],
                        "link": item['link']
                    })
                else:
                    eid = saved['id']

                del item['reddit_id']
                del item['thumbnail']
                del item['subreddit']
                del item['title']
                del item['link']

                item['eid'] = eid
                statuses.insert(item)
            sleep(5)
        sleep(550)

if __name__ == "__main__":
    mine()
