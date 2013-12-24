import dataset
import requests
import os

from bs4 import BeautifulSoup
from time import sleep
from uuid import uuid4
from datetime import datetime


def get_document_from_remote(count, after):
    return BeautifulSoup(requests.get('http://www.reddit.com', params={
        "limit": 100,
        "count": count,
        "after": after
    }).text)


def get_dataset_from_document(document):
    return filter(
        lambda entry: entry and entry[1]['rank'],
        map(get_data_from_html_entry, document.find_all(class_='thing'))
    )


def get_data_from_html_entry(entry):
    title = entry.find('a', 'title')
    if title is None:
        return False

    comments = entry.find(class_='comments').string.split(' ')[0]
    subreddit = entry.find(class_='subreddit')
    thumbnail = entry.select('.thumbnail img')
    rank = entry.find(class_='rank').string

    return ({
        "reddit_id": filter(lambda x: 'id-' in x, entry['class'])[0][3:],
        "title": title.string,
        "link": title['href'],
        "subreddit": subreddit.string if subreddit else False,
        "thumbnail": thumbnail[0]['src'] if len(thumbnail) > 0 else False
    }, {
        "rank": int(rank) if rank else False,
        "upvotes": int(entry['data-ups']),
        "downvotes": int(entry['data-downs']),
        "comments": int(comments) if comments != "comment" else False,
        "observed": datetime.now()
    })


def add_thumbnail_to_entry(entry):
    link = entry['thumbnail']
    if not link:
        entry['thumbnail'] = None
        return entry

    thumbnail = requests.get(link)
    entry['thumbnail'] = './downloads/%s.%s' % (uuid4(), thumbnail.headers['content-type'].split('/')[1])
    with open(entry['thumbnail'], 'w+') as f:
        f.write(thumbnail.content)

    return entry


def mine(sleep_between=5, sleep_total=550):
    db = dataset.connect('sqlite:///reddit.db')
    if not os.path.exists('./downloads'):
        os.mkdir('./downloads')

    while True:
        last_id = "";
        for count in xrange(0, 1000, 100):
            data = get_dataset_from_document(get_document_from_remote(count, last_id))
            for i, (entry, status) in enumerate(data):
                if i == len(data) - 1:
                    last_id = entry['reddit_id']

                saved_entry = db['entry'].find_one(reddit_id=entry['reddit_id'])
                if saved_entry is None:
                    status['eid'] = db['entry'].insert(add_thumbnail_to_entry(entry))
                else:
                    status['eid'] = saved_entry['id']
                db['status'].insert(status)

            sleep(sleep_between)
        sleep(sleep_total)


if __name__ == "__main__":
    mine()
