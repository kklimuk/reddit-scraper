import requests
import dataset
import re
import logging

from bs4 import BeautifulSoup
from datetime import datetime
from threading import Thread
from urlparse import urlparse
from time import sleep


INVISIBLE_ELEMENTS = set(['style', 'script', '[document]', 'head', 'title'])
PROHIBITED_DOMAINS = ['instagram', 'flickr', 'photobucket', 'memebase', '9gag', 'failblog', 'quickmeme', 'youtube', 'vimeo']
SUBREDDITS = [
                'worldnews','technology','science','music','movies','books','television',
                'sports', 'politics','todayilearned','mildlyinteresting','android','celebs',
                'oddlysatisfying','nottheonion','books','trees','marijuanaenthusiasts','diablo',
                'edmproduction','philosophy','programming','lifehacks','freebies','doctorwho','dataisbeautiful',
                'futurology','linux','canada','libertarian','republican','democrats','republican', 'socialism'
            ]


def mine(db, mined_from=None, entry_count=200, sleep_total=600):
    logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p',
                        filename='parser.log',level=logging.DEBUG)

    def get_document_from_remote(count, after):
        url = 'http://www.reddit.com' + ('/r/%s/top' % mined_from if mined_from is not None else '')
        return BeautifulSoup(requests.get(url, params={
            "limit": 100,
            "count": count,
            "after": after,
            "sort": "top",
            "t": "all"
        }).text)

    def get_dataset_from_document(document):
        return filter(
            lambda entry: entry and entry['rank'],
            map(get_data_from_html_entry, document.find_all(class_='thing'))
        )

    def get_data_from_html_entry(entry):
        title = entry.find('a', 'title')
        if title is None:
            return False

        comments = entry.find(class_='comments').string.split(' ')[0]
        subreddit = entry.find(class_='subreddit')
        rank = entry.find(class_='rank').string

        return {
            "reddit_id": filter(lambda x: 'id-' in x, entry['class'])[0][3:],
            "title": title.string,
            "link": title['href'],
            "subreddit": subreddit.string if subreddit else '',
            "upvotes": int(entry['data-ups']),
            "downvotes": int(entry['data-downs']),
            "mined_from": mined_from,
            "rank": int(rank) if rank else 0
        }


    def visible(element):
        if element.parent.name in INVISIBLE_ELEMENTS:
            return False
        return True


    def get_content_from_link(link):
        return ' '.join(' '.join(filter(visible, BeautifulSoup(requests.get(link).text).findAll(text=True))).split())


    def filter_domains(entry):
        domain = urlparse(entry['link']).netloc
        if not domain:
            return False

        for prohibited in PROHIBITED_DOMAINS:
            if prohibited in domain:
                return False
        return True

    last_id = ""
    for count in xrange(0, entry_count, 100):
        data = filter(filter_domains, get_dataset_from_document(get_document_from_remote(count, last_id)))

        for i, entry in enumerate(data):
            if i == len(data) - 1:
                last_id = entry['reddit_id']

            saved_entry = db['entries'].find_one(reddit_id=entry['reddit_id'])

            if saved_entry is None:
                try:
                    entry['article'] = get_content_from_link(entry['link'])
                except Exception, e:
                    logging.error(e)
                    continue

                db['entries'].insert(entry)

            sleep(0.05)

        logging.info('Finished mining entries %d-%d in %s' % (count, count + 100, mined_from))


def setup_db():
    queries = {
        "has_tables": 'SELECT EXISTS(SELECT * FROM information_schema.tables WHERE table_name=\'entries\') as exists;',
        "create_entries": """CREATE TABLE entries (
            id SERIAL NOT NULL, reddit_id TEXT, title TEXT, link TEXT, subreddit TEXT,
            downvotes INTEGER, mined_from TEXT,
            observed TIMESTAMP, upvotes INTEGER, article TEXT,
            PRIMARY KEY (id)
        );""",
        "create_index": """
        CREATE UNIQUE INDEX reddit_id_index ON entries(reddit_id);
        """
    }

    db = dataset.connect('postgresql://foobar:foobarbaz@testdb.cy2ub2trrp92.us-east-1.rds.amazonaws.com:5432/reddit')
    for item in db.query(queries['has_tables']):
        if not item['exists']:
            db.query(queries['create_entries'])
            db.query(queries['create_index'])
        break

    return db


if __name__ == "__main__":
    threads = []
    db = setup_db()
    for subreddit in SUBREDDITS:
        thread = Thread(target=mine, args=(db,), kwargs={ "mined_from": subreddit })
        thread.start()
        threads.append(thread)

    for thread in threads:
        thread.join()
