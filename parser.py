import requests
import dataset

from bs4 import BeautifulSoup
from time import sleep, time
from datetime import datetime


def setup_db():
    queries = {
        "has_tables": 'SELECT EXISTS(SELECT * FROM information_schema.tables WHERE table_name=\'entries\');',
        "create_entries": """CREATE TABLE entries (
            id SERIAL NOT NULL, reddit_id TEXT, title TEXT, link TEXT, thumbnail TEXT, subreddit TEXT,
            PRIMARY KEY (id)
        );""",
        "create_statuses": """CREATE TABLE statuses (
            id SERIAL NOT NULL, downvotes INTEGER, comments INTEGER, mined_from TEXT,
            observed TIMESTAMP, rank INTEGER, upvotes INTEGER, eid INTEGER,
            PRIMARY KEY (id), FOREIGN KEY (eid) REFERENCES entries(id)
        );""",
        "create_index": """
        CREATE UNIQUE INDEX reddit_identifier ON entries(reddit_id);
        """
    }

    db = dataset.connect('postgresql://admin:onelightmessiah@localhost:5432/reddit')
    for item in db.query(queries['has_tables']):
        if not item['exists']:
            db.query(queries['create_entries'])
            db.query(queries['create_statuses'])
            db.query(queries['create_index'])
        break

    return db


def mine(mined_from=None, entry_count=1000, sleep_total=600):

    def get_document_from_remote(count, after):
        url = 'http://www.reddit.com' + ('/r/%s' % mined_from if mined_from is not None else '')
        return BeautifulSoup(requests.get(url, params={
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
            "subreddit": subreddit.string if subreddit else '',
            "thumbnail": thumbnail[0]['src'] if len(thumbnail) > 0 else ''
        }, {
            "rank": int(rank) if rank else 0,
            "upvotes": int(entry['data-ups']),
            "downvotes": int(entry['data-downs']),
            "comments": int(comments) if comments != "comment" else 0,
            "observed": datetime.now(),
            "mined_from": mined_from
        })

    db = setup_db()
    while True:
        last_id = "";
        start_time = time()
        for count in xrange(0, 1000, 100):
            data = get_dataset_from_document(get_document_from_remote(count, last_id))
            for i, (entry, status) in enumerate(data):
                if i == len(data) - 1:
                    last_id = entry['reddit_id']

                saved_entry = db['entries'].find_one(reddit_id=entry['reddit_id'])

                if saved_entry is None:
                    status['eid'] = db['entries'].insert(entry)
                else:
                    status['eid'] = saved_entry['id']
                db['statuses'].insert(status)


            sleep(5)
        sleep(sleep_total - (time() - start_time))


if __name__ == "__main__":
    mine()
