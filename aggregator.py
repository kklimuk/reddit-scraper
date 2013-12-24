from jinja2 import Template
from urlparse import urlparse, parse_qs
import dataset

db = dataset.connect('sqlite:///reddit.db')
response = db.query('SELECT title, link, min(rank) as rank, (upvotes - downvotes) as votes, subreddit FROM status JOIN entry ON status.eid=entry.id WHERE rank <= 10 AND observed > date("now", "start of day") AND observed < date("now", "start of day", "+1 day") GROUP BY eid ORDER BY rank, (upvotes - downvotes) DESC;')

def process(item):
    if 'youtube' in item['link']:
        item['embed'] = 'http://www.youtube.com/embed/%s' % parse_qs(urlparse(item['link']).query)['v'][0]
    elif item['link'][0] == '/':
        item['link'] = 'http://reddit.com%s' % item['link']
    return item

collection = map(
    process,
    filter(
        lambda item: item['link'] is not None,
        [item for item in response]
    )
)

with open('./templates/newsletter.html', 'r') as newspaper:
    template = Template(newspaper.read())
    print template.render(collection=collection).encode('utf-8')