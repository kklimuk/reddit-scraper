import thread
import atexit
from time import sleep
from parser import mine, setup_db


def main():
    queries = {
        "top_subreddits": """SELECT subreddit FROM
        (SELECT subreddit FROM entries JOIN statuses ON entries.id=statuses.eid
            WHERE observed > now() - interval '1 day' AND observed < now()
            GROUP BY subreddit, reddit_id) as subreddits
        GROUP BY subreddit ORDER BY count(*) DESC;"""
    }

    db = setup_db()
    sleep_total = 600
    thread.start_new_thread(mine, (), dict(sleep_total=sleep_total))

    subreddits = set()

    @atexit.register
    def show_subreddits():
        print subreddits

    while True:
        results = db.query(queries['top_subreddits'])
        for row in results:
            if row['subreddit'] not in subreddits:
                subreddits.add(row['subreddit'])
                thread.start_new_thread(mine, (), dict(mined_from=row['subreddit'].lower(), entry_count=100, sleep_total=1200))
        sleep(sleep_total)


if __name__ == '__main__':
    main()