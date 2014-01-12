import sqlite3
from matplotlib.pyplot import boxplot, show
from math import sqrt
from datetime import timedelta
from dateutil.parser import parse

conn = sqlite3.connect('./reddit.db')


def get_entries():
    entries = {}
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT exid, rank, observed FROM
            (SELECT DISTINCT eid as exid FROM status
            WHERE rank=1 AND observed < date("now", "start of day")
            ORDER BY (upvotes - downvotes) DESC) JOIN status ON exid=status.eid
        ORDER BY exid, observed ASC;
        """
    )

    result = cursor.fetchone()
    while result is not None:
        entries.setdefault(result[0], [])
        entries[result[0]].append((result[1], parse(result[2])))
        result = cursor.fetchone()
    cursor.close()

    return entries


def get_time_to_first(entry):
    time_to_first = timedelta()

    previous = timedelta()
    for i, status in enumerate(entry):
        if status[0] == 1:
            break

        if i != 0:
            time_to_first += status[1] - previous
        previous = status[1]

    return time_to_first

def get_times():
    return filter(lambda x: x.total_seconds() != 0, [get_time_to_first(entry) for eid, entry in get_entries().iteritems()])

def get_mean_time(times):
    return reduce(lambda x, y: x+y, times) / len(times)

def get_standard_deviation(times, mean):
    mean_seconds = mean.total_seconds()
    return timedelta(0, sqrt((1 / float(len(times) - 1)) * reduce(lambda x, y: x + y, [(time.total_seconds() - mean_seconds) ** 2 for time in times])))

if __name__ == "__main__":
    times = get_times()
    mean = get_mean_time(times)
    plot = boxplot([time.total_seconds() for time in times], sym='o')
    show()
    print u"%s \u00B1 %s" % (mean, (2 * get_standard_deviation(times, mean)))
