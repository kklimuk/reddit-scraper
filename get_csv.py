import dataset

db = dataset.connect('postgresql://foobar:foobarbaz@testdb.cy2ub2trrp92.us-east-1.rds.amazonaws.com:5432/reddit')

result = db['entries'].all()
dataset.freeze(result, format='csv', filename='entries.csv')