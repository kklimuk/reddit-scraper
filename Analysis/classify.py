from sklearn import naive_bayes
import sys
import csv
# import numpy
# import scipy

data = []
with open('curtable.csv', 'rb') as d:
	fileReader = csv.reader(d, delimiter=',', quotechar='|')

	for row in fileReader:
		data.append(row)
print data

classifer = naive_bayes.BernoulliNB(alpha=1.0)
classifer.fit(data[0], data[1], data[0])
