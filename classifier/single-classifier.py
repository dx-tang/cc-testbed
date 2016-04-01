import sys

import numpy as np
from sklearn.svm import SVC
from sklearn import tree
from sklearn.ensemble import RandomForestClassifier, AdaBoostClassifier
from sklearn.multiclass import OneVsRestClassifier
import math

START = 7
FEATURELEN = 6
PARTAVG = 0
PARTSKEW = 1
RECAVG = 2
LATENCY = 3
READRATE = 4
CONFRATE = 5

class SinglePart(object):

	def __init__(self, f):
		self.clf = self.train(f)

	# X is feature, while Y is label
	def train(self, f):
		X = []
		Y = []
		for line in open(f):
			columns = [float(x) for x in line.strip().split('\t')[START:]]
			tmpX = []
			tmpX.extend(columns[:FEATURELEN])
			#tmpX.extend([math.ceil(columns[3])])
			#tmpX.extend(columns[4:7])
			X.append(tmpX)
			if (columns[FEATURELEN] == 0):
				Y.extend([0])
			else:
				Y.extend([1])
		clf = tree.DecisionTreeClassifier(max_depth=6)
		clf = clf.fit(X, Y)
		return clf

	def Predict(self, partAvg, partSkew, recAvg, latency, readRate, confRate):
		X = [[partAvg, partSkew, recAvg, latency, readRate, confRate]]
		return self.clf.predict(X)[0]

class SingleOCC(object):

	def __init__(self, f):
		self.clf = self.train(f)

	def train(self, f):
		X = []
		Y = []
		for line in open(f):
			columns = [float(x) for x in line.strip().split('\t')[START:]]
			if (columns[FEATURELEN] != 0):
				if (len(columns) <= FEATURELEN + 1 or (len(columns) > FEATURELEN + 1 and columns[FEATURELEN+1] != 0)):
					tmp = []
					tmp.extend(columns[RECAVG:FEATURELEN])
					X.append(tmp)
					#if (len(columns) > FEATURELEN + 1):
					#	Y.extend([3])
					if (columns[FEATURELEN] == 1):
						Y.extend([1])
					elif (columns[FEATURELEN] == 2):
						Y.extend([2])
		clf = tree.DecisionTreeClassifier(max_depth=6)
		clf = clf.fit(X, Y)
		return clf

	def Predict(self, recAvg, hitRate, readRate, confRate):
		X = [[recAvg, hitRate, readRate, confRate]]
		return self.clf.predict(X)[0]

