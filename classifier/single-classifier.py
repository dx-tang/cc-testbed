import sys

import numpy as np
from sklearn.svm import SVC
from sklearn import tree
from sklearn.ensemble import RandomForestClassifier, AdaBoostClassifier
from sklearn.multiclass import OneVsRestClassifier
import math

FEATURESTART = 7
FEATURELEN = 7
PARTAVG = 0
PARTSKEW = 1
RECAVG = 2
LATENCY = 3
READRATE = 4
HOMECONF = 5
CONFRATE = 6

threshold_part = 0.9
threshold = 0.7

class Single(object):

	def __init__(self, partFile, occFile, pureFile, IndexFile):
		self.partclf = self.PartTrain(partFile)
		self.occclf = self.OCCTrain(occFile)
		self.partprob = 0.0
		self.occprob = 0.0
		self.totalprob = 0.0

	def PartTrain(self, f):
		# X is feature, while Y is label
		X = []
		Y = []
		for line in open(f):
			columns = [float(x) for x in line.strip().split('\t')[FEATURESTART:]]
			tmp = []
			tmp.extend(columns[PARTAVG:RECAVG])
			tmp.extend(columns[RECAVG:LATENCY])
			tmp.extend(columns[READRATE:CONFRATE])
			X.append(tmp)
			if (columns[FEATURELEN] == 0):
				Y.extend([0])
			else:
				Y.extend([1])
		partclf = tree.DecisionTreeClassifier(max_depth=6)
		partclf = partclf.fit(np.array(X), np.array(Y))
		return partclf

	def OCCTrain(self, f):
		# X is feature, while Y is label
		X = []
		Y = []
		for line in open(f):
			columns = [float(x) for x in line.strip().split('\t')[FEATURESTART:]]
			tmp = []
			tmp.extend(columns[RECAVG:LATENCY])
			tmp.extend(columns[READRATE:HOMECONF])
			tmp.extend(columns[CONFRATE:FEATURELEN])
			for _, y in enumerate(columns[FEATURELEN:]):
				if y == 1:
					X.append(tmp)
					Y.extend([1])
				if y == 2:
					X.append(tmp)
					Y.extend([2])
		occclf = tree.DecisionTreeClassifier(max_depth=4)
		occclf = occclf.fit(np.array(X), np.array(Y))
		return occclf

	def Predict(self, curType, partAvg, partSkew, recAvg, latency, readRate, homeconf, confRate):
		testPart = [[partAvg, partSkew, recAvg, readRate, homeconf]]
		result = self.partclf.predict(testPart)
		self.partprob = self.partclf.predict_proba(testPart)[0][result[0]]
		self.totalprob = self.partprob
		# cur represent PCC or not
		cur = 0
		if curType > 0:
			cur = 1
		if self.partprob > threshold_part: # use result[0]
			cur = result[0]

		if cur == 0:
			return 0
		else:
			testOCC = [[recAvg, readRate, confRate]]
			result = self.occclf.predict(testOCC)
			self.occprob = self.occclf.predict_proba(testOCC)[0][result[0] - 1]
			return result[0]

	def GetIndexProb(self):
		return self.indexprob

	def GetPartProb(self):
		return self.partprob

	def GetOCCProb(self):
		return self.occprob

	def GetPureProb(self):
		return self.pureprob

	def GetTotalProb(self):
		return self.totalprob
