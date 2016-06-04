import sys

import numpy as np
from sklearn.svm import SVC
from sklearn import tree
from sklearn.ensemble import RandomForestClassifier, AdaBoostClassifier
from sklearn.multiclass import OneVsRestClassifier
import math

FEATURESTART = 5
FEATURELEN = 7
PARTAVG = 0
PARTSKEW = 1
RECAVG = 2
LATENCY = 3
READRATE = 4
HOMECONF = 5
CONFRATE = 6

threshold = 0

class Smallbank(object):

	def __init__(self, partFile, occFile, pureFile, indexFile):
		self.partclf = self.PartTrain(partFile)
		self.occclf = self.OCCTrain(occFile)
		self.pureclf = self.PureTrain(pureFile)
		self.indexclf = self.IndexTrain(indexFile)
		self.indexprob = 0.0
		self.pureprob = 0.0
		self.occprob = 0.0
		self.partprob = 0.0
		self.totalprob = 0.0

	def PartTrain(self, f):
		# X is feature, while Y is label
		X = []
		Y = []
		for line in open(f):
			columns = [float(x) for x in line.strip().split('\t')[FEATURESTART:]]
			tmp = []
			tmp.extend(columns[PARTAVG:PARTSKEW])
			tmp.extend(columns[RECAVG:CONFRATE])
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
			tmp.extend(columns[RECAVG:CONFRATE])
			X.append(tmp)
			if (columns[FEATURELEN] == 1):
				Y.extend([1])
			elif (columns[FEATURELEN] == 2):
				Y.extend([2])
		occclf = tree.DecisionTreeClassifier(max_depth=6)
		occclf = occclf.fit(np.array(X), np.array(Y))
		return occclf

	def PureTrain(self, f):
		X = []
		Y = []
		for line in open(f):
			columns = [float(x) for x in line.strip().split('\t')[FEATURESTART:]]
			tmp = []
			tmp.extend(columns[RECAVG:HOMECONF])
			tmp.extend(columns[CONFRATE:FEATURELEN])
			X.append(tmp)
			if (columns[FEATURELEN] == 3):
				Y.extend([3])
				if len(columns[FEATURELEN:]) == 2:
					if columns[FEATURELEN+1] == 4:
						X.append(tmp)
						Y.extend([4])
			elif (columns[FEATURELEN] == 4):
				Y.extend([4])
		pureclf = tree.DecisionTreeClassifier(max_depth=4)
		pureclf = pureclf.fit(np.array(X), np.array(Y))
		return pureclf


	def IndexTrain(self, f):
		# X is feature, while Y is label
		X = []
		Y = []
		for line in open(f):
			columns = [float(x) for x in line.strip().split('\t')[FEATURESTART:]]
			tmp = []
			tmp.extend(columns[PARTAVG:RECAVG])
			tmp.extend(columns[LATENCY:READRATE])
			tmp.extend(columns[HOMECONF:CONFRATE])
			ok1 = 0
			ok2 = 0
			for _, y in enumerate(columns[FEATURELEN:]):
				if y <= 2:
					ok1 = 1
				if y > 2:
					ok2 = 1
			#if ok1 == 1:
			#	X.append(tmp)
			#	Y.extend([0])
			if ok2 == 1:
				X.append(tmp)
				Y.extend([1])
			else:
				X.append(tmp)
				Y.extend([0])

		indexclf = tree.DecisionTreeClassifier(max_depth=4)
		indexclf = indexclf.fit(np.array(X), np.array(Y))
		return indexclf


	def Predict(self, curType, partAvg, partSkew, recAvg, latency, readRate, homeconf, confRate):
		testIndex = [[partAvg, partSkew, latency, homeconf]]
		result = self.indexclf.predict(testIndex)
		self.indexprob = self.indexclf.predict_proba(testIndex)[0][result[0]]
		self.totalprob = self.indexprob
		cur = 0
		if curType > 2:
			cur = 1
		if self.indexprob > threshold:
			cur = result[0]

		if cur == 0: # Switch within partitioned index
			testPart = [[partAvg, recAvg, latency, readRate, homeconf]]
			result = self.partclf.predict(testPart)
			self.partprob = self.partclf.predict_proba(testPart)[0][result[0]]
			self.totalprob = self.partprob * self.totalprob
			# cur represent PCC or not
			cur = 0
			if curType > 0:
				cur = 1
			if self.partprob > threshold: # use result[0]
				cur = result[0]

			if cur == 0:
				return 0
			else:
				if curType > 2:
					curType = curType - 2
				testOCC = [[recAvg, latency, readRate, homeconf]]
				result = self.occclf.predict(testOCC)
				self.occprob = self.occclf.predict_proba(testOCC)[0][result[0] - 1]
				self.totalprob = self.occprob * self.totalprob
				if self.occprob > threshold:
					return result[0]
				return curType
		else:
			if curType <= 2:
				curType = curType + 2
			if curType == 2:
				curType = 4
			testPure = [[recAvg, latency, readRate, confRate]]
			result = self.pureclf.predict(testPure)
			self.pureprob = self.pureclf.predict_proba(testPure)[0][result[0] - 3]
			self.totalprob = self.pureprob * self.totalprob
			if self.pureprob > threshold:
				return result[0]
			return curType

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
