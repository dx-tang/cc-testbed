#ifndef SINGLEOCC_H
#define SINGLEOCC_H

#include <Python.h>


void Init(char *addPath);
PyObject* Train(char *partFile, char *occFile, char *pureFile, char *indexFile);
long Predict(PyObject *pInstance, int curType, double partAvg, double partSkew, double recAvg, double latency, double readRate, double homeconf, double confRate);
double GetIndexProb(PyObject *pInstance);
double GetPartProb(PyObject *pInstance);
double GetOCCProb(PyObject *pInstance);
double GetPureProb(PyObject *pInstance);
void Final();

#endif
