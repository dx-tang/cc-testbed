#ifndef SINGLEOCC_H
#define SINGLEOCC_H

#include <Python.h>


void Init(char *addPath);
PyObject* SingleOCCTrain(char *f);
PyObject* SinglePartTrain(char *f);
PyObject* SBOCCTrain(char *f);
PyObject* SBPartTrain(char *f);
long SingleOCCPredict(PyObject *pInstance, double recAvg, double hitRate, double readRate, double confRate);
long SinglePartPredict(PyObject *pInstance, double partAvg, double partSkew, double partLenSkew, double recAvg, double hitRate, double readRate);
long SBOCCPredict(PyObject *pInstance, double recAvg, double hitRate, double readRate, double confRate);
long SBPartPredict(PyObject *pInstance, double partAvg, double partSkew, double partLenSkew, double recAvg, double hitRate, double readRate);
void Final();

#endif
