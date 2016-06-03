#include "classifier.h"

void Init(char *addPath) {
    Py_Initialize();
    PyRun_SimpleString("import sys");
    PyRun_SimpleString(addPath); 
}

PyObject* SingleTrain(char *partFile, char *occFile, char *pureFile, char *indexFile) {
    PyObject *pModule = NULL, *pDict = NULL, *pClass = NULL, *pInstance = NULL, *pArg = NULL;

    pModule = PyImport_ImportModule("single-classifier");
    if (pModule == NULL) {
        return NULL;
    }

    pDict = PyModule_GetDict(pModule);
    if (pDict == NULL) {
        return NULL;
    }

    pClass = PyDict_GetItemString(pDict, "Single");
    if (pDict == NULL) {
        return NULL;
    }

    pArg = Py_BuildValue("(s,s,s,s)", partFile, occFile, pureFile, indexFile);
    pInstance = PyObject_CallObject(pClass, pArg);
    return pInstance;
}

PyObject* SBTrain(char *partFile, char *occFile, char *pureFile, char *indexFile) {
    PyObject *pModule = NULL, *pDict = NULL, *pClass = NULL, *pInstance = NULL, *pArg = NULL;

    pModule = PyImport_ImportModule("sb-classifier");
    if (pModule == NULL) {
        return NULL;
    }

    pDict = PyModule_GetDict(pModule);
    if (pDict == NULL) {
        return NULL;
    }

    pClass = PyDict_GetItemString(pDict, "Smallbank");
    if (pDict == NULL) {
        return NULL;
    }

    pArg = Py_BuildValue("(s,s,s,s)", partFile, occFile, pureFile, indexFile);
    pInstance = PyObject_CallObject(pClass, pArg);
    return pInstance;
}

long Predict(PyObject *pInstance, int curType, double partAvg, double partSkew, double recAvg, double latency, double readRate, double homeconf, double confRate) {
	PyObject *result = PyObject_CallMethod(pInstance, "Predict", "(i,d,d,d,d,d,d,d)", curType, partAvg, partSkew, recAvg, latency, readRate, homeconf, confRate);
    if (result == NULL) {
        return -1;
    }
	return PyLong_AsLong(result);
}

double GetIndexProb(PyObject *pInstance) {
    PyObject *result = PyObject_CallMethod(pInstance, "GetIndexProb", NULL);
    if (result == NULL) {
        return -1;
    }
    return PyFloat_AsDouble(result);
}

double GetPartProb(PyObject *pInstance) {
    PyObject *result = PyObject_CallMethod(pInstance, "GetPartProb", NULL);
    if (result == NULL) {
        return -1;
    }
    return PyFloat_AsDouble(result);
}

double GetOCCProb(PyObject *pInstance) {
    PyObject *result = PyObject_CallMethod(pInstance, "GetOCCProb", NULL);
    if (result == NULL) {
        return -1;
    }
    return PyFloat_AsDouble(result);
}

double GetPureProb(PyObject *pInstance) {
    PyObject *result = PyObject_CallMethod(pInstance, "GetPureProb", NULL);
    if (result == NULL) {
        return -1;
    }
    return PyFloat_AsDouble(result);
}

void Final() {
    Py_Finalize();
}

