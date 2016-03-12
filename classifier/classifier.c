#include "classifier.h"

void Init(char *addPath) {
    Py_Initialize();
    PyRun_SimpleString("import sys");
    PyRun_SimpleString(addPath); 
}

PyObject* SingleOCCTrain(char *f) {
	PyObject *pModule = NULL, *pDict = NULL, *pClass = NULL, *pInstance = NULL, *pArg = NULL;

    pModule = PyImport_ImportModule("single-classifier");
    if (pModule == NULL) {
        return NULL;
    }

    pDict = PyModule_GetDict(pModule);
    if (pDict == NULL) {
        return NULL;
    }

    pClass = PyDict_GetItemString(pDict, "SingleOCC");
    if (pDict == NULL) {
        return NULL;
    }

    pArg = Py_BuildValue("(s)", f);
    pInstance = PyObject_CallObject(pClass, pArg);
    return pInstance;
}

PyObject* SinglePartTrain(char *f) {
    PyObject *pModule = NULL, *pDict = NULL, *pClass = NULL, *pInstance = NULL, *pArg = NULL;

    pModule = PyImport_ImportModule("single-classifier");
    if (pModule == NULL) {
        return NULL;
    }

    pDict = PyModule_GetDict(pModule);
    if (pDict == NULL) {
        return NULL;
    }

    pClass = PyDict_GetItemString(pDict, "SinglePart");
    if (pDict == NULL) {
        return NULL;
    }

    pArg = Py_BuildValue("(s)", f);
    pInstance = PyObject_CallObject(pClass, pArg);
    return pInstance;
}

PyObject* SBPartTrain(char *f) {
    PyObject *pModule = NULL, *pDict = NULL, *pClass = NULL, *pInstance = NULL, *pArg = NULL;

    pModule = PyImport_ImportModule("smallbank-classifier");
    if (pModule == NULL) {
        return NULL;
    }

    pDict = PyModule_GetDict(pModule);
    if (pDict == NULL) {
        return NULL;
    }

    pClass = PyDict_GetItemString(pDict, "SmallbankPart");
    if (pDict == NULL) {
        return NULL;
    }

    pArg = Py_BuildValue("(s)", f);
    pInstance = PyObject_CallObject(pClass, pArg);
    return pInstance;
}

PyObject* SBOCCTrain(char *f) {
    PyObject *pModule = NULL, *pDict = NULL, *pClass = NULL, *pInstance = NULL, *pArg = NULL;

    pModule = PyImport_ImportModule("smallbank-classifier");
    if (pModule == NULL) {
        return NULL;
    }

    pDict = PyModule_GetDict(pModule);
    if (pDict == NULL) {
        return NULL;
    }

    pClass = PyDict_GetItemString(pDict, "SmallbankOCC");
    if (pDict == NULL) {
        return NULL;
    }

    pArg = Py_BuildValue("(s)", f);
    pInstance = PyObject_CallObject(pClass, pArg);
    return pInstance;
}

long SingleOCCPredict(PyObject *pInstance, double recAvg, double latency, double readRate, double confRate) {
	PyObject *result = PyObject_CallMethod(pInstance, "Predict", "(d,d,d,d)", recAvg, latency, readRate, confRate);
    if (result == NULL) {
        return -1;
    }
	return PyLong_AsLong(result);
}

long SinglePartPredict(PyObject *pInstance, double partAvg, double partSkew, double partLenSkew, double recAvg, double latency, double readRate) {
    PyObject *result = PyObject_CallMethod(pInstance, "Predict", "(d,d,d,d,d,d)", partAvg, partSkew, partLenSkew, recAvg, latency, readRate);
    if (result == NULL) {
        return -1;
    }
    return PyLong_AsLong(result);
}

long SBOCCPredict(PyObject *pInstance, double recAvg, double latency, double readRate, double confRate) {
    PyObject *result = PyObject_CallMethod(pInstance, "Predict", "(d,d,d,d)", recAvg, latency, readRate, confRate);
    if (result == NULL) {
        return -1;
    }
    return PyLong_AsLong(result);
}

long SBPartPredict(PyObject *pInstance, double partAvg, double partSkew, double partLenSkew, double recAvg, double latency, double readRate) {
    PyObject *result = PyObject_CallMethod(pInstance, "Predict", "(d,d,d,d,d,d)", partAvg, partSkew, partLenSkew, recAvg, latency, readRate);
    if (result == NULL) {
        return -1;
    }
    return PyLong_AsLong(result);
}

void Final() {
    Py_Finalize();
}

