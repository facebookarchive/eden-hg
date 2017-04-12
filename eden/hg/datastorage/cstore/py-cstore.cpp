// py-cstore.cpp - C++ implementation of a store
//
// Copyright 2016 Facebook, Inc.
//
// This software may be used and distributed according to the terms of the
// GNU General Public License version 2 or any later version.
//
// no-check-code

// The PY_SSIZE_T_CLEAN define must be defined before the Python.h include,
// as per the documentation.
#define PY_SSIZE_T_CLEAN

#include <Python.h>

#include "py-cdatapack.h"
#include "py-datapackstore.h"
#include "py-treemanifest.h"

static PyMethodDef mod_methods[] = {
  {NULL, NULL}
};

static char mod_description[] =
    "Module containing a native store implementation";

PyMODINIT_FUNC initcstore(void) {
  PyObject *mod;

  mod = Py_InitModule3("cstore", mod_methods, mod_description);

  // Init cdatapack
  cdatapack_type.tp_new = PyType_GenericNew;
  if (PyType_Ready(&cdatapack_type) < 0) {
    return;
  }
  Py_INCREF(&cdatapack_type);
  PyModule_AddObject(mod, "datapack", (PyObject *)&cdatapack_type);

  // Init treemanifest
  treemanifestType.tp_new = PyType_GenericNew;
  if (PyType_Ready(&treemanifestType) < 0) {
    return;
  }
  Py_INCREF(&treemanifestType);
  PyModule_AddObject(mod, "treemanifest", (PyObject *)&treemanifestType);

  // Init datapackstore
  datapackstoreType.tp_new = PyType_GenericNew;
  if (PyType_Ready(&datapackstoreType) < 0) {
    return;
  }
  Py_INCREF(&datapackstoreType);
  PyModule_AddObject(mod, "datapackstore", (PyObject *)&datapackstoreType);

  // Init datapackstore
  uniondatapackstoreType.tp_new = PyType_GenericNew;
  if (PyType_Ready(&uniondatapackstoreType) < 0) {
    return;
  }
  Py_INCREF(&uniondatapackstoreType);
  PyModule_AddObject(mod, "uniondatapackstore", (PyObject *)&uniondatapackstoreType);
}
