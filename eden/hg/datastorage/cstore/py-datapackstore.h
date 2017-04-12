// py-cstore.cpp - c++ implementation of a store
//
// Copyright 2017 Facebook, Inc.
//
// This software may be used and distributed according to the terms of the
// GNU General Public License version 2 or any later version.
//
// no-check-code

// The PY_SSIZE_T_CLEAN define must be defined before the Python.h include,
// as per the documentation.
#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <memory>
#include <string>

extern "C" {
#include "../cdatapack/cdatapack.h"
}

#include "pythonutil.h"
#include "datapackstore.h"
#include "key.h"
#include "py-structs.h"
#include "uniondatapackstore.h"

// --------- DatapackStore Implementation ---------

/*
 * Initializes the contents of a datapackstore
 */
static int datapackstore_init(py_datapackstore *self, PyObject *args) {
  char *packdir;
  Py_ssize_t packdirlen;

  if (!PyArg_ParseTuple(args, "s#", &packdir, &packdirlen)) {
    return -1;
  }

  // We have to manually call the member constructor, since the provided 'self'
  // is just zerod out memory.
  try {
    std::string packdirstr(packdir, packdirlen);
    new(&self->datapackstore) DatapackStore(packdirstr);
  } catch (const std::exception &ex) {
    PyErr_SetString(PyExc_RuntimeError, ex.what());
    return -1;
  }

  return 0;
}

static void datapackstore_dealloc(py_datapackstore *self) {
  // When py_datapackstore is free'd by python in the PyObject_Del below,
  // its destructor is not called, so we need to manually destruct the members.
  self->datapackstore.~DatapackStore();
  PyObject_Del(self);
}

static PyObject *datapackstore_getdeltachain(py_datapackstore *self, PyObject *args) {
  try {
    char *name;
    Py_ssize_t namelen;
    char *node;
    Py_ssize_t nodelen;
    if (!PyArg_ParseTuple(args, "s#s#", &name, &namelen, &node, &nodelen)) {
      return NULL;
    }

    Key key(name, namelen, node, nodelen);

    DeltaChainIterator chain = self->datapackstore.getDeltaChain(key);

    PythonObj resultChain = PyList_New(0);

    delta_chain_link_t *link;
    size_t index = 0;
    while ((link = chain.next()) != NULL) {
      PythonObj name = PyString_FromStringAndSize(link->filename, link->filename_sz);
      PythonObj retnode = PyString_FromStringAndSize((const char *) link->node, NODE_SZ);
      PythonObj deltabasenode = PyString_FromStringAndSize(
          (const char *) link->deltabase_node, NODE_SZ);
      PythonObj delta = PyString_FromStringAndSize(
          (const char *) link->delta, (Py_ssize_t) link->delta_sz);

      PythonObj tuple = PyTuple_Pack(5, (PyObject*)name, (PyObject*)retnode,
                                        (PyObject*)name, (PyObject*)deltabasenode,
                                        (PyObject*)delta);

      if (PyList_Append((PyObject*)resultChain, tuple.returnval())) {
        return NULL;
      }

      index++;
    }

    return resultChain.returnval();
  } catch (const pyexception &ex) {
    return NULL;
  } catch (const MissingKeyError &ex) {
    PyErr_SetString(PyExc_KeyError, ex.what());
    return NULL;
  } catch (const std::exception &ex) {
    PyErr_SetString(PyExc_RuntimeError, ex.what());
    return NULL;
  }
}

class PythonKeyIterator : public KeyIterator {
  private:
    PythonObj _input;
    Key _current;
  public:
    PythonKeyIterator(PythonObj input) :
      _input(input) {}

    Key *next() {
      PyObject *item;
      while ((item = PyIter_Next((PyObject*)_input)) != NULL) {
        PythonObj itemObj = item;

        char *name;
        Py_ssize_t namelen;
        char *node;
        Py_ssize_t nodelen;
        if (!PyArg_ParseTuple(item, "s#s#", &name, &namelen, &node, &nodelen)) {
          throw pyexception();
        }

        _current = Key(name, namelen, node, nodelen);
        return &_current;
      }

      return NULL;
    }
};

static PyObject *datapackstore_getmissing(py_datapackstore *self, PyObject *keys) {
  try {
    PythonObj result = PyList_New(0);

    PythonObj inputIterator = PyObject_GetIter(keys);
    PythonKeyIterator keysIter(inputIterator);

    DatapackStoreKeyIterator missingIter = self->datapackstore.getMissing(keysIter);

    Key *key;
    while ((key = missingIter.next()) != NULL) {
      PythonObj missingKey = Py_BuildValue("(s#s#)", key->name.c_str(), key->name.size(),
                                                     key->node, 20);
      if (PyList_Append(result, (PyObject*)missingKey)) {
        return NULL;
      }
    }

    return result.returnval();
  } catch (const pyexception &ex) {
    return NULL;
  } catch (const std::exception &ex) {
    PyErr_SetString(PyExc_RuntimeError, ex.what());
    return NULL;
  }
}

static PyObject *datapackstore_markforrefresh(py_datapackstore *self) {
  self->datapackstore.markForRefresh();
  Py_RETURN_NONE;
}

// --------- DatapackStore Declaration ---------

static PyMethodDef datapackstore_methods[] = {
  {"getdeltachain", (PyCFunction)datapackstore_getdeltachain, METH_VARARGS, ""},
  {"getmissing", (PyCFunction)datapackstore_getmissing, METH_O, ""},
  {"markforrefresh", (PyCFunction)datapackstore_markforrefresh, METH_NOARGS, ""},
  {NULL, NULL}
};

static PyTypeObject datapackstoreType = {
  PyObject_HEAD_INIT(NULL)
  0,                                                /* ob_size */
  "cstore.datapackstore",                           /* tp_name */
  sizeof(py_datapackstore),                         /* tp_basicsize */
  0,                                                /* tp_itemsize */
  (destructor)datapackstore_dealloc,                /* tp_dealloc */
  0,                                                /* tp_print */
  0,                                                /* tp_getattr */
  0,                                                /* tp_setattr */
  0,                                                /* tp_compare */
  0,                                                /* tp_repr */
  0,                                                /* tp_as_number */
  0,                                                /* tp_as_sequence - length/contains */
  0,                                                /* tp_as_mapping - getitem/setitem */
  0,                                                /* tp_hash */
  0,                                                /* tp_call */
  0,                                                /* tp_str */
  0,                                                /* tp_getattro */
  0,                                                /* tp_setattro */
  0,                                                /* tp_as_buffer */
  Py_TPFLAGS_DEFAULT,                               /* tp_flags */
  "TODO",                                           /* tp_doc */
  0,                                                /* tp_traverse */
  0,                                                /* tp_clear */
  0,                                                /* tp_richcompare */
  0,                                                /* tp_weaklistoffset */
  0,                                                /* tp_iter */
  0,                                                /* tp_iternext */
  datapackstore_methods,                            /* tp_methods */
  0,                                                /* tp_members */
  0,                                                /* tp_getset */
  0,                                                /* tp_base */
  0,                                                /* tp_dict */
  0,                                                /* tp_descr_get */
  0,                                                /* tp_descr_set */
  0,                                                /* tp_dictoffset */
  (initproc)datapackstore_init,                     /* tp_init */
  0,                                                /* tp_alloc */
};

// --------- UnionDatapackStore Implementation ---------

/*
 * Initializes the contents of a uniondatapackstore
 */
static int uniondatapackstore_init(py_uniondatapackstore *self, PyObject *args) {
  PyObject *storeList;
  if (!PyArg_ParseTuple(args, "O", &storeList)) {
    return -1;
  }

  try {
    std::vector<DatapackStore*> stores;
    std::vector<PythonObj> pySubStores;

    PyObject *item;
    PythonObj inputIterator = PyObject_GetIter(storeList);
    while ((item = PyIter_Next(inputIterator)) != NULL) {
      // Record the substore references, so:
      // A) We can decref them in case of an error.
      // B) They don't get GC'd while the uniondatapackstore holds on to them.
      pySubStores.push_back(PythonObj(item));

      int isinstance = PyObject_IsInstance(item, (PyObject*)&datapackstoreType);
      if (isinstance == 0) {
        PyErr_SetString(PyExc_RuntimeError, "cuniondatapackstore only accepts cdatapackstore");
        return -1;
      } else if (isinstance != 1) {
        // Error
        return -1;
      }

      py_datapackstore *pySubStore = (py_datapackstore*)item;
      stores.push_back(&pySubStore->datapackstore);
    }

    // We have to manually call the member constructor, since the provided 'self'
    // is just zerod out memory.
    new(&self->uniondatapackstore) std::shared_ptr<UnionDatapackStore>(new UnionDatapackStore(stores));
    new(&self->substores) std::vector<PythonObj>();
    self->substores = pySubStores;
  } catch (const std::exception &ex) {
    PyErr_SetString(PyExc_RuntimeError, ex.what());
    return -1;
  }

  return 0;
}

static void uniondatapackstore_dealloc(py_uniondatapackstore *self) {
  self->uniondatapackstore.~shared_ptr<UnionDatapackStore>();
  self->substores.~vector<PythonObj>();
  PyObject_Del(self);
}

static PyObject *uniondatapackstore_get(py_uniondatapackstore *self, PyObject *args) {
  try {
    char *name;
    Py_ssize_t namelen;
    char *node;
    Py_ssize_t nodelen;
    if (!PyArg_ParseTuple(args, "s#s#", &name, &namelen, &node, &nodelen)) {
      return NULL;
    }

    Key key(name, namelen, node, nodelen);

    ConstantStringRef fulltext = self->uniondatapackstore->get(key);

    return PyString_FromStringAndSize(fulltext.content(), fulltext.size());
  } catch (const pyexception &ex) {
    return NULL;
  } catch (const MissingKeyError &ex) {
    PyErr_SetString(PyExc_KeyError, ex.what());
    return NULL;
  } catch (const std::exception &ex) {
    PyErr_SetString(PyExc_RuntimeError, ex.what());
    return NULL;
  }
}

static PyObject *uniondatapackstore_getdeltachain(py_uniondatapackstore *self, PyObject *args) {
  try {
    char *name;
    Py_ssize_t namelen;
    char *node;
    Py_ssize_t nodelen;
    if (!PyArg_ParseTuple(args, "s#s#", &name, &namelen, &node, &nodelen)) {
      return NULL;
    }

    Key key(name, namelen, node, nodelen);

    UnionDeltaChainIterator chain = self->uniondatapackstore->getDeltaChain(key);

    PythonObj resultChain = PyList_New(0);

    delta_chain_link_t *link;
    while ((link = chain.next()) != NULL) {
      PythonObj name = PyString_FromStringAndSize(link->filename, link->filename_sz);
      PythonObj retnode = PyString_FromStringAndSize((const char *) link->node, NODE_SZ);
      PythonObj deltabasenode = PyString_FromStringAndSize(
          (const char *) link->deltabase_node, NODE_SZ);
      PythonObj delta = PyString_FromStringAndSize(
          (const char *) link->delta, (Py_ssize_t) link->delta_sz);

      PythonObj tuple = PyTuple_Pack(5, (PyObject*)name, (PyObject*)retnode,
                                        (PyObject*)name, (PyObject*)deltabasenode,
                                        (PyObject*)delta);

      if (PyList_Append((PyObject*)resultChain, tuple.returnval())) {
        return NULL;
      }
    }

    return resultChain.returnval();
  } catch (const pyexception &ex) {
    return NULL;
  } catch (const MissingKeyError &ex) {
    PyErr_SetString(PyExc_KeyError, ex.what());
    return NULL;
  } catch (const std::exception &ex) {
    PyErr_SetString(PyExc_RuntimeError, ex.what());
    return NULL;
  }
}

static PyObject *uniondatapackstore_getmissing(py_uniondatapackstore *self, PyObject *keys) {
  try {
    PythonObj result = PyList_New(0);

    PythonObj inputIterator = PyObject_GetIter(keys);
    PythonKeyIterator keysIter((PyObject*)inputIterator);

    UnionDatapackStoreKeyIterator missingIter = self->uniondatapackstore->getMissing(keysIter);

    Key *key;
    while ((key = missingIter.next()) != NULL) {
      PythonObj missingKey = Py_BuildValue("(s#s#)", key->name.c_str(), key->name.size(),
                                                     key->node, 20);
      if (PyList_Append(result, (PyObject*)missingKey)) {
        return NULL;
      }
    }

    return result.returnval();
  } catch (const pyexception &ex) {
    return NULL;
  } catch (const std::exception &ex) {
    PyErr_SetString(PyExc_RuntimeError, ex.what());
    return NULL;
  }
}

static PyObject *uniondatapackstore_markforrefresh(py_uniondatapackstore *self) {
  self->uniondatapackstore->markForRefresh();
  Py_RETURN_NONE;
}

// --------- UnionDatapackStore Declaration ---------

static PyMethodDef uniondatapackstore_methods[] = {
  {"get", (PyCFunction)uniondatapackstore_get, METH_VARARGS, ""},
  {"getdeltachain", (PyCFunction)uniondatapackstore_getdeltachain, METH_VARARGS, ""},
  {"getmissing", (PyCFunction)uniondatapackstore_getmissing, METH_O, ""},
  {"markforrefresh", (PyCFunction)uniondatapackstore_markforrefresh, METH_NOARGS, ""},
  {NULL, NULL}
};

static PyTypeObject uniondatapackstoreType = {
  PyObject_HEAD_INIT(NULL)
  0,                                                /* ob_size */
  "cstore.uniondatapackstore",                      /* tp_name */
  sizeof(py_uniondatapackstore),                    /* tp_basicsize */
  0,                                                /* tp_itemsize */
  (destructor)uniondatapackstore_dealloc,           /* tp_dealloc */
  0,                                                /* tp_print */
  0,                                                /* tp_getattr */
  0,                                                /* tp_setattr */
  0,                                                /* tp_compare */
  0,                                                /* tp_repr */
  0,                                                /* tp_as_number */
  0,                                                /* tp_as_sequence - length/contains */
  0,                                                /* tp_as_mapping - getitem/setitem */
  0,                                                /* tp_hash */
  0,                                                /* tp_call */
  0,                                                /* tp_str */
  0,                                                /* tp_getattro */
  0,                                                /* tp_setattro */
  0,                                                /* tp_as_buffer */
  Py_TPFLAGS_DEFAULT,                               /* tp_flags */
  "TODO",                                           /* tp_doc */
  0,                                                /* tp_traverse */
  0,                                                /* tp_clear */
  0,                                                /* tp_richcompare */
  0,                                                /* tp_weaklistoffset */
  0,                                                /* tp_iter */
  0,                                                /* tp_iternext */
  uniondatapackstore_methods,                       /* tp_methods */
  0,                                                /* tp_members */
  0,                                                /* tp_getset */
  0,                                                /* tp_base */
  0,                                                /* tp_dict */
  0,                                                /* tp_descr_get */
  0,                                                /* tp_descr_set */
  0,                                                /* tp_dictoffset */
  (initproc)uniondatapackstore_init,                /* tp_init */
  0,                                                /* tp_alloc */
};
