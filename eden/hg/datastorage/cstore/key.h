// key.h - c++ declarations for a key to pack data
//
// Copyright 2017 Facebook, Inc.
//
// This software may be used and distributed according to the terms of the
// GNU General Public License version 2 or any later version.
//
// no-check-code

#ifndef KEY_H
#define KEY_H

#include <cstring>
#include <vector>
#include <stdexcept>

#include "../ctreemanifest/convert.h"

/* Represents a key into the Mercurial store. Each key is a (name, node) pair,
 * though store implementations can choose to ignore the name in some cases. */
struct Key {
  /* The filename portion of the key. */
  std::string name;

  /* The byte node portion of the key. */
  char node[BIN_NODE_SIZE];

  Key() :
    node() {}

  Key(const char *name, size_t namelen, const char *node, size_t nodelen) :
    name(name, namelen) {
    if (nodelen != BIN_NODE_SIZE) {
      throw std::logic_error("invalid node length");
    }

    memcpy(this->node, node, BIN_NODE_SIZE);
  }
};

class MissingKeyError : public std::runtime_error {
  public:
    MissingKeyError(const char *what_arg) :
      std::runtime_error(what_arg) {
    }
};

class KeyIterator {
  protected:
    KeyIterator() {}
  public:
    virtual ~KeyIterator() = default;
    virtual Key *next() = 0;
};

#endif //KEY_H
