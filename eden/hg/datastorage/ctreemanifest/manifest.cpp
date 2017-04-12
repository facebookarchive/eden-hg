// manifest.cpp - c++ implementation of a single manifest
//
// Copyright 2016 Facebook, Inc.
//
// This software may be used and distributed according to the terms of the
// GNU General Public License version 2 or any later version.
//
// no-check-code

#include "manifest.h"

Manifest::Manifest(ConstantStringRef &rawobj) :
    _rawobj(rawobj),
    _refcount(0),
    _mutable(false) {
  char *parseptr = _rawobj.content();
  char *endptr = parseptr + _rawobj.size();

  while (parseptr < endptr) {
    ManifestEntry entry;
    parseptr = entry.initialize(parseptr);
    entries.push_back(entry);
  }
}

ManifestPtr Manifest::copy() {
  ManifestPtr copied(new Manifest());
  copied->_rawobj = this->_rawobj;

  for (std::list<ManifestEntry>::iterator thisIter = this->entries.begin();
       thisIter != this->entries.end();
       thisIter ++) {
    copied->addChild(copied->entries.end(), &(*thisIter));
  }

  return copied;
}

ManifestIterator Manifest::getIterator() {
  return ManifestIterator(this->entries.begin(), this->entries.end());
}

SortedManifestIterator Manifest::getSortedIterator() {
  // populate the sorted list if it's not present.
  if (this->entries.size() != this->mercurialSortedEntries.size()) {
    this->mercurialSortedEntries.clear();

    for (std::list<ManifestEntry>::iterator iterator = this->entries.begin();
         iterator != this->entries.end();
         iterator ++) {
      this->mercurialSortedEntries.push_back(&(*iterator));
    }

    this->mercurialSortedEntries.sort(ManifestEntry::compareMercurialOrder);
  }

  return SortedManifestIterator(
      this->mercurialSortedEntries.begin(),
      this->mercurialSortedEntries.end());
}

/**
 * Returns an iterator correctly positioned for a child of a given
 * filename.  If a child with the same name already exists, *exacthit will
 * be set to true.  Otherwise, it will be set to false.
 */
std::list<ManifestEntry>::iterator Manifest::findChild(
    const char *filename, const size_t filenamelen, FindResultType resulttype,
    bool *exacthit) {
  for (std::list<ManifestEntry>::iterator iter = this->entries.begin();
       iter != this->entries.end();
       iter ++) {
    size_t minlen = filenamelen < iter->filenamelen ?
                    filenamelen : iter->filenamelen;

    // continue until we are lexicographically <= than the current location.
    int cmp = strncmp(filename, iter->filename, minlen);
    bool current_isdir = iter->isdirectory();
    if (cmp == 0 && filenamelen == iter->filenamelen) {
      if ((current_isdir && resulttype != RESULT_FILE) ||
          (!current_isdir && resulttype != RESULT_DIRECTORY)) {
        *exacthit = true;
        return iter;
      } else if (current_isdir) {
        // the current entry we're looking at is a directory, but we want to
        // insert a file.  we need to move to the next entry.
        continue;
      } else {
        *exacthit = false;
        return iter;
      }
    } else if (cmp > 0 ||
        (cmp == 0 && filenamelen > iter->filenamelen)) {
      continue;
    } else {
      *exacthit = false;
      return iter;
    }
  }

  *exacthit = false;
  return this->entries.end();
}

ManifestEntry *Manifest::addChild(std::list<ManifestEntry>::iterator iterator,
    const char *filename, const size_t filenamelen, const char *node,
    const char *flag) {
  if (!this->isMutable()) {
    throw std::logic_error("attempting to mutate immutable Manifest");
  }

  ManifestEntry entry;
  this->entries.insert(iterator, entry);

  // move back to the element we just added.
  --iterator;

  // return a reference to the element we added, not the one on the stack.
  ManifestEntry *result = &(*iterator);

  result->initialize(filename, filenamelen, node, flag);

  // invalidate the mercurial-ordered list of entries
  this->mercurialSortedEntries.clear();

  return result;
}

ManifestEntry *Manifest::addChild(std::list<ManifestEntry>::iterator iterator,
        ManifestEntry *otherChild) {
  if (!this->isMutable()) {
    throw std::logic_error("attempting to mutate immutable Manifest");
  }

  ManifestEntry entry;
  iterator = this->entries.insert(iterator, entry);

  // return a reference to the element we added, not the one on the stack.
  ManifestEntry *result = &(*iterator);

  result->initialize(otherChild);

  // invalidate the mercurial-ordered list of entries
  this->mercurialSortedEntries.clear();

  return result;
}

ManifestIterator::ManifestIterator(
    std::list<ManifestEntry>::iterator iterator,
    std::list<ManifestEntry>::const_iterator end) :
    iterator(iterator), end(end) {
}

ManifestEntry *ManifestIterator::next() {
  if (this->isfinished()) {
    return NULL;
  }

  ManifestEntry *result = &(*this->iterator);
  this->iterator ++;

  return result;
}

ManifestEntry *ManifestIterator::currentvalue() const {
  if (this->isfinished()) {
    throw std::logic_error("iterator has no current value");
  }

  return &(*iterator);
}

bool ManifestIterator::isfinished() const {
  return iterator == end;
}

SortedManifestIterator::SortedManifestIterator(
    std::list<ManifestEntry *>::iterator iterator,
    std::list<ManifestEntry *>::const_iterator end) :
    iterator(iterator),
    end(end) {
}

ManifestEntry *SortedManifestIterator::next() {
  if (this->isfinished()) {
    return NULL;
  }

  ManifestEntry *result = *this->iterator;
  this->iterator ++;
  return result;
}

ManifestEntry *SortedManifestIterator::currentvalue() const {
  if (this->isfinished()) {
    throw std::logic_error("iterator has no current value");
  }

  return *iterator;
}

bool SortedManifestIterator::isfinished() const {
  return iterator == end;
}

void Manifest::serialize(std::string &result) {
  result.erase();
  result.reserve(16 * 1024 * 1024);
  ManifestIterator iterator = this->getIterator();
  ManifestEntry *entry;
  while ((entry = iterator.next()) != NULL) {
    result.append(entry->filename, entry->filenamelen);
    result.append("\0", 1);
    result.append(entry->node, HEX_NODE_SIZE);
    if (entry->flag) {
      result.append(entry->flag, 1);
    }
    result.append("\n", 1);
  }
}

void Manifest::computeNode(const char *p1, const char *p2, char *result) {
  std::string content;
  this->serialize(content);

  SHA_CTX ctx;
  SHA1_Init(&ctx);

  if (memcmp(p1, p2, BIN_NODE_SIZE) == -1) {
    SHA1_Update(&ctx, p1, BIN_NODE_SIZE);
    SHA1_Update(&ctx, p2, BIN_NODE_SIZE);
  } else {
    SHA1_Update(&ctx, p2, BIN_NODE_SIZE);
    SHA1_Update(&ctx, p1, BIN_NODE_SIZE);
  }
  SHA1_Update(&ctx, content.c_str(), content.size());

  SHA1_Final((unsigned char*)result, &ctx);
}

ManifestPtr::ManifestPtr() :
  manifest(NULL) {
}

ManifestPtr::ManifestPtr(Manifest *manifest) :
  manifest(manifest) {
  if (!manifest) {
    throw std::logic_error("passed NULL manifest pointer");
  }
  this->manifest->incref();
}

ManifestPtr::ManifestPtr(const ManifestPtr &other) :
  manifest(other.manifest) {
  if (this->manifest) {
    this->manifest->incref();
  }
}

ManifestPtr::~ManifestPtr() {
  if (this->manifest && this->manifest->decref() == 0) {
    delete(this->manifest);
  }
}

ManifestPtr& ManifestPtr::operator= (const ManifestPtr& other) {
  if (this->manifest) {
    this->manifest->decref();
  }
  this->manifest = other.manifest;
  if (this->manifest) {
    this->manifest->incref();
  }
  return *this;
}

ManifestPtr::operator Manifest* () const {
  return this->manifest;
}

Manifest *ManifestPtr::operator-> () {
  return this->manifest;
}

bool ManifestPtr::isnull() const {
  return this->manifest == NULL;
}

void Manifest::incref() {
  this->_refcount++;
}

size_t Manifest::decref() {
  this->_refcount--;
  return this->_refcount;
}

bool Manifest::isMutable() const {
  return this->_mutable;
}

void Manifest::markPermanent() {
  this->_mutable = false;
}
