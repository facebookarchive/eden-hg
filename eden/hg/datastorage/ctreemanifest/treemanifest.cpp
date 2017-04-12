// treemanifest.cpp - c++ implementation of a tree manifest
//
// Copyright 2016 Facebook, Inc.
//
// This software may be used and distributed according to the terms of the
// GNU General Public License version 2 or any later version.
//
// no-check-code

#include "treemanifest.h"
#include <cassert>

/**
 * Helper function that performs the actual recursion on the tree entries.
 */
void treemanifest_diffrecurse(
    Manifest *selfmf,
    Manifest *othermf,
    std::string &path,
    DiffResult &diff,
    const ManifestFetcher &fetcher,
    bool clean,
    Matcher &matcher) {
  ManifestIterator selfiter;
  ManifestIterator otheriter;

  if (selfmf != NULL) {
    selfiter = selfmf->getIterator();
  }
  if (othermf != NULL) {
    otheriter = othermf->getIterator();
  }

  // Iterate through both directory contents
  while (!selfiter.isfinished() || !otheriter.isfinished()) {
    int cmp = 0;

    ManifestEntry *selfentry = NULL;
    std::string selfbinnode;
    if (!selfiter.isfinished()) {
      cmp--;
      selfentry = selfiter.currentvalue();
      if (selfentry->node) {
        selfbinnode = binfromhex(selfentry->node);
      } else {
        assert(selfentry->isdirectory());
      }
    }

    ManifestEntry *otherentry = NULL;
    std::string otherbinnode;
    if (!otheriter.isfinished()) {
      cmp++;
      otherentry = otheriter.currentvalue();
      if (otherentry->node) {
        otherbinnode = binfromhex(otherentry->node);
      } else {
        assert(otherentry->isdirectory());
      }
    }

    // If both sides are present, cmp == 0, so do a filename comparison
    if (cmp == 0) {
      cmp = strcmp(selfentry->filename, otherentry->filename);
    }

    size_t originalpathsize = path.size();
    if (cmp < 0) {
      // selfentry should be processed first and only exists in self
      selfentry->appendtopath(path);
      if (selfentry->isdirectory()) {
        if (matcher.visitdir(path)) {
          Manifest *selfchildmanifest = selfentry->get_manifest(
              fetcher, path.c_str(), path.size());
          treemanifest_diffrecurse(selfchildmanifest, NULL, path, diff, fetcher,
                                   clean, matcher);
        }
      } else if (matcher.matches(path)) {
        diff.add(path, selfbinnode.c_str(), selfentry->flag, NULL, NULL);
      }
      selfiter.next();
    } else if (cmp > 0) {
      // otherentry should be processed first and only exists in other
      otherentry->appendtopath(path);
      if (otherentry->isdirectory()) {
        if (matcher.visitdir(path)) {
          Manifest *otherchildmanifest = otherentry->get_manifest(
              fetcher, path.c_str(), path.size());
          treemanifest_diffrecurse(NULL, otherchildmanifest, path, diff,
                                   fetcher, clean, matcher);
        }
      } else if (matcher.matches(path)) {
        diff.add(path, NULL, NULL, otherbinnode.c_str(), otherentry->flag);
      }
      otheriter.next();
    } else {
      // Append the non-directory form to the path when possible
      if (!selfentry->isdirectory()) {
        selfentry->appendtopath(path);
      } else {
        otherentry->appendtopath(path);
      }

      // Filenames match - now compare directory vs file
      if (selfentry->isdirectory() && otherentry->isdirectory()) {
        // Both are directories - recurse
        if (matcher.visitdir(path) &&
            (selfbinnode != otherbinnode || clean || selfbinnode.size() == 0)) {
          Manifest *selfchildmanifest = selfentry->get_manifest(fetcher,
              path.c_str(), path.size());
          Manifest *otherchildmanifest = otherentry->get_manifest(fetcher,
              path.c_str(), path.size());

          treemanifest_diffrecurse(
              selfchildmanifest,
              otherchildmanifest,
              path,
              diff,
              fetcher,
              clean,
              matcher);
        }
      } else if (selfentry->isdirectory() && !otherentry->isdirectory()) {
        if (matcher.matches(path)) {
          // self is directory, other is not - process other then self
          diff.add(path, NULL, NULL, otherbinnode.c_str(), otherentry->flag);
        }

        if (matcher.visitdir(path)) {
          path.append(1, '/');
          Manifest *selfchildmanifest = selfentry->get_manifest(fetcher,
              path.c_str(), path.size());
          treemanifest_diffrecurse(selfchildmanifest, NULL, path, diff, fetcher,
                                   clean, matcher);
        }
      } else if (!selfentry->isdirectory() && otherentry->isdirectory()) {
        if (matcher.matches(path)) {
          // self is not directory, other is - process self then other
          diff.add(path, selfbinnode.c_str(), selfentry->flag, NULL, NULL);
        }

        if (matcher.visitdir(path)) {
          path.append(1, '/');
          Manifest *otherchildmanifest = otherentry->get_manifest(fetcher,
              path.c_str(), path.size()
          );
          treemanifest_diffrecurse(NULL, otherchildmanifest, path, diff,
                                   fetcher, clean, matcher);
        }
      } else {
        // both are files
        if (matcher.matches(path)) {
          bool flagsdiffer = (
              (selfentry->flag && otherentry->flag && *selfentry->flag != *otherentry->flag) ||
              ((bool)selfentry->flag != (bool)otherentry->flag)
          );

          if (selfbinnode != otherbinnode || flagsdiffer) {
            diff.add(path, selfbinnode.c_str(), selfentry->flag,
                           otherbinnode.c_str(), otherentry->flag);
          } else if (clean) {
            diff.addclean(path);
          }
        }
      }

      selfiter.next();
      otheriter.next();
    }
    path.erase(originalpathsize);
  }
}

FindResult treemanifest::find(
    ManifestEntry *manifestentry,
    PathIterator &path,
    FindMode findMode,
    FindContext *findContext,
    FindResult (*callback)(
        Manifest *manifest,
        const char *filename, size_t filenamelen,
        FindContext *findContext,
        ManifestPtr *resultManifest),
    ManifestPtr *resultManifest) {
  if (manifestentry->resolved.isnull()) {
    const char *pathstart;
    size_t pathlen;

    path.getPathToPosition(&pathstart, &pathlen);
    findContext->nodebuffer.erase();
    appendbinfromhex(manifestentry->node, findContext->nodebuffer);
    manifestentry->resolved = this->fetcher.get(pathstart, pathlen,
        findContext->nodebuffer);
  }
  ManifestPtr manifest = manifestentry->resolved;
  *resultManifest = manifest;

  FindResult result;

  const char *word = NULL;
  size_t wordlen = 0;

  path.next(&word, &wordlen);
  if (path.isfinished()) {
    // time to execute the callback.
    result = callback(manifest,
        word, wordlen,
        findContext,
        resultManifest);
  } else {
    // position the iterator at the right location
    bool exacthit;
    std::list<ManifestEntry>::iterator iterator = manifest->findChild(
        word, wordlen, RESULT_DIRECTORY, &exacthit);

    ManifestEntry *entry;

    if (!exacthit) {
      // do we create the intermediate node?
      if (findMode != CREATE_IF_MISSING) {
        return FIND_PATH_NOT_FOUND;
      }

      if (!manifest->isMutable()) {
        manifest = ManifestPtr(manifest->copy());
        iterator = manifest->findChild(word, wordlen, RESULT_DIRECTORY,
                                       &exacthit);
      }

      // create the intermediate node...
      entry = manifest->addChild(
          iterator, word, wordlen, NULL, MANIFEST_DIRECTORY_FLAGPTR);
    } else {
      entry = &(*iterator);
    }

    // now find the next subdir
    ManifestPtr newChildManifest;
    result = find(
        entry,
        path,
        findMode,
        findContext,
        callback,
        &newChildManifest);

    // If the child was changed, apply it to this manifest
    if (!entry->resolved->isMutable() && newChildManifest->isMutable()) {
      // If we're not mutable, copy ourselves
      if (!manifest->isMutable()) {
        manifest = ManifestPtr(manifest->copy());
        // Refind the entry in the new iterator
        iterator = manifest->findChild(word, wordlen, RESULT_DIRECTORY,
                                       &exacthit);
        entry = &(*iterator);
      }

      // Replace the reference to the child
      entry->resolved = newChildManifest;
    }

    // if the child manifest has 0 entries, we may want to prune it, if the mode
    // indicates that we should.
    if (findMode == REMOVE_EMPTY_IMPLICIT_NODES) {
      if (newChildManifest->children() == 0) {
        if (!manifest->isMutable()) {
          manifest = ManifestPtr(manifest->copy());
          iterator = manifest->findChild(word, wordlen, RESULT_DIRECTORY,
                                         &exacthit);
          entry = &(*iterator);
        }

        manifest->removeChild(iterator);
      }
    }

    *resultManifest = manifest;

    if (findContext->invalidate_checksums) {
      if (!manifest->isMutable()) {
        throw std::logic_error("attempting to null node on immutable manifest");
      }
      entry->node = NULL;
    }
  }

  return result;
}

struct GetResult {
  std::string *resultnode;
  const char **resultflag;
  FindResultType resulttype;
};

static FindResult get_callback(
    Manifest *manifest,
    const char *filename, size_t filenamelen,
    FindContext *context,
    ManifestPtr *resultManifest) {
  GetResult *result = (GetResult *) context->extras;

  // position the iterator at the right location
  bool exacthit;
  std::list<ManifestEntry>::iterator iterator = manifest->findChild(
      filename, filenamelen, result->resulttype, &exacthit);

  if (!exacthit) {
    // TODO: not found. :( :(
    return FIND_PATH_NOT_FOUND;
  }

  ManifestEntry &entry = *iterator;

  result->resultnode->erase();
  if (entry.node != NULL) {
    appendbinfromhex(entry.node, *result->resultnode);
  }

  *result->resultflag = entry.flag;

  return FIND_PATH_OK;
}

bool treemanifest::get(
    const std::string &filename,
    std::string *resultnode, const char **resultflag,
    FindResultType resulttype) {
  getRootManifest();

  GetResult extras = {resultnode, resultflag, resulttype};
  PathIterator pathiter(filename);
  FindContext changes;
  changes.nodebuffer.reserve(BIN_NODE_SIZE);
  changes.extras = &extras;

  ManifestPtr resultManifest;
  FindResult result = this->find(
      &this->root,
      pathiter,
      BASIC_WALK,
      &changes,
      get_callback,
      &resultManifest
  );

  return result == FIND_PATH_OK;
}

struct SetParams {
  const std::string &resultnode;
  const char *resultflag;
};

static FindResult set_callback(
    Manifest *manifest,
    const char *filename, size_t filenamelen,
    FindContext *context,
    ManifestPtr *resultManifest) {
  SetParams *params = (SetParams *) context->extras;

  if (!manifest->isMutable()) {
    *resultManifest = ManifestPtr(manifest->copy());
    manifest = *resultManifest;
  }

  // position the iterator at the right location
  bool exacthit;
  std::list<ManifestEntry>::iterator iterator = manifest->findChild(
      filename, filenamelen, RESULT_FILE, &exacthit);

  if (!exacthit) {
    // create the entry, insert it.
    manifest->addChild(
        iterator,
        filename, filenamelen,
        params->resultnode.c_str(), params->resultflag);
  } else {
    ManifestEntry *entry = &(*iterator);

    entry->update(params->resultnode.c_str(), params->resultflag);
  }
  context->invalidate_checksums = true;

  return FIND_PATH_OK;
}

SetResult treemanifest::set(
    const std::string &filename,
    const std::string &resultnode,
    const char *resultflag) {
  SetParams extras = {resultnode, resultflag};
  PathIterator pathiter(filename);
  FindContext changes;
  changes.nodebuffer.reserve(BIN_NODE_SIZE);
  changes.extras = &extras;

  ManifestPtr resultManifest;
  FindResult result = this->find(
      &this->root,
      pathiter,
      CREATE_IF_MISSING,
      &changes,
      set_callback,
      &resultManifest
  );

  this->root.resolved = resultManifest;
  if (changes.invalidate_checksums) {
    this->root.node = NULL;
  }

  switch (result) {
    case FIND_PATH_OK:
      return SET_OK;
    case FIND_PATH_CONFLICT:
      return SET_CONFLICT;
    default:
      return SET_WTF;
  }
}

struct RemoveResult {
  bool found;
};

static FindResult remove_callback(
    Manifest *manifest,
    const char *filename, size_t filenamelen,
    FindContext *context,
    ManifestPtr *resultManifest) {
  RemoveResult *params = (RemoveResult *) context->extras;

  // position the iterator at the right location
  bool exacthit;
  std::list<ManifestEntry>::iterator iterator = manifest->findChild(
      filename, filenamelen, RESULT_FILE, &exacthit);

  if (exacthit) {
    if (!manifest->isMutable()) {
      *resultManifest = ManifestPtr(manifest->copy());
      manifest = *resultManifest;

      iterator = manifest->findChild(
        filename, filenamelen, RESULT_FILE, &exacthit);
    }

    manifest->removeChild(iterator);
    params->found = true;
    context->invalidate_checksums = true;
  }

  return FIND_PATH_OK;
}

bool treemanifest::remove(
    const std::string &filename) {
  RemoveResult extras = {false};
  PathIterator pathiter(filename);
  FindContext changes;
  changes.nodebuffer.reserve(BIN_NODE_SIZE);
  changes.extras = &extras;

  ManifestPtr resultManifest;
  FindResult result = this->find(
      &this->root,
      pathiter,
      REMOVE_EMPTY_IMPLICIT_NODES,
      &changes,
      remove_callback,
      &resultManifest
  );

  this->root.resolved = resultManifest;
  if (changes.invalidate_checksums) {
    this->root.node = NULL;
  }

  return (result == FIND_PATH_OK) && extras.found;
}

NewTreeIterator::NewTreeIterator(Manifest *mainRoot,
                const std::vector<char*> &cmpNodes,
                const std::vector<Manifest*> &cmpRoots,
                const ManifestFetcher &fetcher) :
    cmpNodes(cmpNodes),
    fetcher(fetcher) {
  this->mainStack.push_back(stackframe(mainRoot, false));

  for (size_t i = 0; i < cmpRoots.size(); i++) {
    Manifest *cmpRoot = cmpRoots[i];

    std::vector<stackframe> stack;
    stack.push_back(stackframe(cmpRoot, false));
    this->cmpStacks.push_back(stack);
  }
}

bool NewTreeIterator::popResult(std::string **path, ManifestNode **result,
                                ManifestNode **p1, ManifestNode **p2) {
  stackframe &mainFrame = this->mainStack.back();
  Manifest *mainManifest = mainFrame.manifest;
  std::string mainSerialized;

  // When we loop over the cmpStacks, record the cmp nodes that are parents
  // of the level we're about to return.
  memcpy(this->parents[0].node, NULLID, BIN_NODE_SIZE);
  memcpy(this->parents[1].node, NULLID, BIN_NODE_SIZE);

  bool alreadyExists = false;
  size_t matchingParent = -1;
  bool isRootManifest = this->mainStack.size() == 1;

  // Record the nodes of all cmp manifest equivalents
  for (size_t i = 0; i < cmpStacks.size(); i++) {
    // If a cmpstack is at the same level as the main stack, it represents
    // the same diretory and should be inspected.
    if (this->mainStack.size() == cmpStacks[i].size()) {
      std::vector<stackframe> &cmpStack = cmpStacks[i];
      Manifest *cmpManifest = cmpStack.back().manifest;

      if (!alreadyExists) {
        std::string cmpSerialized;
        cmpManifest->serialize(cmpSerialized);
        mainManifest->serialize(mainSerialized);

        // If the main manifest content is identical to a cmp content, we
        // shouldn't return it. Note: We already do this check when pushing
        // directories onto the stack, but for in-memory manifests we don't
        // know the node until after we've traversed the children, so we can't
        // verify their content until now.
        if (cmpSerialized.compare(mainSerialized) == 0) {
          alreadyExists = true;
          matchingParent = i;
        }
      }

      // Record the cmp parent nodes so later we can compute the main node
      if (cmpStack.size() > 1) {
        stackframe &priorCmpFrame = cmpStack[cmpStack.size() - 2];
        ManifestEntry *priorCmpEntry = priorCmpFrame.currentvalue();
        this->parents[i].manifest = cmpManifest;
        memcpy(this->parents[i].node, binfromhex(priorCmpEntry->node).c_str(),
               BIN_NODE_SIZE);
      } else {
        // Use the original passed in parent nodes
        this->parents[i].manifest = cmpManifest;
        memcpy(this->parents[i].node, binfromhex(this->cmpNodes[i]).c_str(),
               BIN_NODE_SIZE);
      }
    }
  }

  // We've finished processing this frame, so pop all the stacks
  this->mainStack.pop_back();
  for (size_t i = 0; i < cmpStacks.size(); i++) {
    if (this->mainStack.size() < cmpStacks[i].size()) {
      cmpStacks[i].pop_back();
    }
  }

  char tempnode[BIN_NODE_SIZE];
  if (alreadyExists && !isRootManifest) {
    // Reuse existing node if we're not the root node. This is important,
    // because otherwise we compute a new node for the same content, since p1/p2
    // will be different. The root node is special since it changes when
    // given a different p1/p2, regardless of content.
    memcpy(tempnode, this->parents[matchingParent].node, BIN_NODE_SIZE);
  } else {
    mainManifest->computeNode(this->parents[0].node, this->parents[1].node,
                              tempnode);
  }

  // Update the node on the manifest entry
  if (!isRootManifest) {
    // Peek back up the stack so we can put the right node on the
    // ManifestEntry.
    stackframe &priorFrame = mainStack[mainStack.size() - 1];
    ManifestEntry *priorEntry = priorFrame.currentvalue();

    std::string hexnode;
    hexfrombin(tempnode, hexnode);
    priorEntry->update(hexnode.c_str(), MANIFEST_DIRECTORY_FLAGPTR);

    // Now that it has a node, it is permanent and shouldn't be modified.
    priorEntry->resolved->markPermanent();
  }

  // If the current manifest has the same contents as a cmp manifest,
  // just give up now. Unless we're the root node (because the root node
  // will always change based on the parent nodes).
  if (alreadyExists && !isRootManifest) {
    size_t slashoffset = this->path.find_last_of('/', this->path.size() - 2);
    if (slashoffset == std::string::npos) {
      this->path.erase();
    } else {
      this->path.erase(slashoffset + 1);
    }
    return false;
  }

  this->result.manifest = mainManifest;
  memcpy(this->result.node, tempnode, BIN_NODE_SIZE);

  *path = &this->path;
  *result = &this->result;
  *p1 = &this->parents[0];
  *p2 = &this->parents[1];
  return true;
}

bool NewTreeIterator::processDirectory(ManifestEntry *mainEntry) {
  // mainEntry is a new entry we need to compare against each cmpEntry, and
  // then push if it is different from all of them.

  // First move all the cmp iterators forward to the same name as mainEntry.
  bool alreadyExists = false;
  std::vector<std::vector<stackframe>*> requirePush;
  for (size_t i = 0; i < cmpStacks.size(); i++) {
    std::vector<stackframe> &cmpStack = cmpStacks[i];

    // If the cmpStack is at a different level, it is not at the same
    // location as main, so don't bother searching it.
    if (cmpStack.size() < mainStack.size()) {
      continue;
    }

    stackframe &cmpFrame = cmpStack.back();

    // Move cmp iterator forward until we match or pass the current
    // mainEntry filename.
    while (!cmpFrame.isfinished()) {
      ManifestEntry *cmpEntry = cmpFrame.currentvalue();
      if (!cmpEntry->isdirectory() ) {
        cmpFrame.next();
        continue;
      }
      int cmp = ManifestEntry::compareName(cmpEntry, mainEntry);
      if (cmp >= 0) {
        // If the directory names match...
        if (cmp == 0) {
          // And the nodes match...
          if (!alreadyExists &&
              (mainEntry->node && memcmp(mainEntry->node, cmpEntry->node, HEX_NODE_SIZE) == 0)) {
            // Skip this entry
            alreadyExists = true;
          }
          // Remember this stack so we can push to it later
          requirePush.push_back(&cmpStack);
        }
        break;
      }
      cmpFrame.next();
    }
  }

  // If mainEntry matched any of the cmpEntries, we should skip mainEntry.
  if (alreadyExists) {
    assert(mainEntry->node != NULL);
    return false;
  }

  // Otherwise, push to the main stack
  mainEntry->appendtopath(this->path);
  Manifest *mainManifest = mainEntry->get_manifest(this->fetcher,
      this->path.c_str(), this->path.size());
  this->mainStack.push_back(stackframe(mainManifest, false));

  // And push all cmp stacks we remembered that have the same directory.
  for (size_t i = 0; i < requirePush.size(); i++) {
    std::vector<stackframe> *cmpStack = requirePush[i];
    ManifestEntry *cmpEntry = cmpStack->back().currentvalue();
    Manifest *cmpManifest = cmpEntry->get_manifest(this->fetcher,
        this->path.c_str(), this->path.size());
    cmpStack->push_back(stackframe(cmpManifest, false));
  }

  return true;
}

bool NewTreeIterator::next(std::string **path, ManifestNode **result,
                           ManifestNode **p1, ManifestNode **p2) {
  // Pop the last returned directory off the path
  size_t slashoffset = this->path.find_last_of('/', this->path.size() - 2);
  if (slashoffset == std::string::npos) {
    this->path.erase();
  } else {
    this->path.erase(slashoffset + 1);
  }

  while (true) {
    if (this->mainStack.empty()) {
      return false;
    }

    stackframe &mainFrame = this->mainStack.back();

    // If we've reached the end of this manifest, we've processed all the
    // children, so we can now return it.
    if (mainFrame.isfinished()) {
      // This can return false if this manifest ended up being equivalent to
      // a cmp parent manifest, which means we should skip it.
      if (this->popResult(path, result, p1, p2)) {
        if (this->mainStack.size() > 0) {
          this->mainStack.back().next();
        }
        return true;
      }
      if (this->mainStack.size() > 0) {
        this->mainStack.back().next();
      }
    } else {
      // Use currentvalue instead of next so that the stack of frames match the
      // actual current filepath.
      ManifestEntry *mainEntry = mainFrame.currentvalue();
      if (mainEntry->isdirectory()) {
        // If we're at a directory, process it, either by pushing it on the
        // stack, or by skipping it if it already matches a cmp parent.
        if (!this->processDirectory(mainEntry)) {
          mainFrame.next();
        }
      } else {
        mainFrame.next();
      }
    }
  }
}
