#!/bin/bash
# This script is intended to be used to sync a copy of the code
# from the mercurial upstream repo into the eden repo.
if ! test -f .arcconfig -a -f eden/hg/datastorage/sync.sh ; then
  echo "Must be run from the fbcode dir"
  exit 1
fi
for dir in cdatapack clib cstore ctreemanifest portability ; do
  rsync -a --delete ~/local/facebook-hg-rpms/fb-hgext/$dir/ eden/hg/datastorage/$dir
done
