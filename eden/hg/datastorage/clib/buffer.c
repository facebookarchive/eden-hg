// Copyright 2016-present Facebook. All Rights Reserved.
//
// buffer.c: implementation for a generic mechanism to expand a heap-allocated
//           buffer.
//
// no-check-code

#include <stdlib.h>
#include <string.h>

#include "buffer.h"

bool buffer_append(
    char **buffer, size_t *buffer_idx, size_t *buffer_sz,
    char *input, size_t input_sz,
    const float factor,
    const size_t min_increment,
    const size_t max_increment) {
  if (expand_to_fit((void **) buffer, *buffer_idx, buffer_sz, input_sz,
      sizeof(char), factor, min_increment, max_increment) == false) {
    return false;
  }

  memcpy(&(*buffer)[*buffer_idx], input, input_sz);
  *buffer_idx += input_sz;

  return true;
}
