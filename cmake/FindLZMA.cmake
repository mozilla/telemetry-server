# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# The module defines the following variables
#   LZMA_INCLUDE_DIR
#   LZMA_LIBRARIES
#   LZMA_FOUND

IF (LZMA_INCLUDE_DIR)
  SET(LZMA_FIND_QUIETLY TRUE)
ENDIF (LZMA_INCLUDE_DIR)

FIND_PATH(LZMA_INCLUDE_DIR lzma.h)
FIND_LIBRARY(LZMA_LIBRARY NAMES lzma )

INCLUDE(FindPackageHandleStandardArgs)
FIND_PACKAGE_HANDLE_STANDARD_ARGS(LZMA DEFAULT_MSG LZMA_LIBRARY LZMA_INCLUDE_DIR)

IF(LZMA_FOUND)
  SET( LZMA_LIBRARIES ${LZMA_LIBRARY} )
ELSE(LZMA_FOUND)
  SET( LZMA_LIBRARIES )
ENDIF(LZMA_FOUND)

MARK_AS_ADVANCED(LZMA_LIBRARY LZMA_INCLUDE_DIR)
