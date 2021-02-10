#
# Find module for binutils (needed for backward)
#

include(FindPackageHandleStandardArgs)

unset(Binutils_INCLUDE_DIR CACHE)

if(${APPLE})
    find_path (Binutils_INCLUDE_DIR
            bfd.h
            PATHS /usr/local/opt/binutils/include
            NO_DEFAULT_PATH)
else()
    find_path (Binutils_INCLUDE_DIR
            bfd.h
            PATHS /usr/include /usr/local/Cellar/binutils/2.36.1/include
            NO_DEFAULT_PATH)
endif()

find_package_handle_standard_args(binutils DEFAULT_MSG Binutils_INCLUDE_DIR)
