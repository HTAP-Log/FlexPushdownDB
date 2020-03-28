# CAF

set(CAF_VERSION "7ff369655076e9baf7acd9021e2e27966f9cffb4")  # 0.17.4
set(CAF_GIT_URL "https://github.com/actor-framework/actor-framework.git")


include(ExternalProject)
find_package(Git REQUIRED)

set(CAF_BASE caf_ep)
set(CAF_PREFIX ${DEPS_PREFIX}/${CAF_BASE})
set(CAF_BASE_DIR ${CMAKE_CURRENT_BINARY_DIR}/${CAF_PREFIX})
set(CAF_INSTALL_DIR ${CAF_BASE_DIR}/install)
set(CAF_INCLUDE_DIR ${CAF_INSTALL_DIR}/include)
set(CAF_LIB_DIR ${CAF_INSTALL_DIR}/lib)
set(CAF_CORE_SHARED_LIBS ${CAF_LIB_DIR}/${CMAKE_SHARED_LIBRARY_PREFIX}caf_core${CMAKE_SHARED_LIBRARY_SUFFIX})
set(CAF_CORE_STATIC_LIBS ${CAF_LIB_DIR}/${CMAKE_STATIC_LIBRARY_PREFIX}caf_core${CMAKE_STATIC_LIBRARY_SUFFIX})
set(CAF_IO_SHARED_LIBS ${CAF_LIB_DIR}/${CMAKE_SHARED_LIBRARY_PREFIX}caf_io${CMAKE_SHARED_LIBRARY_SUFFIX})
set(CAF_IO_STATIC_LIBS ${CAF_LIB_DIR}/${CMAKE_STATIC_LIBRARY_PREFIX}caf_io${CMAKE_STATIC_LIBRARY_SUFFIX})
set(CAF_OPENSSL_SHARED_LIBS ${CAF_LIB_DIR}/${CMAKE_SHARED_LIBRARY_PREFIX}caf_openssl${CMAKE_SHARED_LIBRARY_SUFFIX})
set(CAF_OPENSSL_STATIC_LIBS ${CAF_LIB_DIR}/${CMAKE_STATIC_LIBRARY_PREFIX}caf_openssl${CMAKE_STATIC_LIBRARY_SUFFIX})


ExternalProject_Add(${CAF_BASE}
        PREFIX ${CAF_BASE_DIR}
        INSTALL_DIR ${CAF_INSTALL_DIR}
        GIT_REPOSITORY ${CAF_GIT_URL}
        GIT_TAG ${CAF_VERSION}
        BUILD_BYPRODUCTS ${CAF_CORE_SHARED_LIBS} ${CAF_CORE_STATIC_LIBS} ${CAF_IO_SHARED_LIBS} ${CAF_IO_STATIC_LIBS} ${CAF_OPENSSL_SHARED_LIBS} ${CAF_OPENSSL_STATIC_LIBS}
        CMAKE_ARGS
        -DCAF_BUILD_STATIC:BOOL=ON
        -DCAF_NO_EXAMPLES:BOOL=ON
        -DCAF_NO_UNIT_TESTS:BOOL=ON
        -DCAF_NO_PYTHON:BOOL=ON
        -DCAF_NO_OPENCL:BOOL=ON
        -DCAF_NO_TOOLS:BOOL=ON
        -DCAF_NO_AUTO_LIBCPP:BOOL=ON
        -DCMAKE_C_COMPILER:STRING=${CMAKE_C_COMPILER}
        -DCMAKE_CXX_COMPILER:STRING=${CMAKE_CXX_COMPILER}
        -DCMAKE_BUILD_TYPE:STRING=${CMAKE_BUILD_TYPE}
        -DCMAKE_INSTALL_PREFIX:STRING=${CAF_INSTALL_DIR}
        )


file(MAKE_DIRECTORY ${CAF_INCLUDE_DIR}) # Include directory needs to exist to run configure step


add_library(caf::libcaf_core_shared SHARED IMPORTED)
set_target_properties(caf::libcaf_core_shared PROPERTIES IMPORTED_LOCATION ${CAF_CORE_SHARED_LIBS})
set_target_properties(caf::libcaf_core_shared PROPERTIES INTERFACE_INCLUDE_DIRECTORIES ${CAF_INCLUDE_DIR})
add_dependencies(caf::libcaf_core_shared caf_ep)

add_library(caf::libcaf_core_static STATIC IMPORTED)
set_target_properties(caf::libcaf_core_static PROPERTIES IMPORTED_LOCATION ${CAF_CORE_STATIC_LIBS})
set_target_properties(caf::libcaf_core_static PROPERTIES INTERFACE_INCLUDE_DIRECTORIES ${CAF_INCLUDE_DIR})
add_dependencies(caf::libcaf_core_static caf_ep)

add_library(caf::libcaf_io_shared SHARED IMPORTED)
set_target_properties(caf::libcaf_io_shared PROPERTIES IMPORTED_LOCATION ${CAF_IO_SHARED_LIBS})
set_target_properties(caf::libcaf_io_shared PROPERTIES INTERFACE_INCLUDE_DIRECTORIES ${CAF_INCLUDE_DIR})
add_dependencies(caf::libcaf_io_shared caf_ep)

add_library(caf::libcaf_io_static STATIC IMPORTED)
set_target_properties(caf::libcaf_io_static PROPERTIES IMPORTED_LOCATION ${CAF_IO_STATIC_LIBS})
set_target_properties(caf::libcaf_io_static PROPERTIES INTERFACE_INCLUDE_DIRECTORIES ${CAF_INCLUDE_DIR})
add_dependencies(caf::libcaf_io_static caf_ep)

add_library(caf::libcaf_openssl_static STATIC IMPORTED)
set_target_properties(caf::libcaf_openssl_static PROPERTIES IMPORTED_LOCATION ${CAF_OPENSSL_STATIC_LIBS})
set_target_properties(caf::libcaf_openssl_static PROPERTIES INTERFACE_INCLUDE_DIRECTORIES ${CAF_INCLUDE_DIR})
add_dependencies(caf::libcaf_openssl_static caf_ep)

add_library(caf::libcaf_openssl_shared SHARED IMPORTED)
set_target_properties(caf::libcaf_openssl_shared PROPERTIES IMPORTED_LOCATION ${CAF_OPENSSL_SHARED_LIBS})
set_target_properties(caf::libcaf_openssl_shared PROPERTIES INTERFACE_INCLUDE_DIRECTORIES ${CAF_INCLUDE_DIR})
add_dependencies(caf::libcaf_openssl_shared caf_ep)


#showTargetProps(caf::libcaf_core_shared)
#showTargetProps(caf::libcaf_core_static)
#showTargetProps(caf::libcaf_io_shared)
#showTargetProps(caf::libcaf_io_static)
#showTargetProps(caf::libcaf_openssl_static)
#showTargetProps(caf::libcaf_openssl_shared)
