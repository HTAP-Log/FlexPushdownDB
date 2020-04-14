# Arrow
set(ARROW_VERSION "apache-arrow-0.16.0")
set(ARROW_GIT_URL "https://github.com/apache/arrow.git")


include(ExternalProject)
find_package(Git REQUIRED)


set(ARROW_BASE arrow_ep)
set(ARROW_PREFIX ${DEPS_PREFIX}/${ARROW_BASE})
set(ARROW_BASE_DIR ${CMAKE_CURRENT_BINARY_DIR}/${ARROW_PREFIX})
set(ARROW_INSTALL_DIR ${ARROW_BASE_DIR}/install)
set(ARROW_INCLUDE_DIR ${ARROW_INSTALL_DIR}/include)
set(ARROW_LIB_DIR ${ARROW_INSTALL_DIR}/lib)
set(ARROW_CORE_SHARED_LIBS ${ARROW_LIB_DIR}/${CMAKE_SHARED_LIBRARY_PREFIX}arrow${CMAKE_SHARED_LIBRARY_SUFFIX})
set(ARROW_CORE_STATIC_LIBS ${ARROW_LIB_DIR}/${CMAKE_STATIC_LIBRARY_PREFIX}arrow${CMAKE_STATIC_LIBRARY_SUFFIX})
set(ARROW_RE2_BASE_DIR ${ARROW_BASE_DIR}/src/${ARROW_BASE}-build/re2_ep-install)
set(ARROW_RE2_STATIC_LIB ${ARROW_RE2_BASE_DIR}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}re2${CMAKE_STATIC_LIBRARY_SUFFIX})
set(ARROW_GANDIVA_SHARED_LIB ${ARROW_LIB_DIR}/${CMAKE_SHARED_LIBRARY_PREFIX}gandiva${CMAKE_SHARED_LIBRARY_SUFFIX})
set(ARROW_GANDIVA_STATIC_LIB ${ARROW_LIB_DIR}/${CMAKE_STATIC_LIBRARY_PREFIX}gandiva${CMAKE_STATIC_LIBRARY_SUFFIX})

ExternalProject_Add(${ARROW_BASE}
        PREFIX ${ARROW_PREFIX}
        GIT_REPOSITORY ${ARROW_GIT_URL}
        GIT_TAG ${ARROW_VERSION}
        GIT_PROGRESS ON
        GIT_SHALLOW ON
        SOURCE_SUBDIR cpp
        INSTALL_DIR ${ARROW_INSTALL_DIR}
        BUILD_BYPRODUCTS
        ${ARROW_CORE_SHARED_LIBS}
        ${ARROW_CORE_STATIC_LIBS}
        ${ARROW_DATASET_SHARED_LIBS}
        ${ARROW_DATASET_STATIC_LIBS}
        ${ARROW_JEMALLOC_SHARED_LIBS}
        ${ARROW_JEMALLOC_STATIC_LIBS}
        CMAKE_ARGS
        -DARROW_USE_CCACHE:BOOL=ON
        -DARROW_CSV:BOOL=ON
        -DARROW_DATASET:BOOL=OFF
        -DARROW_FLIGHT:BOOL=OFF
        -DARROW_IPC:BOOL=OFF
        -DARROW_PARQUET:BOOL=OFF
        -DARROW_WITH_SNAPPY:BOOL=OFF
        -DARROW_JEMALLOC:BOOL=OFF
        -DARROW_GANDIVA:BOOL=ON
        -DCMAKE_INSTALL_MESSAGE=NEVER
        -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}
        -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}
        -DCMAKE_BUILD_TYPE:STRING=${CMAKE_BUILD_TYPE}
        -DCMAKE_INSTALL_PREFIX=${ARROW_INSTALL_DIR}
        )


file(MAKE_DIRECTORY ${ARROW_INCLUDE_DIR}) # Include directory needs to exist to run configure step


add_library(re2_static STATIC IMPORTED)
set_target_properties(re2_static PROPERTIES IMPORTED_LOCATION ${ARROW_CORE_STATIC_LIBS})
target_include_directories(re2_static INTERFACE ${ARROW_INCLUDE_DIR})
add_dependencies(re2_static ${ARROW_BASE})

add_library(arrow_static STATIC IMPORTED)
set_target_properties(arrow_static PROPERTIES IMPORTED_LOCATION ${ARROW_CORE_STATIC_LIBS})
target_include_directories(arrow_static INTERFACE ${ARROW_INCLUDE_DIR})
target_link_libraries(arrow_static INTERFACE ${ARROW_RE2_STATIC_LIB})
add_dependencies(arrow_static ${ARROW_BASE})

add_library(arrow_shared STATIC IMPORTED)
set_target_properties(arrow_shared PROPERTIES IMPORTED_LOCATION ${ARROW_CORE_SHARED_LIBS})
target_include_directories(arrow_shared INTERFACE ${ARROW_INCLUDE_DIR})
target_link_libraries(arrow_shared INTERFACE ${ARROW_RE2_STATIC_LIB})
add_dependencies(arrow_shared ${ARROW_BASE})

# Gandiva needs LLVM version 7 or 7.1
find_package(LLVM)

add_library(gandiva_static STATIC IMPORTED)
set_target_properties(gandiva_static PROPERTIES IMPORTED_LOCATION ${ARROW_GANDIVA_STATIC_LIB})
target_include_directories(gandiva_static INTERFACE ${ARROW_INCLUDE_DIR})
target_link_libraries(gandiva_static INTERFACE LLVM)
add_dependencies(gandiva_static ${ARROW_BASE})

add_library(gandiva_shared SHARED IMPORTED)
set_target_properties(gandiva_shared PROPERTIES IMPORTED_LOCATION ${ARROW_GANDIVA_SHARED_LIB})
target_include_directories(gandiva_shared INTERFACE ${ARROW_INCLUDE_DIR})
target_link_libraries(gandiva_static INTERFACE LLVM)
add_dependencies(gandiva_shared ${ARROW_BASE})


#showTargetProps(arrow_static)
#showTargetProps(arrow_dataset_static)
