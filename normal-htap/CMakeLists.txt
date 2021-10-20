cmake_policy(SET CMP0048 NEW)

project("normal-htap" VERSION "${PROJECT_VERSION}" LANGUAGES C CXX)

# need to link libraries on mac
LINK_DIRECTORIES(/usr/local/opt/llvm@11/lib)
LINK_DIRECTORIES(/usr/local/opt/binutils/lib)

# Construct java home
set(JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64)

find_package(Java REQUIRED)
include(UseJava)
find_package(JNI REQUIRED)

if (JNI_FOUND)
    message (STATUS "JNI_INCLUDE_DIRS=${JNI_INCLUDE_DIRS}")
    message (STATUS "JNI_LIBRARIES=${JNI_LIBRARIES}")
endif()

include_directories(${JNI_INCLUDE_DIRS}/include ${JNI_INCLUDE_DIRS}/include/linux)

#-----------------------------------------------------------------------------------------------------------------------
# Target
#-----------------------------------------------------------------------------------------------------------------------

add_library(normal-htap STATIC
        # delta merge
        include/deltamerge/DeltaMerge.h src/deltamerge/DeltaMerge.cpp
        # get tails delta
        include/deltamanager/GetTailDeltas.h src/deltamanager/GetTailDeltas.cpp
        )

target_include_directories(normal-htap PUBLIC include)

target_link_libraries(normal-htap PUBLIC normal-deltapump)
target_link_libraries(normal-htap PUBLIC aws-cpp-sdk-core)
target_link_libraries(normal-htap PUBLIC aws-cpp-sdk-s3)
target_link_libraries(normal-htap PUBLIC normal-pushdown)
target_link_libraries(normal-htap PUBLIC /usr/local/lib/libavrocpp.so)
target_link_libraries(normal-htap PUBLIC normal-plan)
target_link_libraries(normal-htap PUBLIC normal-ssb)
target_link_libraries(normal-htap PUBLIC normal-core)
