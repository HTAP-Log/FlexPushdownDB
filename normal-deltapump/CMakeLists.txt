cmake_policy(SET CMP0048 NEW)

project("normal-deltapump" VERSION "${PROJECT_VERSION}" LANGUAGES C CXX)

# need to link libraries on mac
LINK_DIRECTORIES(/usr/local/opt/llvm@11/lib)
LINK_DIRECTORIES(/usr/local/opt/binutils/lib)

#-----------------------------------------------------------------------------------------------------------------------
# Target
#-----------------------------------------------------------------------------------------------------------------------

# build Parser.java
#set(JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64)
#
#find_package(Java REQUIRED)
#include(UseJava)
#find_package(JNI REQUIRED)
#
#if (JNI_FOUND)
#    message (STATUS "JNI_INCLUDE_DIRS=${JNI_INCLUDE_DIRS}")
#    message (STATUS "JNI_LIBRARIES=${JNI_LIBRARIES}")
#endif()

set(CMAKE_JAVA_INCLUDE_PATH ./include/deltapump/lib/mysql-binlog-connector-java-0.25.1.jar;./include/deltapump/lib/avro-1.10.2.jar;./include/deltapump/lib/avro-tools-1.10.2.jar)

add_jar(Parser src/deltapump/Parser.java)

get_target_property(_jarFile Parser JAR_FILE)
get_target_property(_classDir Parser CLASSDIR)

#include_directories(${JNI_INCLUDE_DIRS}/include ${JNI_INCLUDE_DIRS}/include/linux)

# Create a sources variable for BinlogParser
set(SOURCES
        # test
        test/deltapumpTest.cpp
        # src
        src/deltapump/BinlogParser.cpp
        src/deltapump/makeTuple.cpp
        # header
        include/deltapump/makeTuple.h
        include/deltapump/BinlogParser.h
        # schema
        include/deltapump/lineorder_d.hh
        include/deltapump/customer_d.hh
        include/deltapump/supplier_d.hh
        include/deltapump/part_d.hh
        include/deltapump/date_d.hh
        )

#-----------------------------------------------------------------------------------------------------------------------
# Target
#-----------------------------------------------------------------------------------------------------------------------

add_library(normal-deltapump STATIC ${SOURCES})

target_link_libraries(normal-deltapump PUBLIC ${JNI_LIBRARIES} /usr/local/lib/libavrocpp.so)
target_include_directories(normal-deltapump
        PUBLIC
        ${PROJECT_SOURCE_DIR}/include/deltapump
        )

#-----------------------------------------------------------------------------------------------------------------------
# Test
#-----------------------------------------------------------------------------------------------------------------------


# BinlogParser Test
add_executable(normal-deltapump-output ${SOURCES})

target_link_libraries(normal-deltapump-output PUBLIC ${JNI_LIBRARIES} /usr/local/lib/libavrocpp.so)
target_include_directories(normal-deltapump-output
        PRIVATE
        ${PROJECT_SOURCE_DIR}/include/deltapump
        )

# copy mysql-binlog-connector-java-0.25.1.jar to cmake-build-remote-release directory
file(COPY ${CMAKE_CURRENT_SOURCE_DIR}/include/deltapump/lib
        DESTINATION ${CMAKE_CURRENT_BINARY_DIR})
file(COPY ${CMAKE_CURRENT_SOURCE_DIR}/include/deltapump/schemas
        DESTINATION ${CMAKE_CURRENT_BINARY_DIR})
file(COPY ${CMAKE_CURRENT_SOURCE_DIR}/include/deltapump/logs/bin.000002
        DESTINATION ${CMAKE_CURRENT_BINARY_DIR})
file(COPY ${CMAKE_CURRENT_SOURCE_DIR}/include/deltapump/partitions/ranges.csv
        DESTINATION ${CMAKE_CURRENT_BINARY_DIR}/partitions/)