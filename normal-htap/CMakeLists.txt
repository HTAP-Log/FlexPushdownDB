cmake_policy(SET CMP0048 NEW)

project("normal-htap" VERSION "${PROJECT_VERSION}" LANGUAGES C CXX)

# need to add and link libraries on mac
LINK_DIRECTORIES(/usr/local/opt/llvm@11/lib)
LINK_DIRECTORIES(/usr/local/opt/binutils/lib)
INCLUDE_DIRECTORIES(/usr/local/include)

#-----------------------------------------------------------------------------------------------------------------------
# Target
#-----------------------------------------------------------------------------------------------------------------------

add_library(normal-htap STATIC
        src/antijoin/HashAntiJoinProbe.cpp include/antijoin/HashAntiJoinProbe.h
        src/antijoin/HashAntiJoinProbeKernel.cpp include/antijoin/HashAntiJoinProbeKernel.h
        src/antijoin/RecordBatchAntiJoiner.cpp include/antijoin/RecordBatchAntiJoiner.h
        src/antijoin/AntiJoinPredicate.cpp include/antijoin/AntiJoinPredicate.h
        src/deltamerge/DeltaMerge.cpp include/deltamerge/DeltaMerge.h)

target_include_directories(normal-htap PUBLIC include)

target_link_libraries(normal-pushdown PUBLIC pthread)
target_link_libraries(normal-pushdown PRIVATE spdlog::spdlog)
target_link_libraries(normal-pushdown PRIVATE primesieve::libprimesieve-static)
target_link_libraries(normal-pushdown PRIVATE fmt::fmt)
target_link_libraries(normal-pushdown PUBLIC aws-cpp-sdk-core)
target_link_libraries(normal-pushdown PUBLIC aws-cpp-sdk-s3)
target_link_libraries(normal-pushdown PUBLIC arrow_static)
target_link_libraries(normal-pushdown PUBLIC gandiva_static)
target_link_libraries(normal-pushdown PUBLIC normal-core)
target_link_libraries(normal-pushdown PUBLIC normal-tuple)
target_link_libraries(normal-pushdown PUBLIC normal-expression)
target_link_libraries(normal-pushdown PUBLIC normal-expression-gandiva)