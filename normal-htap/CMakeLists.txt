cmake_policy(SET CMP0048 NEW)

project("normal-htap" VERSION "${PROJECT_VERSION}" LANGUAGES C CXX)

# need to link libraries on mac
LINK_DIRECTORIES(/usr/local/opt/llvm@11/lib)
LINK_DIRECTORIES(/usr/local/opt/binutils/lib)

#-----------------------------------------------------------------------------------------------------------------------
# Target
#-----------------------------------------------------------------------------------------------------------------------

add_library(normal-htap STATIC
        # delta merge
        include/deltamerge/DeltaMerge.h src/deltamerge/DeltaMerge.cpp
        # get tails delta
        include/deltamanager/GetTailDeltas.h src/deltamanager/GetTailDeltas.cpp
        include/deltamanager/DeltaCacheKey.h src/deltamanager/DeltaCacheKey.cpp
        include/deltamanager/DeltaCacheData.h src/deltamanager/DeltaCacheData.cpp
        include/deltamanager/DeltaCache.h src/deltamanager/DeltaCache.cpp
        include/deltamanager/DeltaCacheActor.h src/deltamanager/DeltaCacheActor.cpp
        include/deltamanager/LoadDeltasRequestMessage.h src/deltamanager/LoadDeltasRequestMessage.cpp
        include/deltamanager/LoadDeltasResponseMessage.h src/deltamanager/LoadDeltasResponseMessage.cpp
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
