project(normal-frontend VERSION "${PROJECT_VERSION}" LANGUAGES C CXX)

# need to link libraries on mac
if(${APPLE})
    LINK_DIRECTORIES(/usr/local/opt/llvm/lib)
    LINK_DIRECTORIES(/usr/local/opt/binutils/lib)
else()
    message("NOT MAC OS X")
endif()

#-------------------------------------------------------------------------------------------------------------
# Target
#-----------------------------------------------------------------------------------------------------------------------

add_executable(normal-frontend src/Main.cpp
        include/normal/frontend/Client.h src/Client.cpp
        include/normal/frontend/Global.h
        include/normal/frontend/Serialization.h)

target_include_directories(normal-frontend PUBLIC include)

target_link_libraries(normal-frontend PUBLIC normal-pushdown)
target_link_libraries(normal-frontend PUBLIC normal-sql)
target_link_libraries(normal-frontend PUBLIC normal-util)
target_link_libraries(normal-frontend PRIVATE spdlog::spdlog)

add_executable(variant_serialization src/Variant_serialization.cpp)
target_link_libraries(variant_serialization PUBLIC caf::libcaf_core_shared)
target_link_libraries(variant_serialization PUBLIC caf::libcaf_io_shared)

#-----------------------------------------------------------------------------------------------------------------------
# change lib position for mac (not sure why mac cannot find the correct location of graphviz's lib)
#-----------------------------------------------------------------------------------------------------------------------
message(${CMAKE_CURRENT_BINARY_DIR}/../_deps/aws-cpp-sdk-ep/install/lib/libaws-cpp-sdk-core.dylib)
set(executables normal-frontend)
if(${APPLE})
    FOREACH(executable ${executables})
        add_custom_command(TARGET ${executable} POST_BUILD COMMAND
                ${CMAKE_INSTALL_NAME_TOOL} -change libgvplugin_neato_layout.6.dylib /usr/local/opt/graphviz/lib/graphviz/libgvplugin_neato_layout.6.dylib ${executable})
        add_custom_command(TARGET ${executable} POST_BUILD COMMAND
                ${CMAKE_INSTALL_NAME_TOOL} -change libgvplugin_core.6.dylib /usr/local/opt/graphviz/lib/graphviz/libgvplugin_core.6.dylib ${executable})
        # FIXME: not good to use absolute path
        add_custom_command(TARGET ${executable} POST_BUILD COMMAND
                ${CMAKE_INSTALL_NAME_TOOL} -change @rpath/libaws-cpp-sdk-core.dylib /Users/yyf/Desktop/pushdownDB/normal/cmake-build-relwithdebinfo/_deps/aws-cpp-sdk_ep/install/lib/libaws-cpp-sdk-core.dylib ${executable})
    ENDFOREACH()
endif()

#-----------------------------------------------------------------------------------------------------------------------
# Copy data to runtime directory
#-----------------------------------------------------------------------------------------------------------------------

configure_file(${CMAKE_CURRENT_LIST_DIR}/conf/cluster_ips ${CMAKE_CURRENT_BINARY_DIR}/conf/cluster_ips COPYONLY)
