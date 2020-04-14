function(showTargetProps target)

    get_target_property(TARGET_INCLUDE_DIRECTORIES ${target} INCLUDE_DIRECTORIES)
    get_target_property(TARGET_INTERFACE_INCLUDE_DIRECTORIES ${target} INTERFACE_INCLUDE_DIRECTORIES)
    get_target_property(TARGET_LINK_LIBRARIES ${target} LINK_LIBRARIES)
    get_target_property(TARGET_LINK_DIRECTORIES ${target} LINK_DIRECTORIES)
    get_target_property(TARGET_INTERFACE_LINK_LIBRARIES ${target} INTERFACE_LINK_LIBRARIES)
    get_target_property(TARGET_INTERFACE_LINK_DIRECTORIES ${target} INTERFACE_LINK_DIRECTORIES)
    get_target_property(TARGET_IMPORTED_LOCATION ${target} IMPORTED_LOCATION)

    message("Target '${target}'  |  Properties:")
    message("  INCLUDE_DIRECTORIES: '${TARGET_INCLUDE_DIRECTORIES}'")
    message("  INTERFACE_INCLUDE_DIRECTORIES: '${TARGET_INTERFACE_INCLUDE_DIRECTORIES}'")
    message("  LINK_LIBRARIES: '${TARGET_LINK_LIBRARIES}'")
    message("  LINK_DIRECTORIES: '${TARGET_LINK_DIRECTORIES}'")
    message("  INTERFACE_LINK_LIBRARIES: '${TARGET_INTERFACE_LINK_LIBRARIES}'")
    message("  INTERFACE_LINK_DIRECTORIES: '${TARGET_INTERFACE_LINK_DIRECTORIES}'")
    message("  IMPORTED_LOCATION: '${TARGET_IMPORTED_LOCATION}'")
    message("")

endfunction()

function(setDefaults)

    set(CMAKE_CXX_STANDARD 17 CACHE INTERNAL "CMAKE_CXX_STANDARD")
    set(CMAKE_CXX_STANDARD_REQUIRED ON CACHE INTERNAL "CMAKE_CXX_STANDARD_REQUIRED")

    add_compile_options(-Wall -Wextra -pedantic)

    #    add_compile_options(-fsanitize=undefined -fno-omit-frame-pointer)
    #    add_compile_options(-fsanitize=address -fno-omit-frame-pointer)

    if ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU")
        add_compile_options(-fdiagnostics-color=always)
    elseif ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang")
        add_compile_options(-fcolor-diagnostics)
    endif ()

endfunction()