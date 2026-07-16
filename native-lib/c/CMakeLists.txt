cmake_minimum_required(VERSION 3.10)
project(dataweave-c VERSION 0.1.0 LANGUAGES C)

# Options
option(BUILD_SHARED_LIBS "Build shared library" ON)
option(BUILD_TESTS "Build test suite" ON)

# Compiler settings
set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)

if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release)
endif()

# Platform-specific settings
if(UNIX AND NOT APPLE)
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fPIC")
endif()

# Find dependencies
find_package(Threads REQUIRED)

# Library source files
set(LIBRARY_SOURCES
    src/dataweave.c
)

set(LIBRARY_HEADERS
    include/dataweave.h
)

# Create library target
if(BUILD_SHARED_LIBS)
    add_library(dataweave SHARED ${LIBRARY_SOURCES})
    # On Windows, automatically export all symbols to create import library
    if(WIN32)
        set_target_properties(dataweave PROPERTIES
            WINDOWS_EXPORT_ALL_SYMBOLS ON
        )
    endif()
else()
    add_library(dataweave STATIC ${LIBRARY_SOURCES})
endif()

target_include_directories(dataweave
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/src
)

target_link_libraries(dataweave
    PRIVATE
        Threads::Threads
        ${CMAKE_DL_LIBS}
)

# Compiler warnings
target_compile_options(dataweave PRIVATE
    $<$<C_COMPILER_ID:GNU,Clang,AppleClang>:-Wall -Wextra -Wpedantic>
    $<$<C_COMPILER_ID:MSVC>:/W4>
)

# Version
set_target_properties(dataweave PROPERTIES
    VERSION ${PROJECT_VERSION}
    SOVERSION ${PROJECT_VERSION_MAJOR}
    PUBLIC_HEADER "${LIBRARY_HEADERS}"
)

# For Windows multi-config generators (Visual Studio), place outputs in build root
# instead of Debug/Release subdirectories to match test expectations
if(CMAKE_CONFIGURATION_TYPES)
    foreach(CONFIG_TYPE ${CMAKE_CONFIGURATION_TYPES})
        string(TOUPPER ${CONFIG_TYPE} CONFIG_TYPE_UPPER)
        set_target_properties(dataweave PROPERTIES
            RUNTIME_OUTPUT_DIRECTORY_${CONFIG_TYPE_UPPER} ${CMAKE_BINARY_DIR}
            LIBRARY_OUTPUT_DIRECTORY_${CONFIG_TYPE_UPPER} ${CMAKE_BINARY_DIR}
            ARCHIVE_OUTPUT_DIRECTORY_${CONFIG_TYPE_UPPER} ${CMAKE_BINARY_DIR}
        )
    endforeach()
endif()

# Tests
if(BUILD_TESTS)
    enable_testing()

    add_executable(test_dataweave
        tests/test_dataweave.c
    )

    target_link_libraries(test_dataweave
        PRIVATE
            dataweave
            Threads::Threads
    )

    # For Windows multi-config generators, place test executable in build root
    if(CMAKE_CONFIGURATION_TYPES)
        foreach(CONFIG_TYPE ${CMAKE_CONFIGURATION_TYPES})
            string(TOUPPER ${CONFIG_TYPE} CONFIG_TYPE_UPPER)
            set_target_properties(test_dataweave PROPERTIES
                RUNTIME_OUTPUT_DIRECTORY_${CONFIG_TYPE_UPPER} ${CMAKE_BINARY_DIR}
            )
        endforeach()
    endif()

    # Copy test data
    configure_file(
        ${CMAKE_CURRENT_SOURCE_DIR}/tests/person.xml
        ${CMAKE_CURRENT_BINARY_DIR}/tests/person.xml
        COPYONLY
    )

    add_test(
        NAME dataweave_tests
        COMMAND $<TARGET_FILE:test_dataweave>
        WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/tests
    )

    # Set environment for tests
    # Use DWLIB_PATH if provided (from Gradle), otherwise try to find it
    if(DEFINED DWLIB_PATH)
        # Use absolute path to ensure it resolves from any working directory
        get_filename_component(DWLIB_LOCATION_ABS "${DWLIB_PATH}/dwlib${CMAKE_SHARED_LIBRARY_SUFFIX}" ABSOLUTE)
        set(DWLIB_LOCATION "${DWLIB_LOCATION_ABS}")
    else()
        # Default path relative to native-lib/c (make it absolute)
        get_filename_component(DWLIB_LOCATION_ABS "${CMAKE_SOURCE_DIR}/../build/native/nativeCompile/dwlib${CMAKE_SHARED_LIBRARY_SUFFIX}" ABSOLUTE)
        set(DWLIB_LOCATION "${DWLIB_LOCATION_ABS}")
    endif()

    set_tests_properties(dataweave_tests PROPERTIES
        ENVIRONMENT "DATAWEAVE_NATIVE_LIB=${DWLIB_LOCATION}"
    )
endif()

# Installation
include(GNUInstallDirs)

install(TARGETS dataweave
    EXPORT dataweave-targets
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
    PUBLIC_HEADER DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
)

install(EXPORT dataweave-targets
    FILE dataweave-targets.cmake
    NAMESPACE dataweave::
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/dataweave
)

# Generate config file
include(CMakePackageConfigHelpers)

configure_package_config_file(
    ${CMAKE_CURRENT_SOURCE_DIR}/cmake/dataweave-config.cmake.in
    ${CMAKE_CURRENT_BINARY_DIR}/dataweave-config.cmake
    INSTALL_DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/dataweave
)

write_basic_package_version_file(
    ${CMAKE_CURRENT_BINARY_DIR}/dataweave-config-version.cmake
    VERSION ${PROJECT_VERSION}
    COMPATIBILITY SameMajorVersion
)

install(FILES
    ${CMAKE_CURRENT_BINARY_DIR}/dataweave-config.cmake
    ${CMAKE_CURRENT_BINARY_DIR}/dataweave-config-version.cmake
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/dataweave
)

# Package
set(CPACK_PACKAGE_NAME "dataweave-c")
set(CPACK_PACKAGE_VERSION ${PROJECT_VERSION})
set(CPACK_PACKAGE_DESCRIPTION_SUMMARY "C wrapper for DataWeave native library")
set(CPACK_PACKAGE_VENDOR "MuleSoft")
set(CPACK_RESOURCE_FILE_README "${CMAKE_CURRENT_SOURCE_DIR}/README.md")

include(CPack)

# Print configuration summary
message(STATUS "DataWeave C Library Configuration:")
message(STATUS "  Version:         ${PROJECT_VERSION}")
message(STATUS "  Build type:      ${CMAKE_BUILD_TYPE}")
message(STATUS "  Shared library:  ${BUILD_SHARED_LIBS}")
message(STATUS "  Build tests:     ${BUILD_TESTS}")
message(STATUS "  Install prefix:  ${CMAKE_INSTALL_PREFIX}")
