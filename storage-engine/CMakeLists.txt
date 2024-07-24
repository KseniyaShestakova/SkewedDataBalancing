cmake_minimum_required(VERSION 3.17)

set(CMAKE_CXX_STANDARD 20)

project(storage_engine)

include(FetchContent)
FetchContent_Declare(
        googletest
        URL https://github.com/google/googletest/archive/03597a01ee50ed33e9dfd640b249b4be3799d395.zip
)
# For Windows: Prevent overriding the parent project's compiler/linker settings
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(googletest)

include_directories(include)

enable_testing()

add_subdirectory(abseil-cpp)

add_executable(${PROJECT_NAME}
        main.cpp
        src/storage_engine.cpp
        src/data_generator.cpp
)

add_executable(
        tests
        src/storage_engine.cpp
        tests/test.cpp
)

target_link_libraries(
        tests
        GTest::gtest_main
        absl::status
        absl::statusor
)

target_link_libraries(${PROJECT_NAME} absl::status absl::statusor)

include(GoogleTest)
gtest_discover_tests(tests)
