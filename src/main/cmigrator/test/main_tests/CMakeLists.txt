file(GLOB_RECURSE SRCS *.cc *.cpp *.h)

set(main_test migratorTest)
ADD_EXECUTABLE(${main_test} ${SRCS})

TARGET_LINK_LIBRARIES(${main_test}
    libdataformat
    libgtest
    libgmock
)

add_test(NAME ${main_test} COMMAND ${main_test})

