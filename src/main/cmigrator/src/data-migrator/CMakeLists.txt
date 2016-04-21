cmake_minimum_required(VERSION 2.8.9)
project(data-migrator)

set(CMAKE_BUILD_TYPE Release)

#SET(CMAKE_CXX_COMPILER "g++")
# another possible parameter: -std=c++11 / or -std=c++14
set(CMAKE_CXX_FLAGS " -Wall -Wextra -Woverloaded-virtual -Wconversion -Wpointer-arith -Wcast-qual -Wcast-align -Wwrite-strings -Winit-self -Wno-sign-compare -Wno-unused-parameter -D__STDC_CONSTANT_MACROS -D__STDC_LIMIT_MACROS -DNOCLOCK -fno-omit-frame-pointer -fvisibility=hidden -DBOOST_SP_DISABLE_THREADS -Wno-ignored-qualifiers -fno-strict-aliasing -Wno-return-type")

#Bring the headers, such as Student.h into the project
#include_directories(include/attribute)
file(GLOB HEADERS "include/data-migrator/*.h")
MESSAGE( STATUS "HEADERS:         " ${HEADERS} )
#include_directories(include)
include_directories(include/data-migrator)
include_directories(include/data-migrator/common)
 
#Can manually add the sources using the set command as follows:
#set(SOURCES src/mainapp.cpp src/Student.cpp)
 
#However, the file(GLOB...) allows for wildcard additions:
#file(GLOB SOURCES "src/attribute/*.cpp")
file(GLOB SOURCES "src/data-migrator/*.cpp")

# create the main executable 
add_executable(data-migrator-exe ${SOURCES})

# this is an example how to add executable that is merely a single file
#add_executable(data-migrator-exe src/data-migrator.cpp)

# creat the shared library for the data migrator
#Generate the shared library from the sources
add_library(data-migrator SHARED ${SOURCES})
#add_library(attribute SHARED src/attribute.cpp include/attribute.h)
 
#Set the location for library installation -- i.e., /usr/lib in this case
# not really necessary in this example. Use "sudo make install" to apply
#install(TARGETS data-migrator DESTINATION /usr/lib)
#install(TARGETS attribute DESTINATION /usr/lib)

#will install data-migrator to <prefix>/bin and myStaticLib to <prefix>/lib/static. On non-DLL platforms mySharedLib will be installed to <prefix>/lib and /some/full/path. On DLL platforms the mySharedLib DLL will be installed to <prefix>/bin and /some/full/path and its import library will be installed to <prefix>/lib/static and /some/full/path.
# <prefix> make DESTDIR=/home/john install; This variable defaults to /usr/local on UNIX and c:/Program Files on Windows.
install(TARGETS data-migrator
        RUNTIME DESTINATION bin
        LIBRARY DESTINATION lib
        ARCHIVE DESTINATION lib/static
	PUBLIC_HEADER DESTINATION include/data-migrator)

INSTALL(FILES ${HEADERS}
        DESTINATION include/data-migrator)

# install(TARGETS data-migrator
#         RUNTIME DESTINATION /usr/bin
#         LIBRARY DESTINATION /usr/lib
#         ARCHIVE DESTINATION /usr/lib/static
# 	PUBLIC_HEADER DESTINATION /usr/include)

# INSTALL(FILES ${HEADERS}
#         DESTINATION /usr/include)
 
# add boost library
set(Boost_USE_STATIC_LIBS OFF) 
set(Boost_USE_MULTITHREADED ON)  
set(Boost_USE_STATIC_RUNTIME OFF) 
find_package(Boost 1.60.0 COMPONENTS log thread system log_setup) 

if(Boost_FOUND)
    include_directories(${Boost_INCLUDE_DIRS})
    add_definitions(-DBOOST_LOG_DYN_LINK)
    target_link_libraries(data-migrator ${Boost_LIBRARIES})
    target_link_libraries(data-migrator-exe ${Boost_LIBRARIES})
endif()

# add pthread
find_package(Threads REQUIRED)
if(THREADS_HAVE_PTHREAD_ARG)
  target_compile_options(PUBLIC data-migrator "-pthread")
  target_compile_options(PUBLIC data-migrator-exe "-pthread")
endif()
if(CMAKE_THREAD_LIBS_INIT)
  target_link_libraries(data-migrator "${CMAKE_THREAD_LIBS_INIT}")
endif()

# tests
option(build_tests "Build all of gtest's own tests." OFF)

