# set the data-formatter library
set(CMAKE_CXX_FLAGS " -ggdb -Wall -Wextra -Woverloaded-virtual -Wconversion -Wpointer-arith -Wcast-qual -Wcast-align -Wwrite-strings -Winit-self -Wno-sign-compare -Wno-unused-parameter -D__STDC_CONSTANT_MACROS -D__STDC_LIMIT_MACROS -DNOCLOCK -fno-omit-frame-pointer -fvisibility=hidden -DBOOST_SP_DISABLE_THREADS -Wno-ignored-qualifiers -fno-strict-aliasing -Wno-return-type -O2")

set(target_library libdataformat)

set(project_path ".")
#Bring the headers, such as Student.h into the project
#include_directories(include/attribute)
file(GLOB_RECURSE HEADERS "${project_path}/*.h")
MESSAGE( STATUS "project_path:         " ${project_path} )
MESSAGE( STATUS "HEADERS:         " ${HEADERS} )
include_directories(${project_path} ${project_path}/attribute ${project_path}/format ${project_path}/formatter ${project_path}/postgres ${project_path}/common ${project_path}/csv ${project_path}/vertica)
file(GLOB_RECURSE SOURCES "${project_path}/*.cpp")
MESSAGE(STATUS "SOURCES: " ${SOURCES} )

# Declare the library
add_library(${target_library} STATIC ${SOURCES})

# Specify here the include directories exported
# by this library
target_include_directories(${target_library} PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}
)

# add boost library
set(Boost_USE_STATIC_LIBS OFF) 
set(Boost_USE_MULTITHREADED ON)  
set(Boost_USE_STATIC_RUNTIME OFF)
#find_package(Boost 1.60.0 COMPONENTS log thread system log_setup)
find_package(Boost COMPONENTS log thread system log_setup)

if(Boost_FOUND)
    include_directories(${Boost_INCLUDE_DIRS})
    add_definitions(-DBOOST_LOG_DYN_LINK)
    #target_link_libraries(data-migrator ${Boost_LIBRARIES})
    target_link_libraries(${target_library} ${Boost_LIBRARIES})
endif()

# add pthread
find_package(Threads REQUIRED)
if(THREADS_HAVE_PTHREAD_ARG)
  #target_compile_options(PUBLIC data-migrator "-pthread")
  target_compile_options(PUBLIC ${target_library} "-pthread")
endif()

#if(CMAKE_THREAD_LIBS_INIT)
  #target_link_libraries(data-migrator "${CMAKE_THREAD_LIBS_INIT}")
#endif()
