set(Main_SRCS
    main.cpp
)

set(main-app data-migrator-exe)

add_executable(${main-app} ${Main_SRCS})

# Specify here the libraries this program depends on
target_link_libraries(data-migrator-exe libdataformat)

install(TARGETS ${main-app} DESTINATION bin)
