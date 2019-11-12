
# Name of the internal static library
set(TIDAS_STATIC tidas_)

# Name of the loadable module / shared library
set(TIDAS_SHARED tidas)

# Sources

set(TIDAS_SOURCES
    src/sqlite3.c
    src/tidas_backend_getdata.cpp
    src/tidas_backend_hdf5.cpp
    src/tidas_block.cpp
    src/tidas_copy.cpp
    src/tidas.cpp
    src/tidas_dict.cpp
    src/tidas_dict_getdata.cpp
    src/tidas_dict_hdf5.cpp
    src/tidas_group.cpp
    src/tidas_group_getdata.cpp
    src/tidas_group_hdf5.cpp
    src/tidas_indexdb_mem.cpp
    src/tidas_indexdb_sql.cpp
    src/tidas_intervals.cpp
    src/tidas_intervals_getdata.cpp
    src/tidas_intervals_hdf5.cpp
    src/tidas_schema.cpp
    src/tidas_schema_getdata.cpp
    src/tidas_schema_hdf5.cpp
    src/tidas_utils.cpp
    src/tidas_volume.cpp
)

# Add the library targets

add_library(${TIDAS_STATIC} STATIC ${TIDAS_SOURCES})
add_library(${TIDAS_SHARED} SHARED ${TIDAS_SOURCES})

if (HDF5_FOUND)
    target_compile_definitions(${TIDAS_STATIC} PRIVATE HAVE_HDF5=1)
    target_compile_definitions(${TIDAS_SHARED} PRIVATE HAVE_HDF5=1)
    target_include_directories(${TIDAS_STATIC} PUBLIC ${HDF5_INCLUDE_DIRS})
    target_include_directories(${TIDAS_SHARED} PUBLIC ${HDF5_INCLUDE_DIRS})
    target_link_libraries(${TIDAS_STATIC} ${HDF5_C_LIBRARIES})
    target_link_libraries(${TIDAS_SHARED} ${HDF5_C_LIBRARIES})
endif (HDF5_FOUND)

target_include_directories(${TIDAS_STATIC} PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
    PRIVATE src)

target_include_directories(${TIDAS_SHARED} PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
    PRIVATE src)

install(DIRECTORY include/ DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})

install(TARGETS ${TIDAS_SHARED} DESTINATION ${CMAKE_INSTALL_LIBDIR})
