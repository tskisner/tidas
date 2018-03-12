
# Add the library target

add_library(tidas
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

if (HDF5_FOUND)
    target_compile_definitions(tidas PRIVATE HAVE_HDF5=1)
    target_include_directories(tidas PUBLIC ${HDF5_INCLUDE_DIRS})
endif (HDF5_FOUND)

target_include_directories(tidas PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
    PRIVATE src)

target_link_libraries(tidas ${HDF5_C_LIBRARIES})

install(DIRECTORY include/ DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})

# Hardcode this to "lib" for now, since lib32/lib64 is a pain
# to use on many systems.
#install(TARGETS tidas DESTINATION ${CMAKE_INSTALL_LIBDIR})
install(TARGETS tidas DESTINATION lib)

