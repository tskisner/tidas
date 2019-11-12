
# Name of the serial internal static library
set(TIDAS_STATIC tidas_)

# Name of the serial loadable module / shared library
set(TIDAS_SHARED tidas)

# Name of the MPI internal static library
set(TIDAS_MPI_STATIC tidas-mpi_)

# Name of the MPI loadable module / shared library
set(TIDAS_MPI_SHARED tidas-mpi)

# MPI sources
set(TIDAS_MPI_SOURCES
    src/tidas_mpi.cpp
    src/tidas_mpi_volume.cpp
)

# Add the library targets

add_library(${TIDAS_MPI_STATIC} STATIC ${TIDAS_MPI_SOURCES})
add_library(${TIDAS_MPI_SHARED} SHARED ${TIDAS_MPI_SOURCES})

target_include_directories(${TIDAS_MPI_STATIC} PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
    PRIVATE src)

target_include_directories(${TIDAS_MPI_SHARED} PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
    PRIVATE src)

target_include_directories(${TIDAS_MPI_STATIC} PRIVATE
    "${CMAKE_CURRENT_SOURCE_DIR}"
    "${CMAKE_CURRENT_SOURCE_DIR}/../libtidas/src"
)

target_include_directories(${TIDAS_MPI_SHARED} PRIVATE
    "${CMAKE_CURRENT_SOURCE_DIR}"
    "${CMAKE_CURRENT_SOURCE_DIR}/../libtidas/src"
)

target_include_directories(${TIDAS_MPI_STATIC} PRIVATE "${MPI_CXX_INCLUDE_PATH}")
target_include_directories(${TIDAS_MPI_SHARED} PRIVATE "${MPI_CXX_INCLUDE_PATH}")

string(STRIP "${MPI_CXX_COMPILE_FLAGS}" mpi_comp_flags)
target_compile_options(${TIDAS_MPI_STATIC} PRIVATE "${mpi_comp_flags}")
target_compile_options(${TIDAS_MPI_SHARED} PRIVATE "${mpi_comp_flags}")

string(STRIP "${MPI_CXX_LINK_FLAGS}" mpi_link_flags)
set_target_properties(${TIDAS_MPI_STATIC} PROPERTIES LINK_FLAGS "${mpi_link_flags}")
set_target_properties(${TIDAS_MPI_SHARED} PROPERTIES LINK_FLAGS "${mpi_link_flags}")

target_link_libraries(${TIDAS_MPI_STATIC} ${TIDAS_STATIC} "${MPI_CXX_LIBRARIES}")
target_link_libraries(${TIDAS_MPI_SHARED} ${TIDAS_SHARED} "${MPI_CXX_LIBRARIES}")

install(DIRECTORY include/ DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}")

install(TARGETS ${TIDAS_MPI_SHARED} DESTINATION "${CMAKE_INSTALL_LIBDIR}")
