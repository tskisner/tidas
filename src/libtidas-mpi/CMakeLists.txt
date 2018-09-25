
# Add the library target

add_library(tidas-mpi
    src/tidas_mpi.cpp
    src/tidas_mpi_volume.cpp
)

target_include_directories(tidas-mpi PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
    PRIVATE src)

target_include_directories(tidas-mpi PRIVATE
    "${CMAKE_CURRENT_SOURCE_DIR}"
    "${CMAKE_CURRENT_SOURCE_DIR}/../libtidas/src"
)

target_include_directories(tidas-mpi PRIVATE ${MPI_CXX_INCLUDE_PATH})

string(STRIP ${MPI_CXX_COMPILE_FLAGS} mpi_comp_flags)
target_compile_options(tidas-mpi PRIVATE ${mpi_comp_flags})

string(STRIP ${MPI_CXX_LINK_FLAGS} mpi_link_flags)
set_target_properties(tidas-mpi PROPERTIES LINK_FLAGS ${mpi_link_flags})

target_link_libraries(tidas-mpi tidas ${MPI_CXX_LIBRARIES})

install(DIRECTORY include/ DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}")

# Hardcode this to "lib" for now, since lib32/lib64 is a pain
# to use on many systems.
#install(TARGETS tidas-mpi DESTINATION "${CMAKE_INSTALL_LIBDIR}")
install(TARGETS tidas-mpi DESTINATION lib)
