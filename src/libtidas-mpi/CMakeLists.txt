
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
target_compile_options(tidas-mpi PRIVATE "${MPI_CXX_COMPILE_FLAGS}")
set_target_properties(tidas-mpi PROPERTIES LINK_FLAGS "${MPI_CXX_LINK_FLAGS}")

target_link_libraries(tidas-mpi tidas "${MPI_CXX_LIBRARIES}")

install(DIRECTORY include/ DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}")

install(TARGETS tidas-mpi DESTINATION "${CMAKE_INSTALL_LIBDIR}")
