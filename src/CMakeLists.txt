
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

add_subdirectory(libtidas)

add_subdirectory(python)

add_subdirectory(tests)

#if (MPI_FOUND)
#    add_subdirectory(libtidas-mpi)
#    add_subdirectory(apps-mpi)
#endif (MPI_FOUND)
