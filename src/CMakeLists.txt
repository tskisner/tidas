
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

add_subdirectory(libtidas)

add_subdirectory(python)

add_subdirectory(tests)

if (MPI_FOUND)
    add_subdirectory(libtidas-mpi)
    add_subdirectory(tests-mpi)
endif (MPI_FOUND)
