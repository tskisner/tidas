
# pybind11 - this also finds the python interpreter

add_subdirectory(pybind11)

# Create a module for the serial tidas library

pybind11_add_module(_pytidas MODULE pytidas.cpp)
target_link_libraries(_pytidas PRIVATE tidas)

# This is for the internal libtidas headers
target_include_directories(_pytidas PRIVATE
    "${CMAKE_CURRENT_SOURCE_DIR}/../libtidas/src"
)

# Optionally create a module for the MPI library

#if (MPI_FOUND)
#    pybind11_add_module(_pytidas_mpi MODULE pytidas-mpi.cpp)
#endif (MPI_FOUND)

# Determine our python install location and set up a rule to install the modules

execute_process(
    COMMAND "${PYTHON_EXECUTABLE}" -c "if True:
        import sys
        print('{}.{}'.format(sys.version_info.major, sys.version_info.minor))"
    OUTPUT_VARIABLE PYTHON_MAJORMINOR
    OUTPUT_STRIP_TRAILING_WHITESPACE
)

set(PYTHON_SITE
    "${CMAKE_INSTALL_LIBDIR}/python${PYTHON_MAJORMINOR}/site-packages")

install(TARGETS _pytidas DESTINATION ${PYTHON_SITE}/tidas)

install(FILES
    __init__.py
    test.py
    DESTINATION ${PYTHON_SITE}/tidas
)
