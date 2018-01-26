
# pybind11 - this also finds the python interpreter

add_subdirectory(pybind11)

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

# Create a module for the serial tidas library

pybind11_add_module(_pytidas MODULE pytidas.cpp)
target_link_libraries(_pytidas PRIVATE tidas)

# This is for the internal libtidas headers
target_include_directories(_pytidas PRIVATE
    "${CMAKE_CURRENT_SOURCE_DIR}/../libtidas/src"
)

install(TARGETS _pytidas DESTINATION ${PYTHON_SITE}/tidas)

# Install all the other python files

install(FILES
    __init__.py
    test.py
    DESTINATION ${PYTHON_SITE}/tidas
)

# Optionally create a module for the MPI library

if(MPI_FOUND AND MPI4PY_FOUND)
    pybind11_add_module(_pytidas_mpi MODULE pytidas-mpi.cpp)
    target_compile_options(_pytidas_mpi PRIVATE "${MPI_CXX_COMPILE_FLAGS}")
    target_link_libraries(_pytidas_mpi PRIVATE tidas-mpi "${MPI_CXX_LIBRARIES}")
    target_include_directories(_pytidas_mpi PRIVATE
        "${CMAKE_CURRENT_SOURCE_DIR}/../libtidas/src"
        "${CMAKE_CURRENT_SOURCE_DIR}/../libtidas-mpi/src"
        ${MPI_CXX_INCLUDE_PATH}
        ${MPI4PY_INCLUDE_DIR}
    )
    set_target_properties(_pytidas_mpi
        PROPERTIES LINK_FLAGS "${MPI_CXX_LINK_FLAGS}"
    )
    install(TARGETS _pytidas_mpi DESTINATION ${PYTHON_SITE}/tidas)

    install(FILES
        mpi.py
        DESTINATION ${PYTHON_SITE}/tidas
    )
endif(MPI_FOUND AND MPI4PY_FOUND)
