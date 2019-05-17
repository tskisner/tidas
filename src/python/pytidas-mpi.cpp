
// TImestream DAta Storage (TIDAS).
//
// Copyright (c) 2015-2019 by the parties listed in the AUTHORS file.  All rights
// reserved.  Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include <tidas_mpi_internal.hpp>

#include <mpi4py/mpi4py.h>

#include <pybind11/pybind11.h>
#include <pybind11/operators.h>
#include <pybind11/stl.h>
#include <pybind11/numpy.h>

namespace py = pybind11;

static void reprint(PyObject * obj) {
    PyObject * repr = PyObject_Repr(obj);
    PyObject * str = PyUnicode_AsEncodedString(repr, "utf-8", "~E~");
    const char * bytes = PyBytes_AS_STRING(str);

    printf("REPR: %s\n", bytes);

    Py_XDECREF(repr);
    Py_XDECREF(str);
}

MPI_Comm tidas_mpi_extract_comm(py::object & pycomm) {
    // std::cerr << "ptr = " << pycomm.ptr() << std::endl;
    PyObject * pp = pycomm.ptr();

    // reprint(pp);

    // MPI_Comm * comm = PyMPIComm_Get(pp);
    MPI_Comm * comm = (&((struct PyMPICommObject *)pp)->ob_mpi);

    if (import_mpi4py() < 0) {
        throw std::runtime_error("could not import mpi4py\n");
    }
    if (comm == nullptr) {
        throw std::runtime_error("mpi4py Comm pointer is null\n");
    }
    MPI_Comm newcomm;
    int ret = MPI_Comm_dup((*comm), &newcomm);
    return newcomm;
}

PYBIND11_MODULE(_pytidas_mpi, m) {
    m.doc() = "MPI Interface wrapper for TIDAS.";

    m.def("mpi_dist_uniform", [](py::object & pycomm, size_t n) {
              MPI_Comm comm = tidas_mpi_extract_comm(pycomm);
              size_t offset;
              size_t nlocal;
              tidas::mpi_dist_uniform(comm, n, &offset, &nlocal);
              return py::make_tuple(offset, nlocal);
          });

    py::class_ <tidas::mpi_volume> (m, "MPIVolume")
    .def(py::init([](py::object & pycomm, std::string const & path,
                     tidas::access_mode mode) {
                      MPI_Comm comm = tidas_mpi_extract_comm(pycomm);
                      return new tidas::mpi_volume(comm, path, mode);
                  }))
    .def(py::init([](py::object & pycomm, std::string const & path,
                     tidas::backend_type typ, tidas::compression_type comp,
                     std::map <std::string, std::string> extra) {
                      MPI_Comm comm = tidas_mpi_extract_comm(pycomm);
                      return new tidas::mpi_volume(comm, path, typ, comp, extra);
                  }))
    .def("__enter__", [](tidas::mpi_volume & self) {
             return self;
         })
    .def("__exit__",  [](tidas::mpi_volume & self, py::args) {
             self.close();
         })
    .def("comm", [](tidas::mpi_volume & self) {
             return py::reinterpret_steal <py::object> (PyMPIComm_New(self.comm()));
         })
    .def("comm_rank", &tidas::mpi_volume::comm_rank)
    .def("comm_size", &tidas::mpi_volume::comm_size)
    .def("duplicate", &tidas::mpi_volume::duplicate)
    .def("meta_sync", &tidas::mpi_volume::meta_sync)
    .def("link", &tidas::mpi_volume::link)
    .def("root", (tidas::block & (tidas::mpi_volume::*)())
         & tidas::mpi_volume::root,
         py::return_value_policy::reference_internal)
    .def("info", [](tidas::mpi_volume & self) {
             self.info(std::cout);
         });


    //
    // /// Pass over the root block and all descendents, calling a
    // /// functor on each one.  The specified class should provide the
    // /// operator() method.
    // template < class P >
    // void exec ( P & op, exec_order order, std::string const & filter = "" )
    //
    //
    //
}
