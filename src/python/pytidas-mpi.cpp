/*
  TImestream DAta Storage (TIDAS)
  Copyright (c) 2014-2018, all rights reserved.  Use of this source code
  is governed by a BSD-style license that can be found in the top-level
  LICENSE file.
*/

#include <tidas_mpi_internal.hpp>

#include <mpi4py/mpi4py.h>

#include <pybind11/pybind11.h>
#include <pybind11/operators.h>
#include <pybind11/stl.h>
#include <pybind11/numpy.h>

namespace py = pybind11;


MPI_Comm tidas_mpi_extract_comm (py::object & pycomm) {
    MPI_Comm * comm = PyMPIComm_Get(pycomm.ptr());
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

        py::array_t < size_t > offset;
        offset.resize( {n} );
        py::buffer_info offset_info = offset.request();
        size_t * raw_offset = static_cast < size_t * > (offset_info.ptr);

        py::array_t < size_t > nlocal;
        nlocal.resize( {n} );
        py::buffer_info nlocal_info = nlocal.request();
        size_t * raw_nlocal = static_cast < size_t * > (nlocal_info.ptr);

        tidas::mpi_dist_uniform (comm, n, raw_offset, raw_nlocal);

        return py::make_tuple(offset, nlocal);
    });


    py::class_ < tidas::mpi_volume > (m, "MPIVolume")
        .def(py::init ([](py::object & pycomm, std::string const & path,
            tidas::access_mode mode) {
            MPI_Comm comm = tidas_mpi_extract_comm(pycomm);
            return new tidas::mpi_volume (comm, path, mode);
        }))
        .def(py::init ([](py::object & pycomm, std::string const & path,
            tidas::backend_type typ, tidas::compression_type comp,
            std::map < std::string, std::string > extra) {
            MPI_Comm comm = tidas_mpi_extract_comm(pycomm);
            return new tidas::mpi_volume (comm, path, typ, comp, extra);
        }))
        .def("comm", [](tidas::mpi_volume & self) {
            return PyMPIComm_New(self.comm());
        })
        .def("comm_rank", &tidas::mpi_volume::comm_rank)
        .def("comm_size", &tidas::mpi_volume::comm_size)
        .def("duplicate", &tidas::mpi_volume::duplicate)
        .def("link", &tidas::mpi_volume::link)
        .def("root", (tidas::block & (tidas::mpi_volume::*)())
            &tidas::mpi_volume::root,
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
