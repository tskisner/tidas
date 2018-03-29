/*
  TImestream DAta Storage (TIDAS)
  Copyright (c) 2014-2018, all rights reserved.  Use of this source code
  is governed by a BSD-style license that can be found in the top-level
  LICENSE file.
*/

#include <tidas_internal.hpp>

#include <pybind11/pybind11.h>
#include <pybind11/operators.h>
#include <pybind11/stl.h>
#include <pybind11/numpy.h>


namespace py = pybind11;

using ShapeContainer = py::detail::any_container<ssize_t>;


tidas::data_type numpy_format_to_tidas(std::string const & format) {
    if (format == "d") {
        return tidas::data_type::float64;
    } else if (format == "f") {
        return tidas::data_type::float32;
    } else if (format == "l") {
        return tidas::data_type::int64;
    } else if (format == "L") {
        return tidas::data_type::uint64;
    } else if (format == "i") {
        return tidas::data_type::int32;
    } else if (format == "I") {
        return tidas::data_type::uint32;
    } else if (format == "h") {
        return tidas::data_type::int16;
    } else if (format == "H") {
        return tidas::data_type::uint16;
    } else if (format == "b") {
        return tidas::data_type::int8;
    } else if (format == "B") {
        return tidas::data_type::uint8;
    } else if (format[0] == 'S') {
        return tidas::data_type::string;
    } else {
        return tidas::data_type::none;
    }
}


std::string tidas_format_descr(tidas::data_type typ) {
    std::string format;
    if (typ == tidas::data_type::float64) {
        format = "d";
    } else if (typ == tidas::data_type::float32) {
        format = "f";
    } else if (typ == tidas::data_type::int64) {
        format = "l";
    } else if (typ == tidas::data_type::uint64) {
        format = "L";
    } else if (typ == tidas::data_type::int32) {
        format = "i";
    } else if (typ == tidas::data_type::uint32) {
        format = "I";
    } else if (typ == tidas::data_type::int16) {
        format = "h";
    } else if (typ == tidas::data_type::uint16) {
        format = "H";
    } else if (typ == tidas::data_type::int8) {
        format = "b";
    } else if (typ == tidas::data_type::uint8) {
        format = "B";
    } else if (typ == tidas::data_type::string) {
        std::ostringstream o;
        o << "S" << tidas::backend_string_size;
        format = o.str();
    }
    return format;
}


py::array tidas_numpy_array(tidas::data_type typ, ssize_t len) {
    std::string format = tidas_format_descr(typ);
    // The numpy dtype format for this array
    py::dtype dt(format);
    // pybind11 does not let us easily control the creation flags of numpy
    // arrays allocated directly.  In particular, it does not let us set the
    // flags for contiguous and aligned memory.  Instead, we first allocate a
    // zero-size array (so memory buffer is NULL).  Then we resize the array,
    // which triggers allocation within numpy that "does the right thing".
    py::array nparray(dt, {0}, {1});
    nparray.resize( {len} );
    return nparray;
}


PYBIND11_MODULE(_pytidas, m) {
    m.doc() = "Interface wrapper for TIDAS.";

    py::enum_ < tidas::data_type > (m, "DataType", py::arithmetic(),
        "Scalar raw data types")
    .value(tidas::data_type_to_string(tidas::data_type::none).c_str(),
        tidas::data_type::none)
    .value(tidas::data_type_to_string(tidas::data_type::int8).c_str(),
        tidas::data_type::int8)
    .value(tidas::data_type_to_string(tidas::data_type::uint8).c_str(),
        tidas::data_type::uint8)
    .value(tidas::data_type_to_string(tidas::data_type::int16).c_str(),
        tidas::data_type::int16)
    .value(tidas::data_type_to_string(tidas::data_type::uint16).c_str(),
        tidas::data_type::uint16)
    .value(tidas::data_type_to_string(tidas::data_type::int32).c_str(),
        tidas::data_type::int32)
    .value(tidas::data_type_to_string(tidas::data_type::uint32).c_str(),
        tidas::data_type::uint32)
    .value(tidas::data_type_to_string(tidas::data_type::int64).c_str(),
        tidas::data_type::int64)
    .value(tidas::data_type_to_string(tidas::data_type::uint64).c_str(),
        tidas::data_type::uint64)
    .value(tidas::data_type_to_string(tidas::data_type::float32).c_str(),
        tidas::data_type::float32)
    .value(tidas::data_type_to_string(tidas::data_type::float64).c_str(),
        tidas::data_type::float64)
    .value(tidas::data_type_to_string(tidas::data_type::string).c_str(),
        tidas::data_type::string);

    py::enum_ < tidas::backend_type > (m, "BackendType", py::arithmetic(),
        "Backend I/O Format")
    .value("none", tidas::backend_type::none)
    .value("hdf5", tidas::backend_type::hdf5)
    .value("getdata", tidas::backend_type::getdata);

    py::enum_ < tidas::compression_type > (m, "CompressionType",
        py::arithmetic(), "Data compression type")
    .value("none", tidas::compression_type::none)
    .value("gzip", tidas::compression_type::gzip)
    .value("bzip2", tidas::compression_type::bzip2);

    py::enum_ < tidas::access_mode > (m, "AccessMode",
        py::arithmetic(), "Volume access mode")
    .value("read", tidas::access_mode::read)
    .value("write", tidas::access_mode::write);

    py::enum_ < tidas::link_type > (m, "LinkType",
        py::arithmetic(), "Type of links used for volume aliases")
    .value("none", tidas::link_type::none)
    .value("hard", tidas::link_type::hard)
    .value("soft", tidas::link_type::soft);

    py::enum_ < tidas::exec_order > (m, "ExecOrder",
        py::arithmetic(), "Order of execution when processing a tree of blocks")
    .value("depth_first", tidas::exec_order::depth_first)
    .value("depth_last", tidas::exec_order::depth_last)
    .value("leaf", tidas::exec_order::leaf);


    py::class_ < tidas::dict > (m, "Dictionary")
        .def(py::init<>())
        .def("clear", &tidas::dict::clear)
        .def("keys", &tidas::dict::keys)
        .def("get_int8", &tidas::dict::get<int8_t>,
            py::return_value_policy::copy)
        .def("get_uint8", &tidas::dict::get<uint8_t>,
            py::return_value_policy::copy)
        .def("get_int16", &tidas::dict::get<int16_t>,
            py::return_value_policy::copy)
        .def("get_uint16", &tidas::dict::get<uint16_t>,
            py::return_value_policy::copy)
        .def("get_int32", &tidas::dict::get<int32_t>,
            py::return_value_policy::copy)
        .def("get_uint32", &tidas::dict::get<uint32_t>,
            py::return_value_policy::copy)
        .def("get_int64", &tidas::dict::get<int64_t>,
            py::return_value_policy::copy)
        .def("get_uint64", &tidas::dict::get<uint64_t>,
            py::return_value_policy::copy)
        .def("get_float32", &tidas::dict::get<float>,
            py::return_value_policy::copy)
        .def("get_float64", &tidas::dict::get<double>,
            py::return_value_policy::copy)
        .def("get_string", &tidas::dict::get<std::string>,
            py::return_value_policy::copy)
        .def("put_int8", &tidas::dict::put<int8_t>)
        .def("put_uint8", &tidas::dict::put<uint8_t>)
        .def("put_int16", &tidas::dict::put<int16_t>)
        .def("put_uint16", &tidas::dict::put<uint16_t>)
        .def("put_int32", &tidas::dict::put<int32_t>)
        .def("put_uint32", &tidas::dict::put<uint32_t>)
        .def("put_int64", &tidas::dict::put<int64_t>)
        .def("put_uint64", &tidas::dict::put<uint64_t>)
        .def("put_float32", &tidas::dict::put<float>)
        .def("put_float64", &tidas::dict::put<double>)
        .def("put_string", &tidas::dict::put<std::string>)
        .def(py::pickle(
            [](tidas::dict const & p) { // __getstate__
                /* Return a serialized string representation */
                std::ostringstream sstrm;
                {
                    cereal::PortableBinaryOutputArchive outarch ( sstrm );
                    outarch ( p );
                }
                return py::bytes(sstrm.str());
            },
            [](py::bytes t) { // __setstate__
                tidas::dict p;
                std::istringstream sstrm ( t.cast < std::string > () );
                {
                    cereal::PortableBinaryInputArchive inarch ( sstrm );
                    inarch ( p );
                }
                return p;
            }
        ))
        .def("get_type", [](tidas::dict & self, std::string const & key) {
            return tidas_format_descr(self.types().at(key));
        });


    py::class_ < tidas::intrvl > (m, "Intrvl")
        .def(py::init<>())
        .def(py::init< tidas::time_type, tidas::time_type, tidas::index_type,
            tidas::index_type >())
        .def_readwrite("start", &tidas::intrvl::start)
        .def_readwrite("stop", &tidas::intrvl::stop)
        .def_readwrite("first", &tidas::intrvl::first)
        .def_readwrite("last", &tidas::intrvl::last)
        .def(py::pickle(
            [](tidas::intrvl const & p) { // __getstate__
                /* Return a serialized string representation */
                std::ostringstream sstrm;
                {
                    cereal::PortableBinaryOutputArchive outarch ( sstrm );
                    outarch ( p );
                }
                return py::bytes(sstrm.str());
            },
            [](py::bytes t) { // __setstate__
                tidas::intrvl p;
                std::istringstream sstrm ( t.cast < std::string > () );
                {
                    cereal::PortableBinaryInputArchive inarch ( sstrm );
                    inarch ( p );
                }
                return p;
            }
        ));


    py::class_ < tidas::intervals > (m, "Intervals")
        .def(py::init<>())
        .def(py::init< tidas::dict, size_t >())
        .def("dictionary", &tidas::intervals::dictionary,
            py::return_value_policy::reference_internal)
        .def("size", &tidas::intervals::size,
            py::return_value_policy::take_ownership)
        .def("read", (std::vector <tidas::intrvl>
            (tidas::intervals::*)() const) &tidas::intervals::read_data,
            py::return_value_policy::take_ownership)
        .def("write", &tidas::intervals::write_data)
        .def_static("total_samples", &tidas::intervals::total_samples)
        .def_static("total_time", &tidas::intervals::total_time)
        .def_static("seek", &tidas::intervals::seek)
        .def_static("seek_ceil", &tidas::intervals::seek_ceil)
        .def_static("seek_floor", &tidas::intervals::seek_floor)
        .def("info", &tidas::intervals::info);


    py::class_ < tidas::field > (m, "Field")
        .def(py::init<>())
        .def(py::init< std::string, tidas::data_type, std::string >())
        .def_readwrite("type", &tidas::field::type)
        .def_readwrite("name", &tidas::field::name)
        .def_readwrite("units", &tidas::field::units);


    py::class_ < tidas::schema > (m, "Schema")
        .def(py::init<>())
        .def(py::init< std::vector < tidas::field > >())
        .def("clear", &tidas::schema::clear)
        .def("field_add", &tidas::schema::field_add)
        .def("field_del", &tidas::schema::field_del)
        .def("field_get", &tidas::schema::field_get,
            py::return_value_policy::reference_internal)
        .def("fields", &tidas::schema::fields,
            py::return_value_policy::reference_internal);


    py::class_ < tidas::group > (m, "Group")
        .def(py::init<>())
        .def(py::init< tidas::schema, tidas::dict, size_t >())
        .def("dictionary", &tidas::group::dictionary,
            py::return_value_policy::reference_internal)
        .def("schema", &tidas::group::schema_get,
            py::return_value_policy::reference_internal)
        .def("size", &tidas::group::size,
            py::return_value_policy::take_ownership)
        .def("resize", &tidas::group::resize)
        .def("range", [](tidas::group & self) {
            tidas::time_type start;
            tidas::time_type stop;
            self.range(start, stop);
            return std::make_tuple(start, stop);
        })
        .def("write_times", [](tidas::group & self, tidas::index_type offset,
            py::buffer times) {
            py::buffer_info info = times.request();
            if (info.ndim != 1) {
                throw std::runtime_error("Incompatible buffer dimension!");
            }
            size_t ndata = info.size;
            size_t n = self.size();
            if (offset + ndata > n) {
                throw std::runtime_error("offset plus length out of range!");
            }
            // Check that we have a compatible type
            if (info.format != "d") {
                throw std::runtime_error("Incompatible timestamp format");
            }
            tidas::time_type * raw = static_cast < tidas::time_type * >
                (info.ptr);
            self.write_times ( ndata, raw, offset );
            return;
        })
        .def("read_times", [](tidas::group & self, tidas::index_type offset,
            tidas::index_type ndata) {
            size_t n = self.size();
            if (offset + ndata > n) {
                throw std::runtime_error("offset plus ndata out of range!");
            }
            // Create a numpy array
            py::array_t < tidas::time_type > times;
            times.resize( {ndata} );
            py::buffer_info info = times.request();
            tidas::time_type * raw = static_cast < tidas::time_type * >
                (info.ptr);
            self.read_times ( ndata, raw, offset );
            return times;
        })
        .def("info", &tidas::group::info)
        .def("read", [](tidas::group & self, std::string field_name,
            tidas::index_type offset, tidas::index_type ndata) {
            // Get information about the field
            size_t n = self.size();
            if (offset + ndata > n) {
                throw std::runtime_error("offset plus ndata out of range!");
            }
            tidas::schema schm = self.schema_get();
            tidas::field fld = schm.field_get(field_name);
            // Create a numpy array and fill it
            py::array data = tidas_numpy_array(fld.type, ndata);
            if ( fld.type == tidas::data_type::string ) {
                char * cp = static_cast < char * > (data.mutable_data());
                self.read_field_astype ( field_name, offset, ndata, fld.type,
                    static_cast < void * > (&cp) );
            } else {
                self.read_field_astype ( field_name, offset, ndata, fld.type,
                    data.mutable_data() );
            }
            return data;
        })
        .def("write", [](tidas::group & self, std::string field_name,
            tidas::index_type offset, py::buffer data) {
            // Get info about input buffer
            py::buffer_info info = data.request();
            if (info.ndim != 1) {
                throw std::runtime_error("Incompatible buffer dimension!");
            }
            size_t ndata = info.size;
            size_t n = self.size();
            if (offset + ndata > n) {
                throw std::runtime_error("offset plus length out of range!");
            }
            // Get information about the field
            tidas::schema schm = self.schema_get();
            tidas::field fld = schm.field_get(field_name);
            std::string format = tidas_format_descr(fld.type);
            // Check compatible types (strings are special, since the numpy
            // format code is not the same as what is stored in the pybind11
            // format descriptor).
            if ((fld.type != tidas::data_type::string) && (info.format != format)) {
                throw std::runtime_error("Incompatible buffer data type");
            }
            if ( fld.type == tidas::data_type::string ) {
                char * cp = static_cast < char * > (info.ptr);
                self.write_field_astype ( field_name, offset, ndata, fld.type,
                    static_cast < void * > (&cp) );
            } else {
                self.write_field_astype ( field_name, offset, ndata, fld.type,
                    info.ptr );
            }
            return;
        });


    py::class_ < tidas::block > (m, "Block")
        .def(py::init<>())
        .def("aux_dir", &tidas::block::aux_dir,
            py::return_value_policy::take_ownership)
        .def("clear", &tidas::block::clear)
        .def("range", [](tidas::block & self) {
            tidas::time_type start;
            tidas::time_type stop;
            self.range(start, stop);
            return std::make_tuple(start, stop);
        })
        .def("group_add", &tidas::block::group_add,
            py::return_value_policy::reference_internal)
        .def("group_get",
            (tidas::group & (tidas::block::*)(std::string const &))
            &tidas::block::group_get,
            py::return_value_policy::reference_internal)
        .def("group_del", &tidas::block::group_del)
        .def("clear_groups", &tidas::block::clear_groups)
        .def("group_names", &tidas::block::group_names,
            py::return_value_policy::take_ownership)
        .def("intervals_add", &tidas::block::intervals_add,
            py::return_value_policy::reference_internal)
        .def("intervals_get", (tidas::intervals &
            (tidas::block::*)(std::string const &))
            &tidas::block::intervals_get,
            py::return_value_policy::reference_internal)
        .def("intervals_del", &tidas::block::intervals_del)
        .def("clear_intervals", &tidas::block::clear_intervals)
        .def("intervals_names", &tidas::block::intervals_names,
            py::return_value_policy::take_ownership)
        .def("block_add", &tidas::block::block_add,
            py::return_value_policy::reference_internal)
        .def("block_get", (tidas::block &
            (tidas::block::*)(std::string const &))
            &tidas::block::block_get,
            py::return_value_policy::reference_internal)
        .def("block_del", &tidas::block::block_del)
        .def("clear_blocks", &tidas::block::clear_blocks)
        .def("block_names", &tidas::block::block_names,
            py::return_value_policy::take_ownership)
        .def("select", &tidas::block::select,
            py::return_value_policy::take_ownership)
        .def("info", &tidas::block::info);

//
//
//
// template < class P >
// void exec ( P & op, exec_order order )
//
//
// template < class P >
// void exec ( P & op, exec_order order ) const
//


    m.def("data_copy", [](tidas::intervals const & in, tidas::intervals & out) {
        tidas::data_copy(in, out);
        return;
    });

    m.def("data_copy", [](tidas::group const & in, tidas::group & out) {
        tidas::data_copy(in, out);
        return;
    });

    m.def("data_copy", [](tidas::block const & in, tidas::block & out) {
        tidas::data_copy(in, out);
        return;
    });


    py::class_ < tidas::volume > (m, "Volume")
        .def(py::init < std::string, tidas::access_mode > ())
        .def(py::init < std::string, tidas::backend_type,
            tidas::compression_type, std::map < std::string, std::string > > ())
        .def("duplicate", &tidas::volume::duplicate)
        .def("link", &tidas::volume::link)
        .def("root", (tidas::block & (tidas::volume::*)())
            &tidas::volume::root,
            py::return_value_policy::reference_internal)
        .def("info", [](tidas::volume & self) {
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
