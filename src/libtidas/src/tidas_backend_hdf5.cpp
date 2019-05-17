/*
   TImestream DAta Storage (TIDAS)
   Copyright (c) 2014-2018, all rights reserved.  Use of this source code
   is governed by a BSD-style license that can be found in the top-level
   LICENSE file.
 */

#include <tidas_internal.hpp>

#include <tidas_backend_hdf5.hpp>


using namespace std;
using namespace tidas;


#ifdef HAVE_HDF5


hid_t tidas::hdf5_data_type(data_type type) {
    hid_t datatype;
    herr_t status;

    switch (type) {
        case data_type::int8:
            datatype = H5Tcopy(H5T_NATIVE_INT8);
            status = H5Tset_order(datatype, H5T_ORDER_LE);
            break;

        case data_type::uint8:
            datatype = H5Tcopy(H5T_NATIVE_UINT8);
            status = H5Tset_order(datatype, H5T_ORDER_LE);
            break;

        case data_type::int16:
            datatype = H5Tcopy(H5T_NATIVE_INT16);
            status = H5Tset_order(datatype, H5T_ORDER_LE);
            break;

        case data_type::uint16:
            datatype = H5Tcopy(H5T_NATIVE_UINT16);
            status = H5Tset_order(datatype, H5T_ORDER_LE);
            break;

        case data_type::int32:
            datatype = H5Tcopy(H5T_NATIVE_INT32);
            status = H5Tset_order(datatype, H5T_ORDER_LE);
            break;

        case data_type::uint32:
            datatype = H5Tcopy(H5T_NATIVE_UINT32);
            status = H5Tset_order(datatype, H5T_ORDER_LE);
            break;

        case data_type::int64:
            datatype = H5Tcopy(H5T_NATIVE_INT64);
            status = H5Tset_order(datatype, H5T_ORDER_LE);
            break;

        case data_type::uint64:
            datatype = H5Tcopy(H5T_NATIVE_UINT64);
            status = H5Tset_order(datatype, H5T_ORDER_LE);
            break;

        case data_type::float32:
            datatype = H5Tcopy(H5T_NATIVE_FLOAT);
            status = H5Tset_order(datatype, H5T_ORDER_LE);
            break;

        case data_type::float64:
            datatype = H5Tcopy(H5T_NATIVE_DOUBLE);
            status = H5Tset_order(datatype, H5T_ORDER_LE);
            break;

        case data_type::string:
            datatype = H5Tcopy(H5T_C_S1);
            status = H5Tset_size(datatype, backend_string_size);
            break;

        default:
            TIDAS_THROW("data type not recognized");
            break;
    }

    return datatype;
}

#endif // ifdef HAVE_HDF5
