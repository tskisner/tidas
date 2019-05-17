
// TImestream DAta Storage (TIDAS).
//
// Copyright (c) 2015-2019 by the parties listed in the AUTHORS file.  All rights
// reserved.  Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include <tidas_internal.hpp>

#include <tidas_backend_hdf5.hpp>

#include <regex>


using namespace std;
using namespace tidas;


tidas::dict_backend_hdf5::dict_backend_hdf5() {}

tidas::dict_backend_hdf5::~dict_backend_hdf5() {}

tidas::dict_backend_hdf5::dict_backend_hdf5(dict_backend_hdf5 const & other) {}

dict_backend_hdf5 & tidas::dict_backend_hdf5::operator=(dict_backend_hdf5 const & other)
{
    if (this != &other) {}
    return *this;
}

#ifdef HAVE_HDF5

typedef struct {
    map <string, string> data;
    map <string, data_type> types;
} tidas_dict_backend_hdf5_attr_data;


herr_t tidas_dict_backend_hdf5_attr_parse(hid_t location_id, const char * attr_name,
                                          const H5A_info_t * ainfo, void * op_data) {
    tidas_dict_backend_hdf5_attr_data * d =
        static_cast <tidas_dict_backend_hdf5_attr_data *> (op_data);

    string key = attr_name;

    herr_t status = 0;

    hid_t hattr = H5Aopen(location_id, attr_name, H5P_DEFAULT);

    H5A_info_t info;
    status = H5Aget_info(hattr, &info);

    char buf[info.data_size + 1];

    hid_t datatype = H5Tcopy(H5T_C_S1);
    status = H5Tset_size(datatype, info.data_size);

    status = H5Aread(hattr, datatype, (void *)buf);
    buf[info.data_size] = '\0';

    status = H5Aclose(hattr);
    status = H5Tclose(datatype);

    string val = buf;

    string match = string("^(.*)") + dict_hdf5_type_suffix + string("$");
    regex re(match, std::regex::extended);

    smatch name;

    if (regex_match(key, name, re)) {
        d->types[name[1]] = data_type_from_string(val);
    } else {
        d->data[key] = val;
    }

    return status;
}

herr_t tidas_dict_backend_hdf5_attr_list(hid_t location_id, const char * attr_name,
                                         const H5A_info_t * ainfo, void * op_data) {
    vector <string> * d = static_cast <vector <string> *> (op_data);

    string key = attr_name;

    herr_t status = 0;

    d->push_back(key);

    return status;
}

void tidas::dict_backend_hdf5::read(backend_path const & loc, map <string,
                                                                   string> & data,
                                    map <string, data_type> & types) {
    // open file in write mode

    string fspath = loc.path + path_sep + loc.name;

    herr_t status = H5open();

    hid_t fapl = H5Pcreate(H5P_FILE_ACCESS);
    H5Pset_fclose_degree(fapl, H5F_CLOSE_STRONG);
    hid_t file = H5Fopen(fspath.c_str(), H5F_ACC_RDONLY, fapl);
    H5Pclose(fapl);

    // open dataset

    hid_t dataset = H5Dopen(file, loc.meta.c_str(), H5P_DEFAULT);

    // iterate over attributes and populate data and types

    tidas_dict_backend_hdf5_attr_data iterdata;

    hsize_t aoff = 0;
    status = H5Aiterate(dataset, H5_INDEX_CRT_ORDER, H5_ITER_NATIVE, &aoff,
                        tidas_dict_backend_hdf5_attr_parse, (void *)&iterdata);

    data = iterdata.data;
    types = iterdata.types;

    // cleanup

    status = H5Dclose(dataset);
    status = H5Fflush(file, H5F_SCOPE_GLOBAL);
    status = H5Fclose(file);

    // status = H5close();

    return;
}

void tidas::dict_backend_hdf5::write(backend_path const & loc, map <string,
                                                                    string> const & data, map <string,
                                                                                               data_type> const & types)
const {
    // open file in write mode

    string fspath = loc.path + path_sep + loc.name;

    herr_t status = H5open();

    hid_t fapl = H5Pcreate(H5P_FILE_ACCESS);
    H5Pset_fclose_degree(fapl, H5F_CLOSE_STRONG);
    hid_t file = H5Fopen(fspath.c_str(), H5F_ACC_RDWR, fapl);
    H5Pclose(fapl);

    // open dataset

    hid_t dataset = H5Dopen(file, loc.meta.c_str(), H5P_DEFAULT);

    // get names of existing attributes

    vector <string> names;

    hsize_t aoff = 0;
    status = H5Aiterate(dataset, H5_INDEX_CRT_ORDER, H5_ITER_NATIVE, &aoff,
                        tidas_dict_backend_hdf5_attr_list, (void *)&names);

    // clear all attributes.  we just delete each one by name

    for (size_t i = 0; i < names.size(); ++i) {
        status = H5Adelete(dataset, names[i].c_str());
    }

    // create attributes for each dictionary item

    hid_t datatype = H5Tcopy(H5T_C_S1);

    hid_t dataspace = H5Screate(H5S_SCALAR);

    for (map <string, string>::const_iterator it = data.begin(); it != data.end();
         ++it) {
        string key = it->first;
        string val = it->second;

        string key_type = it->first + dict_hdf5_type_suffix;
        string val_type = data_type_to_string(types.at(it->first));

        status = H5Tset_size(datatype, val.size());
        hid_t attr = H5Acreate(dataset,
                               key.c_str(), datatype, dataspace, H5P_DEFAULT,
                               H5P_DEFAULT);
        status = H5Awrite(attr, datatype, (void *)val.c_str());
        status = H5Aclose(attr);

        status = H5Tset_size(datatype, val_type.size());
        attr = H5Acreate(dataset,
                         key_type.c_str(), datatype, dataspace, H5P_DEFAULT,
                         H5P_DEFAULT);
        status = H5Awrite(attr, datatype, (void *)val_type.c_str());
        status = H5Aclose(attr);
    }

    // cleanup

    status = H5Sclose(dataspace);
    status = H5Tclose(datatype);
    status = H5Dclose(dataset);
    status = H5Fflush(file, H5F_SCOPE_GLOBAL);
    status = H5Fclose(file);

    // status = H5close();

    // mark volume as dirty

    // if ( loc.vol == NULL ) {
    //    TIDAS_THROW( "volume handle is NULL, this should never happen!" );
    // }
    // loc.vol->set_dirty();

    return;
}

#else // ifdef HAVE_HDF5


void tidas::dict_backend_hdf5::read(backend_path const & loc, map <string,
                                                                   string> & data,
                                    map <string, data_type> & types) {
    TIDAS_THROW("TIDAS not compiled with HDF5 support");
    return;
}

void tidas::dict_backend_hdf5::write(backend_path const & loc, map <string,
                                                                    string> const & data, map <string,
                                                                                               data_type> const & types)
const {
    TIDAS_THROW("TIDAS not compiled with HDF5 support");
    return;
}

#endif // ifdef HAVE_HDF5
