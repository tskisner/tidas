
# Initialize compile and linking flags

AM_CPPFLAGS = \
-I$(top_srcdir)/src/libtidas \
-I$(top_srcdir)/src/libtidas/core \
-I$(top_srcdir)/src/libtidas/sqlite

AM_CFLAGS =
AM_CXXFLAGS =
AM_FCFLAGS =

AM_LDFLAGS =

AM_LIBS = 

# Append compile and linking flags based on configure variables
include $(top_srcdir)/src/build_options.am


include_HEADERS = tidas.hpp
noinst_HEADERS = \
	tidas_internal.hpp \
	tidas_group_hdf5.hpp \
	tidas_backend_getdata.hpp \
	tidas_backend_hdf5.hpp

noinst_LTLIBRARIES = libtidas_core.la

libtidas_core_la_includedir = $(includedir)/tidas

libtidas_core_la_include_HEADERS = \
	tidas/utils.hpp \
	tidas/dict.hpp \
	tidas/intervals.hpp \
	tidas/schema.hpp \
	tidas/group.hpp \
	tidas/block.hpp \
	tidas/volume.hpp \
	tidas/copy.hpp \
	tidas/indexdb.hpp
	

libtidas_core_la_SOURCES = \
tidas.cpp \
tidas_utils.cpp \
tidas_backend_hdf5.cpp \
tidas_backend_getdata.cpp \
tidas_dict.cpp \
tidas_dict_hdf5.cpp \
tidas_dict_getdata.cpp \
tidas_intervals.cpp \
tidas_intervals_hdf5.cpp \
tidas_intervals_getdata.cpp \
tidas_schema.cpp \
tidas_schema_hdf5.cpp \
tidas_schema_getdata.cpp \
tidas_group.cpp \
tidas_group_hdf5.cpp \
tidas_group_getdata.cpp \
tidas_block.cpp \
tidas_volume.cpp \
tidas_copy.cpp \
tidas_indexdb_mem.cpp \
tidas_indexdb_sql.cpp

