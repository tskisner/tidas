
CXX = @MPICXX@

# Initialize compile and linking flags

AM_CPPFLAGS = \
-I$(top_srcdir)/src/libtidas \
-I$(top_srcdir)/src/libtidas/sqlite

AM_CFLAGS =
AM_CXXFLAGS =
AM_FCFLAGS =

AM_LDFLAGS =

AM_LIBS = 

# Append compile and linking flags based on configure variables
include $(top_srcdir)/src/build_options.am


include_HEADERS = tidas_mpi.hpp #ctidas_mpi.h
noinst_HEADERS = \
	tidas_mpi_internal.hpp


lib_LTLIBRARIES = libtidas-mpi.la

libtidas_mpi_la_includedir = $(includedir)/tidas

libtidas_mpi_la_include_HEADERS = \
	tidas/mpi_volume.hpp

libtidas_mpi_la_SOURCES = \
tidas_mpi_volume.cpp

libtidas_mpi_la_LDFLAGS = -version-info @TIDAS_LT_VERSION@
libtidas_la_LIBADD =

