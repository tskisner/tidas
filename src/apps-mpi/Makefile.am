
CXX = @MPICXX@

# Initialize compile and linking flags

AM_CPPFLAGS = \
-I$(top_srcdir)/src/apps-mpi \
-I$(top_srcdir)/src/libtidas \
-I$(top_srcdir)/src/libtidas/sqlite \
-I$(top_srcdir)/src/libtidas/core \
-I$(top_srcdir)/src/libtidas/test \
-I$(top_srcdir)/src/libtidas/test/gtest \
-I$(top_srcdir)/src/libtidas-mpi


AM_CFLAGS =
AM_CXXFLAGS =
AM_FCFLAGS =

AM_LDFLAGS =

AM_LIBS = 

# Append compile and linking flags based on configure variables
include $(top_srcdir)/src/build_options.am


bin_PROGRAMS = tidas_test_mpi
TESTS = tidas_test_mpi

tidas_test_mpi_SOURCES = \
tidas_test_mpi.cpp

tidas_test_mpi_LDADD = \
$(top_builddir)/src/libtidas-mpi/libtidas-mpi.la \
$(top_builddir)/src/libtidas/libtidas.la \
$(AM_LIBS)


