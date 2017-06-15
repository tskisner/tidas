
# Initialize compile and linking flags

AM_CPPFLAGS = \
-I$(top_srcdir)/src/apps \
-I$(top_srcdir)/src/libtidas \
-I$(top_srcdir)/src/libtidas/sqlite \
-I$(top_srcdir)/src/libtidas/core \
-I$(top_srcdir)/src/libtidas/test \
-I$(top_srcdir)/src/libtidas/test/gtest

AM_CFLAGS =
AM_CXXFLAGS =
AM_FCFLAGS =

AM_LDFLAGS =

AM_LIBS = 

# Append compile and linking flags based on configure variables
include $(top_srcdir)/src/build_options.am


bin_PROGRAMS = tidas_test
TESTS = tidas_test

tidas_test_SOURCES = \
tidas_test.cpp

tidas_test_LDADD = \
$(top_builddir)/src/libtidas/libtidas.la \
$(AM_LIBS)

