
SUBDIRS = gtest

# Initialize compile and linking flags

AM_CPPFLAGS = \
-I$(top_srcdir)/src/libtidas \
-I$(top_srcdir)/src/libtidas/sqlite \
-I$(top_srcdir)/src/tests/gtest \
-I$(top_srcdir)/src/tests

AM_CFLAGS =
AM_CXXFLAGS =
AM_FCFLAGS =

AM_LDFLAGS = 

AM_LIBS = \
$(top_builddir)/src/tests/gtest/libtidas_gtest.la \
$(top_builddir)/src/libtidas/libtidas.la

# Add check flags

AM_CPPFLAGS += $(CHECK_CFLAGS)
AM_LIBS += $(CHECK_LIBS)

# Append compile and linking flags based on configure variables
include $(top_srcdir)/src/build_options.am


noinst_HEADERS = tidas_test.hpp

bin_PROGRAMS = tidas_test
TESTS = tidas_test

tidas_test_SOURCES = \
tidas_test.cpp \
tidas_test_utils.cpp \
tidas_test_dict.cpp \
tidas_test_intervals.cpp \
tidas_test_schema.cpp \
tidas_test_group.cpp \
tidas_test_block.cpp \
tidas_test_indexdb.cpp \
tidas_test_volume.cpp 

tidas_test_LDADD = $(AM_LIBS)


clean-local :
	@rm -rf *.out

