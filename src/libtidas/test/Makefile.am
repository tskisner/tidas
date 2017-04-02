
SUBDIRS = gtest

# Initialize compile and linking flags

AM_CPPFLAGS = \
-I$(top_srcdir)/src/libtidas \
-I$(top_srcdir)/src/libtidas/sqlite \
-I$(top_srcdir)/src/libtidas/core \
-I$(top_srcdir)/src/libtidas/test \
-I$(top_srcdir)/src/libtidas/test/gtest

AM_CFLAGS =
AM_CXXFLAGS =
AM_FCFLAGS =

AM_LDFLAGS = 

AM_LIBS = \
$(top_builddir)/src/libtidas/test/gtest/libtidas_gtest.la

# Add check flags

AM_CPPFLAGS += $(CHECK_CFLAGS)
AM_LIBS += $(CHECK_LIBS)

# Append compile and linking flags based on configure variables
include $(top_srcdir)/src/build_options.am


noinst_HEADERS = tidas_test.hpp

noinst_LTLIBRARIES = libtidas_test.la

libtidas_test_la_SOURCES = \
tidas_test_runner.cpp \
tidas_test_utils.cpp \
tidas_test_cereal.cpp \
tidas_test_dict.cpp \
tidas_test_intervals.cpp \
tidas_test_schema.cpp \
tidas_test_group.cpp \
tidas_test_block.cpp \
tidas_test_indexdb.cpp \
tidas_test_volume.cpp 

libtidas_test_la_LIBADD = $(AM_LIBS)
