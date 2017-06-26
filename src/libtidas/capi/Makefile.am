
# Initialize compile and linking flags

AM_CPPFLAGS = \
-I$(top_srcdir)/src/libtidas \
-I$(top_srcdir)/src/libtidas/sqlite \
-I$(top_srcdir)/src/libtidas/core \
-I$(top_srcdir)/src/libtidas/test \
-I$(top_srcdir)/src/libtidas/test/gtest \
-I$(top_srcdir)/src/libtidas/capi

AM_CFLAGS =
AM_CXXFLAGS =
AM_FCFLAGS =

AM_LDFLAGS =

AM_LIBS = 

# Append compile and linking flags based on configure variables
include $(top_srcdir)/src/build_options.am

include_HEADERS = ctidas.h
noinst_HEADERS = \
	ctidas.hpp


noinst_LTLIBRARIES = libtidas_capi.la

libtidas_capi_la_SOURCES = \
ctidas.cpp

