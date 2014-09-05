
# Initialize compile and linking flags

AM_CPPFLAGS = \
-I$(top_srcdir)/src/libtidas

AM_CFLAGS =
AM_CXXFLAGS =
AM_FCFLAGS =

AM_LDFLAGS = \
-L$(top_builddir)/src/libtidas

AM_LIBS = 

# Append compile and linking flags based on configure variables
include $(top_srcdir)/src/build_options.am


include_HEADERS = tidas.h
noinst_HEADERS = tidas_internal.h

noinst_SCRIPTS = gen_pkgconfig.sh

EXTRA_DIST = gen_pkgconfig.sh

lib_LTLIBRARIES = libtidas.la

libtidas_la_includedir = $(includedir)/tidas

libtidas_la_include_HEADERS = \
	tidas/utils.h \
	tidas/intervals.h \
	tidas/schema.h

libtidas_la_SOURCES = \
tidas.c \
tidas_utils.c \
tidas_intervals.c \
tidas_schema.c

libtidas_la_LDFLAGS = -version-info @TIDAS_LT_VERSION@

libtidas_la_LIBADD =

