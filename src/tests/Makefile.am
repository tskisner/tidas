
# Initialize compile and linking flags

AM_CPPFLAGS = \
-I$(top_srcdir)/src/libtidas \
-I$(top_srcdir)/src/tests

AM_CFLAGS =
AM_CXXFLAGS =
AM_FCFLAGS =

AM_LDFLAGS = \
-L$(top_builddir)/src/libtidas \
-L$(top_builddir)/src/tests

AM_LIBS = $(top_builddir)/src/libtidas/libtidas.la

# Append compile and linking flags based on configure variables
include $(top_srcdir)/src/build_options.am


noinst_HEADERS = tidas_test.h

bin_PROGRAMS = tidas_test
TESTS = tidas_test

tidas_test_SOURCES = \
tidas_test.c \
tidas_test_utils.c \
tidas_test_intervals.c

tidas_test_LDADD = $(AM_LIBS)


clean-local :
	@rm -f *.out

