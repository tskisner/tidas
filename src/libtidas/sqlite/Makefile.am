
# Initialize compile and linking flags

AM_CPPFLAGS = \
-I$(top_srcdir)/src/libtidas/sqlite

AM_CFLAGS =
AM_CXXFLAGS =
AM_FCFLAGS =

AM_LDFLAGS = 

AM_LIBS = 

# Append compile and linking flags based on configure variables
include $(top_srcdir)/src/build_options.am


noinst_LTLIBRARIES = libtidas_sqlite.la

libtidas_sqlite_la_includedir = $(includedir)/tidas

libtidas_sqlite_la_include_HEADERS = \
	tidas/sqlite3.h \
	tidas/sqlite3ext.h

libtidas_sqlite_la_SOURCES = \
	sqlite3.c

libtidas_sqlite_la_LIBADD =


#bin_PROGRAMS = tidas_sqlite
#tidas_sqlite_SOURCES = shell.c
#tidas_sqlite_LDADD = $(top_builddir)/src/libtidas/sqlite/libtidas_sqlite.la


