SUBDIRS =

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

pkgtidasdir = $(pyexecdir)/tidas

pkgtidas_PYTHON = \
__init__.py \
ctidas.py \
intervals.py \
group.py \
block.py \
volume.py \
test.py


ctidas.py : ctidas.py.in
	@./gen_python.sh \
	"$(DESTDIR)$(libdir)/libtidas.$(TIDAS_SHARED_EXT)"


EXTRA_DIST = gen_python.sh ctidas.py.in

CLEANFILES = ctidas.py

