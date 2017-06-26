
SUBDIRS = sqlite core test capi

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

AM_LIBS = 

# Append compile and linking flags based on configure variables
include $(top_srcdir)/src/build_options.am


EXTRA_DIST = cereal

noinst_SCRIPTS = gen_pkgconfig.sh

EXTRA_DIST += gen_pkgconfig.sh


lib_LTLIBRARIES = libtidas.la

libtidas_la_SOURCES = tidas.cpp

libtidas_la_LDFLAGS = -version-info @TIDAS_LT_VERSION@

libtidas_la_LIBADD = \
$(top_builddir)/src/libtidas/test/libtidas_test.la \
$(top_builddir)/src/libtidas/core/libtidas_core.la \
$(top_builddir)/src/libtidas/sqlite/libtidas_sqlite.la \
$(top_builddir)/src/libtidas/capi/libtidas_capi.la \
$(AM_LIBS)


# install / uninstall cereal headers

install-data-local:
	@$(mkinstalldirs) $(DESTDIR)$(includedir)/cereal; \
	cp -r $(srcdir)/cereal/* $(DESTDIR)$(includedir)/cereal/; \
	chmod -R a=rX,u+w $(DESTDIR)$(includedir)/cereal

uninstall-local:
	@rm -rf $(DESTDIR)$(includedir)/cereal; \
	rm -rf $(DESTDIR)$(includedir)/tidas

