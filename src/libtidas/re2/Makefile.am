
# Initialize compile and linking flags

AM_CPPFLAGS = \
-I$(top_srcdir)/src/libtidas/re2

AM_CFLAGS =
AM_CXXFLAGS =
AM_FCFLAGS =

AM_LDFLAGS = \
-L$(top_builddir)/src/libtidas/re2

AM_LIBS = 

# Append compile and linking flags based on configure variables
include $(top_srcdir)/src/build_options.am


noinst_HEADERS = \
	util/arena.h \
	util/atomicops.h \
	util/benchmark.h \
	util/flags.h \
	util/logging.h \
	util/mutex.h \
	util/pcre.h \
	util/random.h \
	util/sparse_array.h \
	util/sparse_set.h \
	util/test.h \
	util/utf.h \
	util/util.h \
	util/valgrind.h \
	tidas/re2/filtered_re2.h \
	tidas/re2/prefilter.h \
	tidas/re2/prefilter_tree.h \
	tidas/re2/prog.h \
	tidas/re2/re2.h \
	tidas/re2/regexp.h \
	tidas/re2/set.h \
	tidas/re2/stringpiece.h \
	tidas/re2/unicode_casefold.h \
	tidas/re2/unicode_groups.h \
	tidas/re2/variadic_function.h \
	tidas/re2/walker-inl.h 

noinst_LTLIBRARIES = libtidas_re2.la

libtidas_re2_la_includedir = $(includedir)/tidas/re2

libtidas_re2_la_include_HEADERS = \
	tidas/re2/filtered_re2.h \
	tidas/re2/re2.h \
	tidas/re2/set.h \
	tidas/re2/stringpiece.h \
	tidas/re2/variadic_function.h

libtidas_re2_la_SOURCES = \
	arena.cc \
	hash.cc \
	rune.cc \
	stringpiece.cc \
	stringprintf.cc \
	strutil.cc \
	valgrind.cc \
	bitstate.cc \
	compile.cc \
	dfa.cc \
	filtered_re2.cc \
	mimics_pcre.cc \
	nfa.cc \
	onepass.cc \
	parse.cc \
	perl_groups.cc \
	prefilter.cc \
	prefilter_tree.cc \
	prog.cc \
	re2.cc \
	regexp.cc \
	set.cc \
	simplify.cc \
	tostring.cc \
	unicode_casefold.cc \
	unicode_groups.cc

libtidas_re2_la_LIBADD =


