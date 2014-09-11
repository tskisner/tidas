
# Initialize compile and linking flags

AM_CPPFLAGS = \
-I$(top_srcdir)/src/libtidas/libre2

AM_CFLAGS =
AM_CXXFLAGS =
AM_FCFLAGS =

AM_LDFLAGS = \
-L$(top_builddir)/src/libtidas/libre2

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
	re2/filtered_re2.h \
	re2/prefilter.h \
	re2/prefilter_tree.h \
	re2/prog.h \
	re2/re2.h \
	re2/regexp.h \
	re2/set.h \
	re2/stringpiece.h \
	re2/unicode_casefold.h \
	re2/unicode_groups.h \
	re2/variadic_function.h \
	re2/walker-inl.h 

noinst_LTLIBRARIES = libre2.la

libre2_la_includedir = $(includedir)/re2

libre2_la_include_HEADERS = \
	re2/filtered_re2.h \
	re2/re2.h \
	re2/set.h \
	re2/stringpiece.h \
	re2/variadic_function.h

libre2_la_SOURCES = \
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

libtidas_la_LIBADD =


