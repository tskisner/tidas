Fixing Libtool
=================

Libtool is great, but there are some longstanding features that make some use cases
challenging.  We want to use different compiler drivers when linking serial and MPI
shared libraries.  Although we can reset CC and CXX on a per-subdirectory basis, we
can't change the hard-coded compilers used in the generated libtool script.  Instead
we create a new "libtool_mpi" script with the MPI compilers swapped into place, and
then use that for directories with MPI builds.

https://lists.gnu.org/archive/html/libtool/2016-04/msg00001.html
https://lists.gnu.org/archive/html/libtool/2012-05/msg00002.html

