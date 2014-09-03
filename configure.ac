dnl
dnl This is the Autoconf file for TIDAS
dnl
dnl +------------------------
dnl | Initialize package info
dnl +------------------------
AC_INIT([TIDAS], [1.0-alpha], [work@theodorekisner.com], [tidas], [https://theodorekisner.com/software/tidas/])
AC_CONFIG_SRCDIR([Makefile.am])
AM_INIT_AUTOMAKE
AC_CONFIG_HEADERS(config.h)
AC_CONFIG_MACRO_DIR([m4])
dnl +-------------------------
dnl | If the library source code has changed at all since the last update, 
dnl | then increment revision (c:r:a becomes c:r+1:a).  If any interfaces 
dnl | have been added, removed, or changed since the last update, increment
dnl | current, and set revision to 0.  If any interfaces have been added 
dnl | since the last public release, then increment age.  If any interfaces 
dnl | have been removed since the last public release, then set age to 0.
dnl +-------------------------
TIDAS_LT_VERSION="1:0:0"
AC_SUBST(TIDAS_LT_VERSION)
dnl +---------------------
dnl | Compiler features
dnl +---------------------
dnl Set default optimization to '-O3' instead of the default '-g -O2'
if test x"${CFLAGS}" = x; then
   CFLAGS="-O3"
fi
if test x"${CXXFLAGS}" = x; then
   CXXFLAGS="-O3"
fi
if test x"${FCFLAGS}" = x; then
   FCFLAGS="-O3"
fi
AC_CANONICAL_HOST
AC_PROG_INSTALL
LT_INIT
dnl +-------------------------
dnl | C support
dnl +-------------------------
AC_PROG_CC
dnl +-------------------------
dnl | C++ support
dnl +-------------------------
tidas_cxx=yes
AC_ARG_ENABLE(c++, [AC_HELP_STRING([--disable-c++], [Disable C++ bindings])], [tidas_cxx=$enable_c++])
if test $tidas_cxx = yes; then
  AC_PROG_CXX
  if test x"$CXX" = "x"; then
    tidas_cxx=no
  fi
fi
AM_CONDITIONAL([HAVE_AM_CXX], [test $tidas_cxx = yes])
dnl +-------------------------
dnl | Fortran support
dnl +-------------------------
tidas_fortran=yes
FCMODEXT="mod"
AC_ARG_ENABLE(fortran, [AC_HELP_STRING([--disable-fortran], [Disable Fortran 2003 bindings])], [tidas_fortran=$enable_fortran])
if test $tidas_fortran = yes; then
  AC_PROG_FC([], [2000])
  AC_FC_LIBRARY_LDFLAGS
  AC_FC_WRAPPERS
  ACX_FC_PP_SRCEXT([f03])
  ACX_FC_PP_DEFINE
  if test x"$FC" = x; then
    tidas_fortran=no
  else
    AX_F90_MODULE_EXTENSION
    FCMODEXT="$ax_cv_f90_modext"
  fi
fi
AM_CONDITIONAL([HAVE_AM_FC], [test $tidas_fortran != yes])
AC_SUBST(FCMODEXT)
dnl +------------------------------------------------
dnl | Check for MPI
dnl +------------------------------------------------
AC_LANG([C])
tidas_mpi=no
tidas_mpi_cxx=no
tidas_mpi_fortran=no
ACX_MPI([tidas_mpi=yes], [])
if test $tidas_cxx = yes; then
  AC_LANG_PUSH([C++])
  ACX_MPI([tidas_mpi_cxx=yes], [])
  AC_LANG_POP([C++])
fi
if test $tidas_fortran = yes; then
  AC_LANG_PUSH([Fortran])
  ACX_MPI([tidas_mpi_fortran=yes], [])
  AC_LANG_POP([Fortran])
fi
AM_CONDITIONAL([HAVE_AM_MPI], [test tidas_mpi = yes])
AM_CONDITIONAL([HAVE_AM_MPI_CXX], [test tidas_mpi_cxx = yes])
AM_CONDITIONAL([HAVE_AM_MPI_FC], [test tidas_mpi_fortran = yes])
dnl +-------------------------
dnl | Option to perform full static linking of executables, not just
dnl | building of static libraries.
dnl +-------------------------
tidas_static=no
AC_ARG_ENABLE([all-static], [AC_HELP_STRING([--enable-all-static], [force static linking of executables.])])
if test x"$enable_all_static" = x"yes"; then
  if test x"$enable_static" != x"yes"; then
    AC_MSG_ERROR([Cannot perform static linking of executables if static libraries are disabled!])
  fi
  tidas_static=yes
fi
AM_CONDITIONAL([USE_AM_ALLSTATIC], [test "$tidas_static" = "yes"])
dnl +------------------------------------------------
dnl | Check for Python
dnl +------------------------------------------------
tidas_python=yes
AC_ARG_ENABLE(python, [AC_HELP_STRING([--disable-python], [Disable python bindings])], [tidas_python=$enable_python])
if test $tidas_python = yes; then
  AM_PATH_PYTHON
  ACX_PYTHON_DEV
  ACX_NUMPY
fi
dnl +------------------------------------------------
dnl | Check for HDF5
dnl +------------------------------------------------
AX_LIB_HDF5([serial])
if test x"$HDF5_CC" = x; then
  AC_MSG_ERROR([Could not find serial HDF5 library- is h5cc in your PATH?])
fi
dnl +------------------------------------------------
dnl | Set outputs
dnl +------------------------------------------------
AC_CONFIG_FILES([Makefile])
AC_CONFIG_FILES([src/Makefile])
AC_CONFIG_FILES([src/libtidas/Makefile])
dnl AC_CONFIG_FILES([src/tests/Makefile])
dnl AC_CONFIG_FILES([src/apps/Makefile])
dnl AC_CONFIG_FILES([src/python/Makefile])
dnl AC_CONFIG_FILES([src/tests-python/Makefile])
dnl AC_CONFIG_FILES([docs/Makefile])
dnl AC_CONFIG_FILES([src/libtidas-mpi/Makefile])
dnl AC_CONFIG_FILES([src/tests-mpi/Makefile])
dnl AC_CONFIG_FILES([src/apps-mpi/Makefile])
dnl +------------------------------------------------
dnl | Generate Makefiles
dnl +------------------------------------------------
AC_OUTPUT
dnl +------------------------------------------------
dnl | Print out detected build options
dnl +------------------------------------------------
AC_MSG_NOTICE([                                           ])
AC_MSG_NOTICE([=========== Build Configuration ===========])
AC_MSG_NOTICE([                                           ])
AC_MSG_NOTICE([  Serial C Compiler         : $CC])
AC_MSG_NOTICE([  C Compile Flags           : $CFLAGS])
if test tidas_cxx = yes; then
  AC_MSG_NOTICE([  Serial C++ Compiler       : $CXX])
  AC_MSG_NOTICE([  C++ Compile Flags         : $CXXFLAGS])
else
  AC_MSG_NOTICE([  Serial C++ Compiler       : Disabled])
  AC_MSG_NOTICE([  C++ Compile Flags         : Disabled])
fi
if test tidas_fortran = yes; then
  AC_MSG_NOTICE([  Serial F03 Compiler       : $FC])
  AC_MSG_NOTICE([  F03 Compile Flags         : $FCFLAGS])
else
  AC_MSG_NOTICE([  Serial F03 Compiler       : Disabled])
  AC_MSG_NOTICE([  F03 Compile Flags         : Disabled])
fi
if test tidas_mpi = yes; then
  AC_MSG_NOTICE([  MPI C Compiler            : $MPICC])
else
  AC_MSG_NOTICE([  MPI C Compiler            : Disabled])
fi
if test tidas_mpi_cxx = yes; then
  AC_MSG_NOTICE([  MPI C++ Compiler          : $MPICXX])
else
  AC_MSG_NOTICE([  MPI C++ Compiler          : Disabled])
fi
if test tidas_mpi_fortran = yes; then
  AC_MSG_NOTICE([  MPI F03 Compiler          : $MPIFC])
else
  AC_MSG_NOTICE([  MPI F03 Compiler          : Disabled])
fi
if test $tidas_python = yes; then
  AC_MSG_NOTICE([  Python                    : $PYTHON])
  AC_MSG_NOTICE([  Python includes           : $PYTHON_CPPFLAGS $NUMPY_CPPFLAGS])
  AC_MSG_NOTICE([  Python dev libraries      : $PYTHON_LDFLAGS])
else
  AC_MSG_NOTICE([  Python                    : Disabled])
fi
AC_MSG_NOTICE([  HDF5 Compile              : $HDF5_CPPFLAGS])
AC_MSG_NOTICE([  HDF5 Link                 : $HDF5_LDFLAGS $HDF5_LIBS])
AC_MSG_NOTICE([                                           ])
