dnl
dnl This is the Autoconf file for TIDAS
dnl
dnl +------------------------
dnl | Initialize package info
dnl +------------------------
AC_INIT([TIDAS], m4_esyscmd_s([m4/git-version.sh]), [work@theodorekisner.com], [tidas], [https://github.com/hpc4cmb/tidas])
AC_CONFIG_SRCDIR([Makefile.am])
AM_INIT_AUTOMAKE([foreign])
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
LT_INIT([disable-fast-install])
AC_SUBST([LIBTOOL_DEPS])

dnl +-------------------------
dnl | Shared library extension
dnl +-------------------------
TIDAS_SHARED_EXT=""
case $host_os in
  darwin* )
    TIDAS_SHARED_EXT="dylib"
    ;;
  linux*)
    TIDAS_SHARED_EXT="so"
    ;;
  *BSD*)
    TIDAS_SHARED_EXT="so"
    ;;
  *)
    TIDAS_SHARED_EXT="so"
    AC_MSG_WARN([Your platform was not detected- using Linux defaults for shared library suffix])
    ;;
esac
AC_SUBST(TIDAS_SHARED_EXT)

dnl +-------------------------
dnl | Utilities
dnl +-------------------------
AC_PROG_MKDIR_P

dnl +-------------------------
dnl | C support
dnl +-------------------------
AC_PROG_CC

dnl +-------------------------
dnl | C++ support
dnl +-------------------------
AC_PROG_CXX

dnl +-------------------------
dnl | Fortran support
dnl +-------------------------
tidas_fortran=yes
FCMODEXT="mod"
AC_ARG_ENABLE(fortran, [AC_HELP_STRING([--disable-fortran], [Disable Fortran 2003 bindings])], [tidas_fortran=$enable_fortran], [tidas_fortran=yes])
if test $tidas_fortran = yes; then
  AC_PROG_FC([], [2000])
  AC_FC_LIBRARY_LDFLAGS
  AC_FC_WRAPPERS
  ACX_FC_PP_SRCEXT([f03])
  ACX_FC_PP_DEFINE
  AX_F90_MODULE_EXTENSION
  FCMODEXT="$ax_cv_f90_modext"
fi
AM_CONDITIONAL([HAVE_AM_FC], [test $tidas_fortran = yes])
AC_SUBST(FCMODEXT)

dnl +------------------------------------------------
dnl | This is a C++ project...
dnl +------------------------------------------------
AC_LANG([C++])

dnl Ensure that pkg-config is setup
#PKG_PROG_PKG_CONFIG

dnl +------------------------------------------------
dnl | Check for MPI
dnl +------------------------------------------------
tidas_mpi=yes
tidas_mpi_fortran=no
AC_ARG_ENABLE([mpi], [AC_HELP_STRING([--disable-mpi], [Disable all checks for MPI])], [tidas_mpi=$enable_mpi], [tidas_mpi=yes])
if test $tidas_mpi = yes; then
  ACX_MPI
  if test x"$MPICXX" = x; then
    tidas_mpi=no
  else
    AC_LANG_PUSH([C])
    ACX_MPI
    AC_LANG_POP([C])
    if test x"$MPICC" = x; then
      tidas_mpi=no
    fi
    if test $tidas_fortran = yes; then
      AC_LANG_PUSH([Fortran])
      ACX_MPI
      AC_LANG_POP([Fortran])
      if test x"$MPIFC" != x; then
        tidas_mpi_fortran=yes
      fi
    fi
  fi
fi
AM_CONDITIONAL([HAVE_AM_MPI], [test $tidas_mpi = yes])
AM_CONDITIONAL([HAVE_AM_MPI_FC], [test $tidas_mpi_fortran = yes])

dnl +------------------------------------------------
dnl | Check for C++11 language support
dnl | since this modifies CXX and MPICXX, save these
dnl | in other variables for later use.
dnl +------------------------------------------------
CXX_ORIG="$CXX"
MPICXX_ORIG="$MPICXX"
AC_SUBST([CXX_ORIG])
AC_SUBST([MPICXX_ORIG])
AX_CXX_COMPILE_STDCXX([11], [noext], [mandatory])

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

dnl +-------------------------
dnl | Should we enable expensive debugging checks?
dnl +-------------------------
tidas_debug=no
AC_ARG_ENABLE([debug], [AC_HELP_STRING([--enable-debug], [Enable expensive debugging checks])])
if test x"$enable_debug" = x"yes"; then
  tidas_debug=yes
fi
AM_CONDITIONAL([ENABLE_AM_DEBUG], [test $tidas_debug = yes])

dnl +------------------------------------------------
dnl | Check for Python
dnl +------------------------------------------------
AC_LANG_PUSH([C])
tidas_python=yes
AC_ARG_ENABLE(python, [AC_HELP_STRING([--disable-python], [Disable python bindings])], [tidas_python=$enable_python])
if test $tidas_static = yes; then
  tidas_python=no
fi
if test $tidas_python = yes; then
  AM_PATH_PYTHON(,[tidas_python=yes],[tidas_python=no])
fi
AC_LANG_POP([C])
AM_CONDITIONAL([HAVE_AM_PYTHON], [test $tidas_python = yes])

dnl +------------------------------------------------
dnl | Check for HDF5
dnl +------------------------------------------------
AC_LANG_PUSH([C])
tidas_hdf5=yes
AC_ARG_ENABLE(hdf5, [AC_HELP_STRING([--disable-hdf5], [Disable HDF5 backend])], [tidas_hdf5=$enable_hdf5])
if test $tidas_hdf5 = yes; then
  AX_LIB_HDF5([serial])
  if test x"$HDF5_CC" = x; then
    tidas_hdf5=no
    AC_MSG_WARN([Could not find serial HDF5 library- disabling backend])
  fi
fi
AC_LANG_POP([C])
AM_CONDITIONAL([HAVE_AM_HDF5], [test $tidas_hdf5 = yes])

dnl +------------------------------------------------
dnl | Check for GetData
dnl +------------------------------------------------
tidas_getdata=no
#AC_LANG_PUSH([C])
#tidas_getdata=yes
#AC_ARG_ENABLE(getdata, [AC_HELP_STRING([--disable-getdata], [Disable GetData backend])], [tidas_getdata=$enable_getdata])
#if test $tidas_getdata = yes; then
#  ACX_GETDATA([], [tidas_getdata=no])
#  if test $tidas_getdata = no; then
#    AC_MSG_WARN([Could not find GetData library- disabling backend])
#  fi
#fi
#AC_LANG_POP([C])
AM_CONDITIONAL([HAVE_AM_GETDATA], [test $tidas_getdata = yes])

dnl +------------------------------------------------
dnl | Do we have at least one backend?
dnl +------------------------------------------------
if test $tidas_hdf5 = no; then
  if test $tidas_getdata = no; then
    AC_MSG_ERROR([No supported backend formats can be used!])
  fi
fi

dnl +------------------------------------------------
dnl | Set outputs
dnl +------------------------------------------------
AC_CONFIG_FILES([Makefile])
AC_CONFIG_FILES([fixlibtool/Makefile])
AC_CONFIG_FILES([src/Makefile])
AC_CONFIG_FILES([src/libtidas/sqlite/Makefile])
AC_CONFIG_FILES([src/libtidas/core/Makefile])
AC_CONFIG_FILES([src/libtidas/test/Makefile])
AC_CONFIG_FILES([src/libtidas/test/gtest/Makefile])
AC_CONFIG_FILES([src/libtidas/capi/Makefile])
AC_CONFIG_FILES([src/libtidas/Makefile])
AC_CONFIG_FILES([src/libtidas-mpi/Makefile])
AC_CONFIG_FILES([src/tidas/Makefile])
AC_CONFIG_FILES([src/apps/Makefile])
AC_CONFIG_FILES([src/apps-mpi/Makefile])
dnl AC_CONFIG_FILES([docs/Makefile])
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
AC_MSG_NOTICE([  Serial C++ Compiler       : $CXX])
AC_MSG_NOTICE([  C++ Compile Flags         : $CXXFLAGS])
AC_MSG_NOTICE([  Serial C Compiler         : $CC])
AC_MSG_NOTICE([  C Compile Flags           : $CFLAGS])
if test $tidas_fortran = yes; then
  AC_MSG_NOTICE([  Serial F03 Compiler       : $FC])
  AC_MSG_NOTICE([  F03 Compile Flags         : $FCFLAGS])
else
  AC_MSG_NOTICE([  Serial F03 Compiler       : Disabled])
  AC_MSG_NOTICE([  F03 Compile Flags         : Disabled])
fi
if test $tidas_mpi = yes; then
  AC_MSG_NOTICE([  MPI C++ Compiler          : $MPICXX])
  AC_MSG_NOTICE([  MPI C Compiler            : $MPICC])
else
  AC_MSG_NOTICE([  MPI C++ Compiler          : Disabled])
  AC_MSG_NOTICE([  MPI C Compiler            : Disabled])
fi
if test $tidas_mpi_fortran = yes; then
  AC_MSG_NOTICE([  MPI F03 Compiler          : $MPIFC])
else
  AC_MSG_NOTICE([  MPI F03 Compiler          : Disabled])
fi
if test $tidas_python = yes; then
  AC_MSG_NOTICE([  Python                    : $PYTHON])
else
  AC_MSG_NOTICE([  Python                    : Disabled])
fi
if test $tidas_hdf5 = yes; then
  AC_MSG_NOTICE([  HDF5 Compile              : $HDF5_CPPFLAGS])
  AC_MSG_NOTICE([  HDF5 Link                 : $HDF5_LDFLAGS $HDF5_LIBS])
else
  AC_MSG_NOTICE([  HDF5 Compile              : Disabled])
  AC_MSG_NOTICE([  HDF5 Link                 : Disabled])
fi
if test $tidas_hdf5 = yes; then
  AC_MSG_NOTICE([  GetData Compile           : $GETDATA_CPPFLAGS])
  AC_MSG_NOTICE([  GetData Link              : $GETDATA])
else
  AC_MSG_NOTICE([  GetData Compile           : Disabled])
  AC_MSG_NOTICE([  GetData Link              : Disabled])
fi
AC_MSG_NOTICE([                                           ])
