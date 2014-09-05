#
# SYNOPSIS
#
#   ACX_GETDATA([ACTION-IF-FOUND[, ACTION-IF-NOT-FOUND]])
#
# DESCRIPTION
#
#   This macro looks for a version of the getdata (http://getdata.sourceforge.net)
#   library.  By default, it checks user options and then uses pkg-config.
#   The GETDATA_CPPFLAGS, and GETDATA output variables hold the compile and link flags.
#
#   To link an application with Getdata, you should link with:
#
#   	$GETDATA (serial, C)
#
#   The user may use:
# 
#       --with-getdata=<path>
#
#   to manually specify the location of the getdata install.
#
#   ACTION-IF-FOUND is a list of shell commands to run if the getdata library is
#   found, and ACTION-IF-NOT-FOUND is a list of commands to run it if it is
#   not found. If ACTION-IF-FOUND is not specified, the default action will
#   define HAVE_GETDATA and set output variables above.
#
#   This macro requires autoconf 2.50 or later.
#
# LAST MODIFICATION
#
#   2014-09-04
#
# COPYING
#
#   Copyright (c) 2014 Theodore Kisner <tskisner@lbl.gov>
#
#   All rights reserved.
#
#   Redistribution and use in source and binary forms, with or without modification,
#   are permitted provided that the following conditions are met:
#
#   o  Redistributions of source code must retain the above copyright notice, 
#      this list of conditions and the following disclaimer.
#
#   o  Redistributions in binary form must reproduce the above copyright notice, 
#      this list of conditions and the following disclaimer in the documentation
#      and/or other materials provided with the distribution.
#
#   THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND 
#   ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED 
#   WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. 
#   IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, 
#   INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, 
#   BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, 
#   DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF 
#   LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE 
#   OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED
#   OF THE POSSIBILITY OF SUCH DAMAGE.
#

AC_DEFUN([ACX_GETDATA], [
AC_PREREQ(2.50)

acx_gd_ok=no

GETDATA_CPPFLAGS=""
GETDATA=""

AC_ARG_WITH(getdata, [AC_HELP_STRING([--with-getdata=<PATH>], [use the getdata installed in <PATH>.])])

if test x"$with_getdata" != x; then
   GETDATA_CPPFLAGS="-I$with_getdata/include"
   GETDATA="-L$with_getdata/lib -lgetdata -lbz2 -lz"
else
   # use pkg-config
   PKG_CHECK_EXISTS([getdata],[
      PKG_CHECK_MODULES([GETDATAPKG], [getdata])
      GETDATA_CPPFLAGS="$GETDATAPKG_CFLAGS"
      GETDATA="$GETDATAPKG_LIBS -lbz2 -lz"
   ], [])
fi

# Check for headers and libraries                                                                                    

acx_gd_save_CPP="$CPP"
acx_gd_save_CPPFLAGS="$CPPFLAGS"
acx_gd_save_LIBS="$LIBS"

AC_LANG_PUSH([C])

CPPFLAGS="$CPPFLAGS $GETDATA_CPPFLAGS"
LIBS="$GETDATA $LIBS"

AC_CHECK_HEADERS([getdata.h])

AC_MSG_CHECKING([for gd_open])
AC_TRY_LINK_FUNC(gd_open, [acx_gd_ok=yes;AC_DEFINE(HAVE_GETDATA,1,[Define if you have the C getdata library.])], [])
AC_MSG_RESULT($acx_gd_ok)

AC_LANG_POP([C])

CPP="$acx_gd_save_CPP"
LIBS="$acx_gd_save_LIBS"
CPPFLAGS="$acx_gd_save_CPPFLAGS"

# Define exported variables

if test $acx_gd_ok = no; then
   GETDATA_CPPFLAGS=""
   GETDATA=""
fi

AC_SUBST(GETDATA_CPPFLAGS)
AC_SUBST(GETDATA)

# Execute ACTION-IF-FOUND/ACTION-IF-NOT-FOUND:

if test x"$acx_gd_ok" = xyes; then
   ifelse([$1],,[echo "**** Enabling getdata support."],[$1])
else
   ifelse([$2],,[echo "**** getdata not found - disabling support."],[$2])
fi

])
