.. _install:

Installation
====================

TIDAS requires a C++11 compiler for building.  The only currently implemented "backend" storage format is HDF5, so installation also requires the serial HDF5 library.  Optional dependencies are an MPI C++ compiler compatible with the serial compiler and a modern python version.


Compiled Dependencies
--------------------------

The serial HDF5 library can be built from source (see `the downloads here <https://support.hdfgroup.org/HDF5/>`_).  However, most Linux distributions and OS X package managers (MACports, Homebrew, etc) have packages for HDF5 and that is probably the easiest way to install it.


Python Dependencies
------------------------

To use the python bindings you should have a reasonably new version of python (>= 2.7 or >= 3.4).  We also require
several common python packages:

    * numpy
    * mpi4py (>= 2.0.0, only if using the MPI interface)

For mpi4py, ensure that this package is compatible with the MPI C++ compiler
used during TIDAS installation.  There are obviously several ways to meet these python requirements.  Your OS package manager may already provide these, or you might use an external Python distribution (e.g. Anaconda).


Using Configure
-----------------------

TIDAS uses autotools to configure, build, and install both the compiled code
and the python tools.  If you are running from a git checkout (instead of a
distribution tarball), then first do::

    %> ./autogen.sh

Now run configure::

    %> ./configure --prefix=/path/to/install

**NOTE**: if you have both a "python" and a "python3" executable (and they are different), and you want to use python3, be sure to specify the python to use at configure time::

    %> PYTHON=/usr/bin/python3 ./configure --prefix=/path/to/install

Run configure with the "--help" option to see all available options.  See the top-level "platforms" directory for other examples of running the
configure script.  Now build and install the tools::

    %> make
    %> make install

In order to use the installed tools, you must make sure that the installed
location has been added to the search paths for your shell.  For example,
the "<prefix>/bin" directory should be in your PATH and the python install
location "<prefix>/lib/pythonX.X/site-packages" should be in your PYTHONPATH.


Testing the Installation
-----------------------------

After installation, you can test the serial compiled software with::

    %> tidas_test

and can test the MPI compiled library with (your command for launching MPI programs may be different)::

    %> mpirun -np 4 tidas_test_mpi

You can test the Python bindings in a similar way::

    %> python -c "import tidas; tidas.test()"

and::

    %> mpirun -np 4 python -c "import tidas; tidas.test_mpi()"

Some of the above commands will create temporary output directories in your current working directory.

