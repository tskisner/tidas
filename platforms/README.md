## Example Build Configuration

First, make a build directory:

    $> cd tidas
    $> mkdir build

Then go into that build directory and run one of the scripts in this directory,
optionally setting things like the install prefix:

    $> cd build
    $> ../platforms/<script name>.sh \
       -DCMAKE_INSTALL_PREFIX=$HOME/software/tidas

Then build and install the software:

    $> make -j 4
    $> make install

Finally, to use the installed software, be sure to export these tools into your
search paths:

    $> export PATH=${HOME}/software/tidas/bin:${PATH}
    $> export \
        PYTHONPATH=${HOME}/software/tidas/lib/python3.6/site-packages:${PYTHONPATH}
