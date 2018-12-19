#!/bin/sh

#################################################################################
##
## How to install CGAL header only library
##
#################################################################################

mkdir -p CGAL-4.13
pd=`pwd -P`
updir=$(dirname $pd)
/bin/rm -rf cgal-releases-CGAL-4.13
tar zxf CGAL-4.13.tar.gz
cd  cgal-releases-CGAL-4.13

mkir -p build/release
cd build/release


cmake -DWITH_LEDA=OFF \
 -DBoost_INCLUDE_DIR=$updir/Boost/boost_1_68_0/include\
 -DBoost_LIBRARY_DIRS=$updir/Boost/boost_1_68_0/lib\
 -DCGAL_DISABLE_GMP=ON \
 -DCMAKE_INSTALL_PREFIX=$pd/CGAL-4.13 -DCMAKE_BUILD_TYPE=Release .

make
make install
cd ..
#/bin/rm -rf cgal-releases-CGAL-4.13
echo "CGAL-4.13/include contains CGAL header files"
