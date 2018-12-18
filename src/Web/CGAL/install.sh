#!/bin/sh

#################################################################################
##
## How to install CGAL header only library
##
#################################################################################

mkdir -p CGAL-4.13
pd=`pwd -P`
updir=$(dirname $pd)
tar zxf CGAL-4.13.tar.gz
cd  cgal-releases-CGAL-4.13

cmake -DCGAL_HEADER_ONLY=ON -DGMP_DIR=$updir/GMP/GMP-6.1.2  -DWITH_LEDA=no \
 -DCMAKE_INSTALL_PREFIX=$pd/CGAL-4.13 -DCMAKE_BUILD_TYPE=Release .

make install
cd ..
#/bin/rm -rf cgal-releases-CGAL-4.13
echo "CGAL-4.13/include contains CGAL header files"
