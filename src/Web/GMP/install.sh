#!/bin/sh

pd=`pwd -P`
/bin/mkdir -p GMP-6.1.2

tar xf gmp-6.1.2.tar.gz
cd gmp-6.1.2
./configure --prefix=$pd/GMP-6.1.2
make
make install
