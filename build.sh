#!/bin/bash

pushd vendor/luajit ; make clean ; make ; popd
pushd vendor/avro/lang/c ; make clean ; rm -rf CMakeCache.txt ; cmake . -DCMAKE_INSTALL_PREFIX=./ ; make install ; popd
pushd vendor/libuv ; sh autogen.sh ; ./configure --prefix=`pwd` ; make install ; popd
rm -rf CMakeCache.txt ; cmake . ; make
