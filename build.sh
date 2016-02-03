#!/bin/bash

pushd vendor
rm -rf *
wget http://luajit.org/download/LuaJIT-2.0.4.tar.gz
tar zxvf LuaJIT-2.0.4.tar.gz
rm LuaJIT-2.0.4.tar.gz
git clone https://github.com/apache/avro.git
git clone https://github.com/kgabis/parson.git
git clone https://github.com/libuv/libuv.git
popd
pushd vendor/LuaJIT-2.0.4 ; make clean ; make ; popd
pushd vendor/avro/lang/c ; make clean ; rm -rf CMakeCache.txt ; cmake . -DCMAKE_INSTALL_PREFIX=./ ; make install ; popd
pushd vendor/libuv/ ; sh autogen.sh ; ./configure --prefix=`pwd` ; make install ; popd
rm -rf CMakeCache.txt ; cmake . ; make
