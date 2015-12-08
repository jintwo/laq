CC=cc
PARSON=vendor/parson
INC=-Ivendor/avro/lang/c/avrolib/include -I$(PARSON) -Ivendor/LuaJIT-2.0.4/src
LIB=-Lvendor/avro/lang/c/avrolib/lib -Lvendor/LuaJIT-2.0.4/src
OSX_SPECIFIC=-pagezero_size 10000 -image_base 100000000

build:
	$(CC) main.c $(PARSON)/parson.c -pg $(OSX_SPECIFIC) -Wall $(INC) $(LIB) -lavro -lluajit -lz -o laq
