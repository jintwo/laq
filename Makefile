CC=cc
PARSON=vendor/parson
INC=-Ivendor/avro/lang/c/avrolib/include -I$(PARSON) -Ivendor/LuaJIT-2.0.4/src
LIB=-Lvendor/avro/lang/c/avrolib/lib -Lvendor/LuaJIT-2.0.4/src

build:
	$(CC) main.c $(PARSON)/parson.c -pg -Wall $(INC) $(LIB) -lavro -lz -o laq
