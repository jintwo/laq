#include <ctype.h>
#include <fcntl.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include <avro.h>
#include <luajit.h>
#include <zlib.h>

#define CHUNK 10 * 1024 * 1024

typedef void (*record_func)(avro_value_t *, void *);
typedef void (*reader_func)(const char *, record_func, void *);

int inflate_buf(const char *src, char *dst, size_t len, size_t *out_len);
void read_varint(avro_reader_t reader, int64_t *res);
void push_avro_value(lua_State *L, avro_value_t *value);
void print_field(avro_value_t *value, char *field);
void read_avro_file_custom(const char *filename, record_func callback, void *user_data);
void read_avro_file_default(const char *filename, record_func callback, void *user_data);
