#include <stdio.h>
#include <ctype.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mman.h>

#include <avro.h>
#include <zlib.h>
#include <lua.h>
#include <parson.h>

#define CHUNK 4 * 1024 * 1024

typedef void (*record_cb)(avro_value_t, void*);

static int read_long(avro_reader_t reader, int64_t * l)
{
	uint64_t value = 0;
	uint8_t b;
	int offset = 0;
	do {
		if (offset == 10) {
            return -1;
		}
		avro_read(reader, &b, 1);
		value |= (int64_t) (b & 0x7F) << (7 * offset);
		++offset;
	}
	while (b & 0x80);
	*l = ((value >> 1) ^ -(value & 1));
	return 0;
}

JSON_Value *value_to_json(avro_value_t value) {
    char *strval = NULL;
    avro_value_to_json(&value, 1, &strval);
    JSON_Value *val = json_parse_string(strval);
    free(strval);
    return val;
}

int deflate_(const char *src, const char *dst, size_t *sz) {
    int ret = 0;
    z_stream stream;

    size_t len = strlen(src) + 1;

    stream.zalloc = (voidpf)0;
    stream.zfree = (voidpf)0;
    stream.opaque = (voidpf)0;
    ret = deflateInit(&stream, Z_BEST_SPEED);
    if (ret != Z_OK) {
        return ret;
    }
    stream.avail_in = (uInt)len;
    stream.next_in = (Bytef *)src;

    stream.avail_out = CHUNK;
    stream.next_out = (Bytef *)dst;
    ret = deflate(&stream, Z_FINISH);
    deflateEnd(&stream);

    *sz = stream.total_out;

    return ret;
}

int inflate_(const char *src, char *dst, size_t len, size_t *out_len) {
    int ret = 0;
    z_stream stream;

    stream.zalloc = (voidpf)0;
    stream.zfree = (voidpf)0;
    stream.opaque = (voidpf)0;
    stream.next_in = (Bytef *)src;
    stream.avail_in = (uInt)len;
    inflateInit2(&stream, -15);

    stream.avail_out = CHUNK;
    stream.next_out = (Bytef *)dst;
    ret = inflate(&stream, Z_FINISH);
    inflateEnd(&stream);

    *out_len = stream.total_out;

    return ret;
}

// core

// zerocopy file reader
void read_avro_file2(const char *filename, record_cb cb, void *user_data) {
    struct stat st;
    char sync[16], codec_name[11], magic[4];
    size_t size = 0;
    const void *buf = NULL;
    int fd = open(filename, O_RDONLY);
    fstat(fd, &st);
    size_t fsize = st.st_size;
    char *fdata = (char *)mmap(0, fsize, PROT_READ, MAP_PRIVATE, fd, 0);
    avro_reader_t reader = avro_reader_memory(fdata, fsize);

    // read header
    avro_read(reader, magic, sizeof(magic));
    if (magic[0] != 'O' || magic[1] != 'b' || magic[2] != 'j' || magic[3] != 1) {
        fprintf(stderr, "Error: invalid magic\n");
        exit(1);
    }

    // read meta
    avro_value_t meta;
    avro_schema_t meta_values_schema = avro_schema_bytes();
    avro_schema_t meta_schema = avro_schema_map(meta_values_schema);
    avro_value_iface_t *meta_iface = avro_generic_class_from_schema(meta_schema);
    avro_generic_value_new(meta_iface, &meta);
    avro_value_read(reader, &meta);
    avro_schema_decref(meta_schema);

    // read codec
    avro_value_t codec_val;
    avro_value_get_by_name(&meta, "avro.codec", &codec_val, NULL);
    avro_value_get_type(&codec_val);
    avro_value_get_bytes(&codec_val, &buf, &size);
    memset(codec_name, 0, sizeof(codec_name));
    strncpy(codec_name, (const char *)buf, size < 10 ? size : 10);

    // read schema
    avro_schema_t schema;
    avro_value_t schema_bytes;
    avro_value_get_by_name(&meta, "avro.schema", &schema_bytes, NULL);
    avro_value_get_bytes(&schema_bytes, &buf, &size);
    avro_schema_from_json_length((const char *)buf, size, &schema);

    // free meta
    avro_value_decref(&meta);
    avro_value_iface_decref(meta_iface);

    // read records
    if (strcmp(codec_name, "deflate") == 0) {
        avro_value_t value;
        avro_value_iface_t *record_iface = avro_generic_class_from_schema(schema);
        avro_generic_value_new(record_iface, &value);
        size_t chunks_count = 1;
        char *in = malloc(CHUNK), *out = malloc(CHUNK);
        int64_t blocks, size, blocks_total = 0;
        size_t out_size, out_size_total = 0;
        while (avro_read(reader, sync, sizeof(sync)) == 0) {
            read_long(reader, &blocks);
            read_long(reader, &size);
            avro_read(reader, in, size);
            inflate_(in, out + out_size_total, size, &out_size);
            out_size_total += out_size;
            if (chunks_count * CHUNK - out_size_total < CHUNK / 2) {
                chunks_count += 1;
                out = realloc(out, chunks_count * CHUNK);
            }
            blocks_total += blocks;
        }

        avro_reader_t block_reader = avro_reader_memory(out, out_size_total);
        for (int i = 0; i < blocks_total; i++) {
            avro_value_read(block_reader, &value);
            cb(value, user_data);
        }
        avro_reader_free(block_reader);

        free(in);
        free(out);

        avro_value_decref(&value);
        avro_value_iface_decref(record_iface);
    }

    // free value
    avro_schema_decref(schema);
    avro_reader_free(reader);

    // unmap and close file
    munmap(fdata, fsize);
    close(fd);
}

void read_avro_file(const char *filename, record_cb cb, void *user_data) {
    avro_file_reader_t reader;
    avro_value_iface_t *iface;
    avro_value_t value;
    avro_schema_t schema;

    FILE *fp = fopen(filename, "rb");
    avro_file_reader_fp(fp, filename, 0, &reader);
    schema = avro_file_reader_get_writer_schema(reader);

    avro_writer_t avro_stderr = avro_writer_file(stderr);
    avro_schema_to_json(schema, avro_stderr);
    avro_writer_free(avro_stderr);

    iface = avro_generic_class_from_schema(schema);
    avro_generic_value_new(iface, &value);

    while (avro_file_reader_read_value(reader, &value) == 0) {
        cb(value, user_data);
    }

    avro_file_reader_close(reader);
    avro_value_decref(&value);
    avro_value_iface_decref(iface);
    avro_schema_decref(schema);
    fclose(fp);
}

// callbacks examples
void dump_avro_value(avro_value_t value, void *ignored) {
    char *strval = NULL;
    avro_value_to_json(&value, 0, &strval);
    puts(strval);
    free(strval);
}

void field_printer(avro_value_t record, char *field_name) {
    JSON_Value *val = value_to_json(record);
    JSON_Object *obj = json_value_get_object(val);
    JSON_Value *result = json_object_dotget_value(obj, field_name);
    switch (json_type(result)) {
    case JSONString:
        printf("%s: %s\n", field_name, json_string(result));
        break;
    case JSONObject:
        printf("%s: %s\n", field_name, json_serialize_to_string_pretty(result));
        break;
    case JSONNumber:
        printf("%s: %f\n", field_name, json_number(result));
        break;
    default:
        printf("%s: unknown type\n", field_name);
    }
    json_value_free(val);
}

void lua_exec_script(avro_value_t record, char *script_name) {

}

void lua_exec_inline(avro_value_t record, char *inline_script) {

}

int main(int argc, char **argv) {
    if (argc < 2) {
        puts("invalid filename");
        return 1;
    }

    if (argc < 3) {
        puts("invalid field");
        return 1;
    }

    if (argc < 4) {
        puts("invalid reader");
        return 1;
    }

    char *filename = argv[1];
    char *fieldname = argv[2];
    char *reader = argv[3];

    if (strcmp(reader, "default") == 0) {
        printf("[%s] reading %s(%s)\n", reader, filename, fieldname);
        /* read_avro_file(filename, (record_cb)&dump_avro_value, NULL); */
        read_avro_file(filename, (record_cb)&field_printer, fieldname);
    } else if (strcmp(reader, "zeroc") == 0){
        printf("[%s] reading %s(%s)\n", reader, filename, fieldname);
        /* read_avro_file2(filename, (record_cb)&dump_avro_value, NULL); */
        read_avro_file2(filename, (record_cb)&field_printer, fieldname);
    } else {
        puts("valid readers: default, zeroc");
    }
    return 0;
}
