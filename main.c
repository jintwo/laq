#include <stdio.h>
#include <getopt.h>
#include <glob.h>
#include <ctype.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <limits.h>

#include <avro.h>
#include <zlib.h>
#include <luajit.h>
#include <lualib.h>
#include <lauxlib.h>

#ifdef WITH_PARSON
#include <parson.h>
#else
#include <jansson.h>
#endif

#define CHUNK 10 * 1024 * 1024

#define LUA_CB_TYPE_INLINE 0
#define LUA_CB_TYPE_SCRIPT 1

typedef void (*record_cb)(avro_value_t, void*);

typedef struct {
    lua_State *L;
    uint8_t type, cb_ref;
    char *inline_script;
    char *script_path;
} lua_callback;

typedef struct {
    char *input, *handler, *param;
    int count;
} options;

options* new_options() {
    options *opts = malloc(sizeof(options));
    opts->input = NULL;
    opts->handler = NULL;
    opts->param = NULL;
    opts->count = INT_MAX;
    return opts;
}

void free_options(options *opts) {
    free(opts->input);
    free(opts->handler);
    free(opts->param);
}

void read_varint(avro_reader_t reader, int64_t *res)
{
    uint64_t value = 0;
    uint8_t b;
    int offset = 0;
    do {
        avro_read(reader, &b, 1);
        value |= (int64_t) (b & 0x7F) << (7 * offset);
        ++offset;
    }
    while (b & 0x80);
    *res = ((value >> 1) ^ -(value & 1));
}

#ifdef WITH_PARSON
JSON_Value *value_to_json(avro_value_t value) {
    char *strval = NULL;
    avro_value_to_json(&value, 1, &strval);
    JSON_Value *val = json_parse_string(strval);
    free(strval);
    return val;
}
#else
json_t *value_to_json(avro_value_t value) {
    char *strval = NULL;
    avro_value_to_json(&value, 1, &strval);
    json_error_t err;
    json_t *val = json_loads(strval, 0, &err);
    free(strval);
    return val;
}
#endif

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

// custom file reader
void read_avro_file2(const char *filename, record_cb cb, void *user_data, int count) {
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
            read_varint(reader, &blocks);
            read_varint(reader, &size);
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
            if (--count <= 0) {
                break;
            }
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

void print_indent(int indent) {
    for (int i = 0; i < indent; i++) {
        printf(" ");
    }
}

void print_avro_value(avro_value_t *value, int indent) {
    switch (avro_value_get_type(value)) {
    case AVRO_BOOLEAN:
    {
        int val = 0;
        avro_value_get_boolean(value, &val);
        printf(val ? "true" : "false");
        break;
    }

    case AVRO_INT64:
    case AVRO_INT32:
    case AVRO_FLOAT:
    case AVRO_DOUBLE:
    {
        double val = 0;
        avro_value_get_double(value, &val);
        printf("%g");
        break;
    }

    case AVRO_NULL:
    {
        printf("null");
        break;
    }

    case AVRO_BYTES:
    {
        const char *val = NULL;
        size_t size = 0;
        avro_value_get_bytes(value, &val, &size);
        printf("%s", val);
        break;
    }

    case AVRO_STRING:
    {
        const char *val = NULL;
        size_t size = 0;
        avro_value_get_string(value, &val, &size);
        printf("%s", val);
        break;
    }

    case AVRO_ENUM:
    case AVRO_FIXED:
    {
        printf("unsupported type");
        break;
    }

    case AVRO_ARRAY:
    case AVRO_MAP:
    case AVRO_RECORD:
    {
        size_t field_count = 0;
        avro_value_get_size(value, &field_count);

        printf("{\n");
        for (int i = 0; i < field_count; i++) {
            const char *field_name = NULL;
            avro_value_t field;
            avro_value_get_by_index(value, i, &field, &field_name);
            print_indent(indent + 1);
            if (!field_name) {
                printf("%g: ", i);
            } else {
                printf("%s: ", field_name);
            }
            print_avro_value(&field, indent + 1);
            printf("\n");
        }
        print_indent(indent);
        printf("}");
        break;
    }
    case AVRO_UNION:
    {
        avro_value_t branch;
        avro_value_get_current_branch(value, &branch);
        if (avro_value_get_type(&branch) == AVRO_NULL) {
            printf("null");
        } else {
            print_avro_value(&branch, indent);
        }
        break;
    }
    }
}

void print_field(avro_value_t *record, char *field) {
    if (!record) {
        return;
    }

    if (!field || !strlen(field)) {
        print_avro_value(record, 0);
        return;
    }

    if (*field == ':' || *field == '.') {
        field++;
    }

    avro_value_t *child = malloc(sizeof(avro_value_t*));
    char *field_name;
    size_t field_name_len = strlen(field);

    char *delim = strchr(field, ':');
    if (!delim) {
        delim = strchr(field, '.');
    }

    if (delim) {
        field_name_len = delim - field;
    }

    field_name = malloc(field_name_len + 1);
    strncpy(field_name, field, field_name_len);
    field_name[field_name_len] = 0;

    if (avro_value_get_type(record) == AVRO_UNION) {
        avro_value_t branch;
        avro_value_get_current_branch(record, &branch);

        if (avro_value_get_type(&branch) == NULL) {
            printf("null branch\n");
            free(field_name);
            free(child);
            return;
        }

        record = &branch;
    }

    if (isdigit(*field_name)) {
        size_t size = 0;
        avro_value_get_size(record, &size);
        int index = atoi(field_name);

        if (index > size - 1) {
            printf("invalid array index\n");
            free(field_name);
            free(child);
            return;
        }

        avro_value_get_by_index(record, index, child, NULL);
    } else {
        avro_value_get_by_name(record, field_name, child, NULL);
    }

    if (avro_value_get_type(child) != AVRO_NULL) {
       print_field(child, delim);
    }

    free(field_name);
    free(child);
}

void field_printer(avro_value_t record, char *field_names) {
    char *field = strtok(field_names, ",");
    while (field) {
        print_field(&record, field);
        printf("\t");
        field = strtok(NULL, ",");
    }
    printf("\n");
}

void push_avro_value(lua_State *L, avro_value_t *value) {
    switch (avro_value_get_type(value)) {
    case AVRO_BOOLEAN:
    {
        int val = 0;
        avro_value_get_boolean(value, &val);
        lua_pushboolean(L, val);
        break;
    }

    case AVRO_DOUBLE:
    {
        double val = 0;
        avro_value_get_double(value, &val);
        lua_pushnumber(L, val);
        break;
    }

    case AVRO_FLOAT:
    {
        float val = 0;
        avro_value_get_float(value, &val);
        lua_pushnumber(L, val);
        break;
    }

    case AVRO_INT32:
    {
        int32_t val = 0;
        avro_value_get_int(value, &val);
        lua_pushnumber(L, val);
        break;
    }

    case AVRO_INT64:
    {
        int64_t val = 0;
        avro_value_get_long(value, &val);
        lua_pushnumber(L, val);
        break;
    }

    case AVRO_NULL:
    {
        lua_pushnil(L);
        break;
    }

    case AVRO_BYTES:
    {
        const char *val = NULL;
        size_t size = 0;
        avro_value_get_bytes(value, &val, &size);
        lua_pushlstring(L, val, size);
        break;
    }

    case AVRO_STRING:
    {
        const char *val = NULL;
        size_t size = 0;
        avro_value_get_string(value, &val, &size);
        lua_pushlstring(L, val, size);
        break;
    }

    case AVRO_ENUM:
    case AVRO_FIXED:
    {
        lua_pushstring(L, "unsupported type");
        break;
    }

    case AVRO_MAP:
    case AVRO_ARRAY:
    case AVRO_RECORD:
    {
        size_t field_count = 0;
        avro_value_get_size(value, &field_count);

        lua_newtable(L);
        for (int i = 0; i < field_count; i++) {
            const char *field_name = NULL;
            avro_value_t field;
            avro_value_get_by_index(value, i, &field, &field_name);
            if (!field_name) {
                lua_pushnumber(L, i);
            } else {
                lua_pushstring(L, field_name);
            }
            push_avro_value(L, &field);
            lua_settable(L, -3);
        }
        break;
    }
    case AVRO_UNION:
    {
        avro_value_t branch;
        avro_value_get_current_branch(value, &branch);
        if (avro_value_get_type(&branch) == AVRO_NULL) {
            lua_pushnil(L);
        } else {
            push_avro_value(L, &branch);
        }
        break;
    }
    }
}

void lua_script(avro_value_t record, lua_callback *cb_data) {
    if (cb_data->type == LUA_CB_TYPE_INLINE) {
        push_avro_value(cb_data->L, &record);
        lua_setglobal(cb_data->L, "r");
        luaL_dostring(cb_data->L, cb_data->inline_script);
    } else if (cb_data->type == LUA_CB_TYPE_SCRIPT) {
        lua_rawgeti(cb_data->L, LUA_REGISTRYINDEX, cb_data->cb_ref);
        push_avro_value(cb_data->L, &record);
        lua_call(cb_data->L, 1, 0);
    }
}

void _init_lua_cb(lua_callback *cb_data) {
    cb_data->L = luaL_newstate();
    luaL_openlibs(cb_data->L);
    luaJIT_setmode(cb_data->L, 0, LUAJIT_MODE_ENGINE | LUAJIT_MODE_ON);
}

void init_lua_cb_script(lua_callback *cb_data, const char *script_path) {
    _init_lua_cb(cb_data);
    cb_data->type = LUA_CB_TYPE_SCRIPT;
    cb_data->script_path = strdup(script_path);
    luaL_dofile(cb_data->L, cb_data->script_path);
    cb_data->cb_ref = luaL_ref(cb_data->L, LUA_REGISTRYINDEX);
}

void init_lua_cb_inline(lua_callback *cb_data, const char *inline_script) {
    _init_lua_cb(cb_data);
    cb_data->type = LUA_CB_TYPE_INLINE;
    cb_data->inline_script = strdup(inline_script);
}

void free_lua_cb(lua_callback *cb_data) {
    lua_close(cb_data->L);
    if (cb_data->type == LUA_CB_TYPE_SCRIPT) free(cb_data->script_path);
    else if (cb_data->type == LUA_CB_TYPE_INLINE) free(cb_data->inline_script);
}

int parse_opts(int argc, char **argv, options *opts) {
    int c = 0;
    while (1) {
        static struct option long_options[] = {
            {"input", required_argument, 0, 'i'},
            {"handler", required_argument, 0, 'c'},
            {"param", required_argument, 0, 'p'},
            {"count", required_argument, 0, 'n'},
            {0, 0, 0, 0}
        };

        int opt_index = 0;
        c = getopt_long(argc, argv, "i:c:p:n:", long_options, &opt_index);

        if (c == -1)
            break;

        switch (c) {
        case 'i':
            opts->input = strdup(optarg);
            break;
        case 'c':
            opts->handler = strdup(optarg);
            break;
        case 'p':
            opts->param = strdup(optarg);
            break;
        case 'n':
            opts->count = atoi(optarg);
            break;
        default:
            printf(
                "usage: %s\
\n\t-i AVRO_FILE\
\n\t-c [lua_inline|lua_script|field_print|cat]\
\n\t[-p HANDLER_PARAM]\
\n\t[-n RECORDS_COUNT]\n", argv[0]);

            return 1;
        }
    }

    return 0;
}

int main(int argc, char **argv) {
    options *opts = new_options();

    if (parse_opts(argc, argv, opts) != 0) {
        return 1;
    }

    if (!opts->input) {
        puts("invalid avro filename.");
        return 1;
    }

    if (!opts->handler) {
        puts("invalid handler.");
        return 1;
    }

    if (strcmp(opts->handler, "cat") == 0) {
        read_avro_file2(opts->input, &dump_avro_value, NULL, opts->count);
        return 0;
    }

    if (!opts->param) {
        puts("invalid handler param.");
        return 1;
    }

    if (strcmp(opts->handler, "field_print") == 0) {
        read_avro_file2(opts->input, &field_printer, opts->param, opts->count);
        return 0;
    }

    lua_callback cb;
    if (strcmp(opts->handler, "lua_inline") == 0) {
        init_lua_cb_inline(&cb, opts->param);
    } else if (strcmp(opts->handler, "lua_script") == 0) {
        if (access(opts->param, F_OK) == -1) {
            puts("invalid lua script file.");
            return 1;
        }
        init_lua_cb_script(&cb, opts->param);
    }

    glob_t glob_results;
    glob(opts->input, GLOB_NOCHECK, 0, &glob_results);

    for (char **p = glob_results.gl_pathv, c = glob_results.gl_pathc; c; p++, c--) {
        read_avro_file2(*p, &lua_script, &cb, opts->count);
    }

    free_lua_cb(&cb);
    free_options(opts);
    return 0;
}
