#include "utils.h"

int inflate_buf(const char *src, char *dst, size_t len, size_t *out_len) {
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

// avro varint reader
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


// avro value to lua
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
        const void *val = NULL;
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
        lua_pushstring(L, val);
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

// field printer
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
        printf("%g", val);
        break;
    }

    case AVRO_NULL:
    {
        printf("<null>");
        break;
    }

    case AVRO_BYTES:
    {
        const void *val = NULL;
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
                printf("%d: ", i);
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
        print_avro_value(&branch, indent);
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

    char *delim = strchr(field, ':');
    if (!delim) {
        delim = strchr(field, '.');
    }

    size_t field_name_len = strlen(field);

    if (delim) {
        field_name_len = delim - field;
    }

    char *field_name = malloc(field_name_len + 1);
    strncpy(field_name, field, field_name_len);
    field_name[field_name_len] = 0;

    if (avro_value_get_type(record) == AVRO_UNION) {
        avro_value_t branch;
        avro_value_get_current_branch(record, &branch);

        if (avro_value_get_type(&branch) == AVRO_NULL) {
            printf("<null branch>");
            free(field_name);
            free(child);
            return;
        }

        record = &branch;
    }

    size_t size = 0;
    avro_value_get_size(record, &size);
    if (isdigit(*field_name)) {
        int index = atoi(field_name);
        if (index > size - 1) {
            printf("<invalid array index>");
            free(field_name);
            free(child);
            return;
        }
        avro_value_get_by_index(record, index, child, NULL);
    } else {
        const char *f_name = NULL;
        bool found = false;
        for (int i = 0; i < size; i++) {
            avro_value_get_by_index(record, i, child, &f_name);
            if (strcmp(f_name, field_name) == 0) {
                found = true;
                break;
            }
        }

        if (!found) {
            printf("<field not found>");
            free(field_name);
            free(child);
            return;
        }
    }

    if (avro_value_get_type(child) != AVRO_NULL) {
        print_field(child, delim);
    }

    free(field_name);
    free(child);
}

// default avro file reader
void read_avro_file_default(const char *filename, record_func callback, void *user_data) {
    avro_file_reader_t reader;
    avro_value_iface_t *iface;
    avro_schema_t schema;

    FILE *fp = fopen(filename, "rb");
    avro_file_reader_fp(fp, filename, 0, &reader);
    schema = avro_file_reader_get_writer_schema(reader);

    iface = avro_generic_class_from_schema(schema);

    while (1) {
        avro_value_t value;
        avro_generic_value_new(iface, &value);
        int rval = avro_file_reader_read_value(reader, &value);
        if (rval) break;
        callback(&value, user_data);
        avro_value_decref(&value);
    }

    avro_file_reader_close(reader);
    avro_value_iface_decref(iface);
    avro_schema_decref(schema);
    fclose(fp);
}

// custom avro file reader
void read_avro_file_custom(const char *filename, record_func callback, void *user_data) {
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
        fprintf(stderr, "Invalid magic.\n");
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
        size_t chunks_count = 1;
        char *in = malloc(CHUNK), *out = malloc(CHUNK);
        int64_t blocks, size, blocks_total = 0;
        size_t out_size, out_size_total = 0;
        while (avro_read(reader, sync, sizeof(sync)) == 0) {
            read_varint(reader, &blocks);
            read_varint(reader, &size);
            avro_read(reader, in, size);
            inflate_buf(in, out + out_size_total, size, &out_size);
            out_size_total += out_size;
            if (chunks_count * CHUNK - out_size_total < CHUNK / 2) {
                chunks_count += 1;
                out = realloc(out, chunks_count * CHUNK);
            }
            blocks_total += blocks;
        }

        avro_value_iface_t *iface = avro_generic_class_from_schema(schema);
        avro_reader_t block_reader = avro_reader_memory(out, out_size_total);

        for (int i = 0; i < blocks_total; i++) {
            avro_value_t value;
            avro_generic_value_new(iface, &value);
            int rval = avro_value_read(block_reader, &value);
            if (rval) break;
            callback(&value, user_data);
            avro_value_decref(&value);
        }

        avro_reader_free(block_reader);
        avro_value_iface_decref(iface);

        free(in);
        free(out);
    }

    // free value
    avro_schema_decref(schema);
    avro_reader_free(reader);

    // unmap and close file
    munmap(fdata, fsize);
    close(fd);
}
