#include <glob.h>
#include <stdlib.h>
#include <sys/types.h>
#include <pthread.h>

#include <avro.h>
#include <luajit.h>
#include <lualib.h>
#include <lauxlib.h>
#include <uv.h>

#include "options.h"
#include "utils.h"

/* #include "queue.h" */

#define CB_TYPE_CAT 1
#define CB_TYPE_FIELD_PRINT 2
#define CB_TYPE_LUA 4

#define LUA_CB_TYPE_INLINE 1
#define LUA_CB_TYPE_SCRIPT 2

// default libuv loop
uv_loop_t *loop;

// lua cb data
typedef struct lua_cb_user_data {
    lua_State *L;
    uint8_t cb_ref, type;
    char *inline_script;
    char *script_path;
} lua_cb_user_data_t;

void _init_lua_cb(lua_cb_user_data_t *cb_data) {
    cb_data->L = luaL_newstate();
    luaL_openlibs(cb_data->L);
    luaJIT_setmode(cb_data->L, 0, LUAJIT_MODE_ENGINE | LUAJIT_MODE_ON);
}

void init_lua_cb_script(lua_cb_user_data_t *cb_data, const char *script_path) {
    _init_lua_cb(cb_data);
    cb_data->script_path = strdup(script_path);
    luaL_dofile(cb_data->L, cb_data->script_path);
    cb_data->cb_ref = luaL_ref(cb_data->L, LUA_REGISTRYINDEX);
    cb_data->type = LUA_CB_TYPE_SCRIPT;
}

void init_lua_cb_inline(lua_cb_user_data_t *cb_data, const char *inline_script) {
    _init_lua_cb(cb_data);
    cb_data->inline_script = strdup(inline_script);
    cb_data->type = LUA_CB_TYPE_INLINE;
}

void free_lua_cb(lua_cb_user_data_t *cb_data) {
    lua_close(cb_data->L);
    switch (cb_data->type) {
    case LUA_CB_TYPE_INLINE:
        free(cb_data->inline_script);
        break;
    case LUA_CB_TYPE_SCRIPT:
        free(cb_data->script_path);
        break;
    }
}

// worker utils
typedef struct worker_data {
    uint8_t cb_type;
    uv_work_t req;
    record_func callback;
    void *user_data;
    avro_value_t *value;
} worker_data_t;

void cleanup_job(uv_work_t *req, int status) {
    worker_data_t *job = (worker_data_t *)req->data;
    if (job->cb_type == CB_TYPE_LUA) {
        free_lua_cb(job->user_data);
    }
    avro_value_decref(job->value);
    free(job->value);
    free(job->user_data);
    free(job);
}

void execute_job(uv_work_t *req) {
    worker_data_t *data = (worker_data_t *)req->data;
    data->callback(data->value, data->user_data);
}

void enqueue_job(worker_data_t *job) {
    uv_queue_work(uv_default_loop(), &job->req, execute_job, cleanup_job);
}

// callbacks
void dump_avro_value(avro_value_t *value, void *reserved) {
    char *strval = NULL;
    avro_value_to_json(value, 0, &strval);
    puts(strval);
    free(strval);
}

void dump_avro_value_mt(avro_value_t *value, void *reserved) {
    if (value->iface == NULL) {
        fprintf(stderr, "Invalid avro value.\n");
        return;
    }

    // init job
    worker_data_t *job = malloc(sizeof(worker_data_t));
    job->cb_type = CB_TYPE_CAT;
    job->callback = dump_avro_value;

    // set avro value
    job->value = malloc(sizeof(avro_value_t));
    avro_generic_value_new(value->iface, job->value);
    avro_value_copy_fast(job->value, value);

    // set uv req data
    job->req.data = job;
    enqueue_job(job);
}

void field_printer(avro_value_t *value, char *field_names) {
    char *field = strtok(field_names, ",");
    while (field) {
        print_field(value, field);
        printf("\t");
        field = strtok(NULL, ",");
    }
    printf("\n");
}

void field_printer_mt(avro_value_t *value, char *field_names) {
    if (value->iface == NULL) {
        fprintf(stderr, "Invalid avro value.\n");
        return;
    }

    // init job
    worker_data_t *job = malloc(sizeof(worker_data_t));
    job->cb_type = CB_TYPE_FIELD_PRINT;
    job->callback = (record_func)field_printer;

    // set user data
    // TODO: cleanup
    job->user_data = strdup(field_names);

    // set avro value
    job->value = malloc(sizeof(avro_value_t));
    avro_generic_value_new(value->iface, job->value);
    avro_value_copy_fast(job->value, value);

    // set uv req data
    job->req.data = job;
    enqueue_job(job);
}


void lua_inline_handler(avro_value_t *record, lua_State *L, char *script) {
    push_avro_value(L, record);
    lua_setglobal(L, "r");
    luaL_dostring(L, script);
}

void lua_script_handler(avro_value_t *record, lua_State *L, uint8_t cb_ref) {
    lua_rawgeti(L, LUA_REGISTRYINDEX, cb_ref);
    push_avro_value(L, record);
    lua_call(L, 1, 0);
}

void lua_script_wrapper(avro_value_t *record, lua_cb_user_data_t *lua_cb_data) {
    switch (lua_cb_data->type) {
    case LUA_CB_TYPE_INLINE:
        lua_inline_handler(record, lua_cb_data->L, lua_cb_data->inline_script);
        break;
    case LUA_CB_TYPE_SCRIPT:
        lua_script_handler(record, lua_cb_data->L, lua_cb_data->cb_ref);
        break;
    default:
        fprintf(stderr, "Invalid LUA handler type.\n");
    }
}
// end of callbacks

typedef struct read_file_callback {
    char *input;
    record_func callback;
    void *user_data;
} read_file_callback_t;

void read_file_with_callback(char *input, record_func callback, void *user_data) {
    glob_t glob_results;
    glob(input, GLOB_TILDE, NULL, &glob_results);

    for (int i = 0; i < glob_results.gl_pathc; ++i) {
        char *path = glob_results.gl_pathv[i];
        printf("--- [%d] %s ---\n", i, path);
        /* read_avro_file_custom(path, callback, user_data); */
        read_avro_file_default(path, callback, user_data);
    }
}

void read_file_with_callback_wrapper(read_file_callback_t *cb_data) {
    read_file_with_callback(cb_data->input, cb_data->callback, cb_data->user_data);
}

int main(int argc, char **argv) {
    loop = uv_default_loop();

    options_t *options = new_options();
    if (parse_opts(argc, argv, options) != 0) {
        free_options(options);
        return 1;
    }

    if (!options->input) {
        fprintf(stderr, "Invalid avro filename.\n");
        free_options(options);
        return 1;
    }

    if (!options->handler) {
        fprintf(stderr, "Invalid handler.\n");
        free_options(options);
        return 1;
    }

    /* record_func callback; */
    /* if (strcmp(options->handler, "cat") == 0) { */
    /*     callback.cb_type = CB_TYPE_CAT; */
    /*     callback.handler = &dump_avro_value; */
    /* } else if (strcmp(options->handler, "field_print") == 0) { */
    /*     callback.cb_type = CB_TYPE_FIELD_PRINT; */
    /*     callback.handler = &field_printer; */
    /*     callback.user_data = options->param; */
    /* } else { */
    /*     callback.handler = &lua_script; */
    /*     if (strcmp(options->handler, "lua_inline") == 0) { */
    /*         callback.cb_type = CB_TYPE_LUA_INLINE; */
    /*     } else { */
    /*         callback.cb_type = CB_TYPE_LUA_SCRIPT; */
    /*         if (access(options->param, F_OK) == -1) { */
    /*             puts("invalid lua script file."); */
    /*             uv_loop_close(loop); */
    /*             free(loop); */
    /*             free_options(options); */
    /*             return 1; */
    /*         } */
    /*     } */
    /* } */

    // start event loop
    uv_run(loop, UV_RUN_DEFAULT);

    /* lua_cb_user_data_t lua_cb_data; */
    /* init_lua_cb_inline(&lua_cb_data, options->param); */

    read_file_callback_t cb_data = {
        .input = options->input,
        /* .callback = (record_func)lua_script_wrapper, */
        /* .callback = (record_func)dump_avro_value_mt, */
        .callback = (record_func)field_printer_mt,
        .user_data = options->param
        /* .user_data = &lua_cb_data */
    };

    uv_thread_t reader;
    uv_thread_create(&reader, (uv_thread_cb)read_file_with_callback_wrapper, &cb_data);
    uv_thread_join(&reader);

    free_options(options);
    return 0;
}
