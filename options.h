#include <stdio.h>
#include <stdlib.h>
#include <limits.h>
#include <getopt.h>

typedef struct options {
    char *input, *handler, *param;
    int count, thread_count;
} options_t;

options_t* new_options() {
    options_t *opts = malloc(sizeof(options_t));
    opts->input = NULL;
    opts->handler = NULL;
    opts->param = NULL;
    opts->count = INT_MAX;
    opts->thread_count = 1;
    return opts;
}

void free_options(options_t *opts) {
    free(opts->input);
    free(opts->handler);
    free(opts->param);
}

int parse_opts(int argc, char **argv, options_t *opts) {
    int c = 0;
    while (1) {
        static struct option long_options[] = {
            {"input", required_argument, 0, 'i'},
            {"handler", required_argument, 0, 'c'},
            {"param", required_argument, 0, 'p'},
            {"count", required_argument, 0, 'n'},
            {"threads", required_argument, 0, 'j'},
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
