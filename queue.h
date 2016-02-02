#include <avro.h>
#include <uv.h>

typedef struct queue {
    uint16_t pos, size;
    uv_cond_t empty, full;
    uv_mutex_t mutex;
    avro_value_t **data;
} queue_t;

queue_t* queue_new(uint16_t size) {
    queue_t *q = malloc(sizeof(queue_t));
    q->size = size;
    q->pos = 0;
    q->data = calloc(size, sizeof(avro_value_t));
    uv_cond_init(&q->full);
    uv_cond_init(&q->empty);
    uv_mutex_init(&q->mutex);
    return q;
}

void queue_free(queue_t *q) {
    uv_cond_destroy(&q->full);
    uv_cond_destroy(&q->empty);
    uv_mutex_destroy(&q->mutex);
    free(q->data);
    free(q);
}

void queue_put(queue_t *q, avro_value_t *val) {
    uv_mutex_lock(&q->mutex);
    while (q->pos == q->size) {
        uv_cond_wait(&q->full, &q->mutex);
    }
    avro_value_t v;
    memcpy(&v, val, sizeof(avro_value_t));
    uv_mutex_lock(&q->mutex);
    q->data[q->pos++] = &v;
    uv_mutex_unlock(&q->mutex);
    uv_cond_signal(&q->empty);
}

avro_value_t* queue_pop(queue_t *q) {
    uv_mutex_lock(&q->mutex);
    while (q->pos == 0) {
        uv_cond_wait(&q->empty, &q->mutex);
    }
    avro_value_t *res = q->data[q->pos--];
    uv_mutex_unlock(&q->mutex);
    uv_cond_signal(&q->full);
    return res;
}
