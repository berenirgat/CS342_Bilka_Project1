#define _GNU_SOURCE
#include <pthread.h>
#include "common.h"

typedef struct {
    int file_idx;
    int K;
    char filename[256];
    WordMap local;
} ThreadCtx;

static void* worker(void *arg) {
    ThreadCtx *t = (ThreadCtx*)arg;
    wm_init(&t->local);

    FILE *fp = fopen(t->filename, "r");
    if (!fp) die("fopen thread");

    char *line = NULL;
    size_t cap = 0;
    int line_no = 0;

    while (getline(&line, &cap, fp) != -1) {
        line_no++;

        const char *p = line;
        while (*p) {
            while (*p && isspace((unsigned char)*p)) p++;
            if (!*p) break;
            const char *s = p;
            while (*p && !isspace((unsigned char)*p)) p++;
            size_t len = (size_t)(p - s);
            if ((int)len >= t->K) {
                char word[WORD_MAX];
                size_t cpy = len;
                if (cpy >= WORD_MAX) cpy = WORD_MAX - 1;
                memcpy(word, s, cpy);
                word[cpy] = '\0';

                WordEntry *e = wm_get_or_add(&t->local, word);
                occ_push(e, t->file_idx, line_no);
            }
        }
    }

    free(line);
    fclose(fp);
    return NULL;
}

static int cmp_wordentry(const void *a, const void *b) {
    const WordEntry *x = (const WordEntry*)a;
    const WordEntry *y = (const WordEntry*)b;
    return strcmp(x->word, y->word);
}
static int cmp_occ(const void *a, const void *b) {
    const Occ *x = (const Occ*)a;
    const Occ *y = (const Occ*)b;
    if (x->file_idx != y->file_idx) return x->file_idx - y->file_idx;
    return x->line_no - y->line_no;
}

static void merge_into(WordMap *global, WordMap *local) {
    for (size_t i = 0; i < local->sz; i++) {
        WordEntry *le = &local->arr[i];
        WordEntry *ge = wm_get_or_add(global, le->word);
        for (size_t j = 0; j < le->occ_sz; j++) {
            occ_push(ge, le->occ[j].file_idx, le->occ[j].line_no);
        }
    }
}

static void write_output(const char *outname, WordMap *m) {
    qsort(m->arr, m->sz, sizeof(WordEntry), cmp_wordentry);
    for (size_t i = 0; i < m->sz; i++) {
        qsort(m->arr[i].occ, m->arr[i].occ_sz, sizeof(Occ), cmp_occ);
    }
    FILE *out = fopen(outname, "w");
    if (!out) die("fopen out");

    for (size_t i = 0; i < m->sz; i++) {
        WordEntry *e = &m->arr[i];
        fprintf(out, "%s (count=%zu): ", e->word, e->occ_sz);
        for (size_t j = 0; j < e->occ_sz; j++) {
            fprintf(out, "%d-%d", e->occ[j].file_idx, e->occ[j].line_no);
            if (j + 1 < e->occ_sz) fprintf(out, ", ");
        }
        fprintf(out, "\n");
    }
    fclose(out);
}

int main(int argc, char **argv) {
    if (argc != 5) {
        fprintf(stderr, "Usage: %s FilePrefix N K OutFilename\n", argv[0]);
        return 1;
    }

    const char *prefix = argv[1];
    int N = atoi(argv[2]);
    int K = atoi(argv[3]);
    const char *outname = argv[4];

    if (N < 1 || N > 10 || K < 1 || K > 100) {
        fprintf(stderr, "Invalid args\n");
        return 1;
    }

    pthread_t th[10];
    ThreadCtx ctx[10];

    for (int i = 0; i < N; i++) {
        ctx[i].file_idx = i + 1;
        ctx[i].K = K;
        snprintf(ctx[i].filename, sizeof(ctx[i].filename), "%s%d", prefix, i + 1);
        if (pthread_create(&th[i], NULL, worker, &ctx[i]) != 0) die("pthread_create");
    }

    for (int i = 0; i < N; i++) {
        pthread_join(th[i], NULL);
    }

    WordMap global; wm_init(&global);
    for (int i = 0; i < N; i++) {
        merge_into(&global, &ctx[i].local);
        wm_free(&ctx[i].local);
    }

    write_output(outname, &global);
    wm_free(&global);
    return 0;
}