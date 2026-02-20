#define _GNU_SOURCE
#include <unistd.h>
#include <fcntl.h>
#include <sys/wait.h>
#include <sys/select.h>
#include <errno.h>
#include "common.h"

typedef struct {
    int file_idx;
    int K;
    const char *filename;
    int write_fd;
    int DataLen;
    WordMap map;
} ChildCtx;

static void add_token_cb(const char *tok, void *vctx) {
    ChildCtx *c = (ChildCtx*)vctx;
    if ((int)strlen(tok) >= c->K) {
        (void)c;
    }
}

static void write_all_chunked(int fd, const char *buf, size_t n, int DataLen) {
    size_t off = 0;
    while (off < n) {
        size_t chunk = n - off;
        if (chunk > (size_t)DataLen) chunk = (size_t)DataLen;
        ssize_t w = write(fd, buf + off, chunk);
        if (w < 0) {
            if (errno == EINTR) continue;
            die("write");
        }
        off += (size_t)w;
    }
}

static void child_run(int file_idx, const char *filename, int K, int write_fd, int DataLen) {
    FILE *fp = fopen(filename, "r");
    if (!fp) die("fopen child");

    WordMap m; wm_init(&m);

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
            if (len == 0) continue;

            if ((int)len >= K) {
                char word[WORD_MAX];
                size_t cpy = len;
                if (cpy >= WORD_MAX) cpy = WORD_MAX - 1;
                memcpy(word, s, cpy);
                word[cpy] = '\0';

                WordEntry *e = wm_get_or_add(&m, word);
                occ_push(e, file_idx, line_no); 
            }
        }
    }

    free(line);
    fclose(fp);

    for (size_t i = 0; i < m.sz; i++) {
        WordEntry *e = &m.arr[i];
        for (size_t j = 0; j < e->occ_sz; j++) {
            char rec[WORD_MAX + 64];
            int n = snprintf(rec, sizeof(rec), "%s\t%d\t%d\n", e->word, e->occ[j].file_idx, e->occ[j].line_no);
            if (n < 0) die("snprintf");
            write_all_chunked(write_fd, rec, (size_t)n, DataLen);
        }
    }

    wm_free(&m);
    close(write_fd);
    _exit(0);
}

typedef struct {
    int fd;
    int file_done;
    char *buf;
    size_t buf_sz;
    size_t buf_cap;
} PipeState;

static void ps_init(PipeState *ps, int fd) {
    ps->fd = fd;
    ps->file_done = 0;
    ps->buf = NULL;
    ps->buf_sz = 0;
    ps->buf_cap = 0;
}

static void ps_free(PipeState *ps) {
    free(ps->buf);
    ps->buf = NULL;
    ps->buf_sz = ps->buf_cap = 0;
}

static void buf_append(PipeState *ps, const char *data, size_t n) {
    if (ps->buf_sz + n + 1 > ps->buf_cap) {
        size_t nc = ps->buf_cap ? ps->buf_cap * 2 : 1024;
        while (nc < ps->buf_sz + n + 1) nc *= 2;
        char *nb = (char*)realloc(ps->buf, nc);
        if (!nb) die("realloc parent buf");
        ps->buf = nb;
        ps->buf_cap = nc;
    }
    memcpy(ps->buf + ps->buf_sz, data, n);
    ps->buf_sz += n;
    ps->buf[ps->buf_sz] = '\0';
}

static void parent_consume_lines(PipeState *ps, WordMap *global) {
    char *start = ps->buf;
    while (1) {
        char *nl = memchr(start, '\n', (ps->buf + ps->buf_sz) - start);
        if (!nl) break;
        *nl = '\0';
        char *w = start;
        char *t1 = strchr(w, '\t');
        char *t2 = t1 ? strchr(t1 + 1, '\t') : NULL;
        if (t1 && t2) {
            *t1 = '\0'; *t2 = '\0';
            int file_idx = atoi(t1 + 1);
            int line_no  = atoi(t2 + 1);
            if (w[0] != '\0') {
                WordEntry *e = wm_get_or_add(global, w);
                occ_push(e, file_idx, line_no);
            }
        }

        start = nl + 1;
    }
    size_t rem = (ps->buf + ps->buf_sz) - start;
    memmove(ps->buf, start, rem);
    ps->buf_sz = rem;
    ps->buf[ps->buf_sz] = '\0';
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
    if (argc != 6) {
        fprintf(stderr, "Usage: %s FilePrefix N K DataLen OutFilename\n", argv[0]);
        return 1;
    }

    const char *prefix = argv[1];
    int N = atoi(argv[2]);
    int K = atoi(argv[3]);
    int DataLen = atoi(argv[4]);
    const char *outname = argv[5];

    if (N < 1 || N > 10 || K < 1 || K > 100 || DataLen < 1 || DataLen > 1000) {
        fprintf(stderr, "Invalid args (check constraints)\n");
        return 1;
    }

    int pipes[10][2];
    pid_t pids[10];

    for (int i = 0; i < N; i++) {
        if (pipe(pipes[i]) < 0) die("pipe");

        pid_t pid = fork();
        if (pid < 0) die("fork");

        if (pid == 0) {
            close(pipes[i][0]); 
            for (int j = 0; j < i; j++) {
                close(pipes[j][0]);
                close(pipes[j][1]);
            }
            char *fname = NULL;
            if (asprintf(&fname, "%s%d", prefix, i + 1) < 0) die("asprintf");
            child_run(i + 1, fname, K, pipes[i][1], DataLen);
            free(fname);
            _exit(0);
        } else {
            pids[i] = pid;
            close(pipes[i][1]); 
        }
    }

    PipeState ps[10];
    for (int i = 0; i < N; i++) ps_init(&ps[i], pipes[i][0]);

    WordMap global; wm_init(&global);
    int remaining = N;

    while (remaining > 0) {
        fd_set rfds;
        FD_ZERO(&rfds);
        int maxfd = -1;

        for (int i = 0; i < N; i++) {
            if (!ps[i].file_done) {
                FD_SET(ps[i].fd, &rfds);
                if (ps[i].fd > maxfd) maxfd = ps[i].fd;
            }
        }

        int r = select(maxfd + 1, &rfds, NULL, NULL, NULL);
        if (r < 0) {
            if (errno == EINTR) continue;
            die("select");
        }

        for (int i = 0; i < N; i++) {
            if (ps[i].file_done) continue;
            if (!FD_ISSET(ps[i].fd, &rfds)) continue;

            char tmp[1000];
            size_t want = (size_t)DataLen;
            if (want > sizeof(tmp)) want = sizeof(tmp);

            ssize_t n = read(ps[i].fd, tmp, want); 
            if (n < 0) {
                if (errno == EINTR) continue;
                die("read");
            } else if (n == 0) {
                ps[i].file_done = 1;
                remaining--;
                close(ps[i].fd);
            } else {
                buf_append(&ps[i], tmp, (size_t)n);
                parent_consume_lines(&ps[i], &global);
            }
        }
    }

    for (int i = 0; i < N; i++) {
        int st;
        waitpid(pids[i], &st, 0);
    }

    write_output(outname, &global);
    for (int i = 0; i < N; i++) ps_free(&ps[i]);
    wm_free(&global);
    return 0;
}