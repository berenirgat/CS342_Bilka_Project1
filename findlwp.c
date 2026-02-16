#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <ctype.h>
#include <errno.h>
#include <sys/select.h>
#include <sys/wait.h>

#define MAX_N     10
#define WORD_MAX  128
#define LINE_MAX  4096

static void die(const char *msg) { perror(msg); exit(1); }

// write() in chunks 
static void write_chunked(int fd, const char *buf, size_t n, int DataLen) {
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

typedef struct {
    int file_idx;
    int line_no;
} Occ;

typedef struct {
    char word[WORD_MAX];
    Occ *occ;
    size_t occ_sz, occ_cap;
} Entry;

typedef struct {
    Entry *arr;
    size_t sz, cap;
} Map;

static void map_init(Map *m) { m->arr=NULL; m->sz=0; m->cap=0; }

static void entry_free(Entry *e) {
    free(e->occ);
    e->occ=NULL; e->occ_sz=0; e->occ_cap=0;
}

static void map_free(Map *m) {
    for (size_t i=0;i<m->sz;i++) entry_free(&m->arr[i]);
    free(m->arr);
    m->arr=NULL; m->sz=0; m->cap=0;
}

static void occ_push(Entry *e, int file_idx, int line_no) {
    if (e->occ_sz == e->occ_cap) {
        size_t nc = e->occ_cap ? e->occ_cap*2 : 8;
        Occ *no = (Occ*)realloc(e->occ, nc*sizeof(Occ));
        if (!no) die("realloc occ");
        e->occ = no;
        e->occ_cap = nc;
    }
    e->occ[e->occ_sz++] = (Occ){file_idx, line_no};
}

static Entry* map_get_or_add(Map *m, const char *w) {
    for (size_t i=0;i<m->sz;i++) {
        if (strcmp(m->arr[i].word, w) == 0) return &m->arr[i];
    }
    if (m->sz == m->cap) {
        size_t nc = m->cap ? m->cap*2 : 64;
        Entry *na = (Entry*)realloc(m->arr, nc*sizeof(Entry));
        if (!na) die("realloc map");
        m->arr = na;
        m->cap = nc;
    }
    Entry *e = &m->arr[m->sz++];
    memset(e, 0, sizeof(*e));
    strncpy(e->word, w, WORD_MAX-1);
    e->word[WORD_MAX-1] = '\0';
    return e;
}

// child part (file reading in distinct process)
static void child_run(const char *filename, int file_idx, int K, int write_fd, int DataLen) {
    FILE *fp = fopen(filename, "r");
    if (!fp) die("fopen input");

    Map local; map_init(&local);

    char line[LINE_MAX];
    int line_no = 0;

    while (fgets(line, sizeof(line), fp)) {
        line_no++;

        const char *p = line;
        while (*p) {
            while (*p && isspace((unsigned char)*p)) p++;
            if (!*p) break;
            const char *s = p;
            while (*p && !isspace((unsigned char)*p)) p++;
            size_t len = (size_t)(p - s);
            if ((int)len >= K) {
                char w[WORD_MAX];
                size_t cpy = len;
                if (cpy >= WORD_MAX) cpy = WORD_MAX - 1;
                memcpy(w, s, cpy);
                w[cpy] = '\0';

                Entry *e = map_get_or_add(&local, w);
                occ_push(e, file_idx, line_no);  // duplicates ok
            }
        }
    }

    fclose(fp);

    // save occurrences
    for (size_t i=0;i<local.sz;i++) {
        Entry *e = &local.arr[i];
        for (size_t j=0;j<e->occ_sz;j++) {
            char rec[WORD_MAX + 64];
            int n = snprintf(rec, sizeof(rec), "%s\t%d\t%d\n",
                             e->word, e->occ[j].file_idx, e->occ[j].line_no);
            if (n < 0) die("snprintf");
            write_chunked(write_fd, rec, (size_t)n, DataLen);
        }
    }

    map_free(&local);
    close(write_fd);
    _exit(0);
}

// parent part
typedef struct {
    int fd;
    int done;
    char *buf;
    size_t sz, cap;
} PipeState;

static void ps_init(PipeState *ps, int fd) {
    ps->fd = fd;
    ps->done = 0;
    ps->buf = NULL;
    ps->sz = 0;
    ps->cap = 0;
}

static void ps_free(PipeState *ps) {
    free(ps->buf);
    ps->buf = NULL; ps->sz=0; ps->cap=0;
}

static void ps_append(PipeState *ps, const char *data, size_t n) {
    if (ps->sz + n + 1 > ps->cap) {
        size_t nc = ps->cap ? ps->cap*2 : 2048;
        while (nc < ps->sz + n + 1) nc *= 2;
        char *nb = (char*)realloc(ps->buf, nc);
        if (!nb) die("realloc ps->buf");
        ps->buf = nb;
        ps->cap = nc;
    }
    memcpy(ps->buf + ps->sz, data, n);
    ps->sz += n;
    ps->buf[ps->sz] = '\0';
}

static void ps_consume_lines(PipeState *ps, Map *global) {
    char *start = ps->buf;
    char *end = ps->buf + ps->sz;

    while (1) {
        char *nl = memchr(start, '\n', (size_t)(end - start));
        if (!nl) break;
        *nl = '\0';

        // line: word file line 
        char *w = start;
        char *t1 = strchr(w, '\t');
        char *t2 = t1 ? strchr(t1+1, '\t') : NULL;
        if (t1 && t2) {
            *t1 = '\0';
            *t2 = '\0';
            int file_idx = atoi(t1+1);
            int line_no  = atoi(t2+1);
            if (w[0] != '\0') {
                Entry *e = map_get_or_add(global, w);
                occ_push(e, file_idx, line_no);
            }
        }

        start = nl + 1;
    }
    size_t rem = (size_t)(end - start);
    memmove(ps->buf, start, rem);
    ps->sz = rem;
    ps->buf[ps->sz] = '\0';
}

// output sorting
static int cmp_entry_word(const void *a, const void *b) {
    const Entry *x = (const Entry*)a;
    const Entry *y = (const Entry*)b;
    return strcmp(x->word, y->word); /* case-sensitive */
}

static int cmp_occ(const void *a, const void *b) {
    const Occ *x = (const Occ*)a;
    const Occ *y = (const Occ*)b;
    if (x->file_idx != y->file_idx) return x->file_idx - y->file_idx;
    return x->line_no - y->line_no;
}

static void write_output(const char *outname, Map *m) {
    qsort(m->arr, m->sz, sizeof(Entry), cmp_entry_word);
    for (size_t i=0;i<m->sz;i++) {
        qsort(m->arr[i].occ, m->arr[i].occ_sz, sizeof(Occ), cmp_occ);
    }

    FILE *out = fopen(outname, "w");
    if (!out) die("fopen output");

    for (size_t i=0;i<m->sz;i++) {
        Entry *e = &m->arr[i];
        fprintf(out, "%s (count=%zu): ", e->word, e->occ_sz);
        for (size_t j=0;j<e->occ_sz;j++) {
            fprintf(out, "%d-%d", e->occ[j].file_idx, e->occ[j].line_no);
            if (j + 1 < e->occ_sz) fprintf(out, ", ");
        }
        fprintf(out, "\n");
    }
    fclose(out);
}

// execution
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
        fprintf(stderr, "Invalid args (N 1..10, K 1..100, DataLen 1..1000)\n");
        return 1;
    }
    if (N > MAX_N) N = MAX_N;
    if (DataLen > 1000) DataLen = 1000;

    int pipes[MAX_N][2];
    pid_t pids[MAX_N];

    for (int i=0;i<N;i++) {
        if (pipe(pipes[i]) < 0) die("pipe");

        pid_t pid = fork();
        if (pid < 0) die("fork");

        if (pid == 0) { // that means child
            close(pipes[i][0]);
            for (int j=0;j<i;j++) { close(pipes[j][0]); close(pipes[j][1]); }

            char filename[256];
            snprintf(filename, sizeof(filename), "%s%d", prefix, i+1);

            child_run(filename, i+1, K, pipes[i][1], DataLen);
            _exit(0);
        } else { // that means parent
            pids[i] = pid;
            close(pipes[i][1]);
        }
    }

    PipeState ps[MAX_N];
    for (int i=0;i<N;i++) ps_init(&ps[i], pipes[i][0]);

    Map global; map_init(&global);

    int remaining = N;
    while (remaining > 0) {
        fd_set rfds;
        FD_ZERO(&rfds);
        int maxfd = -1;

        for (int i=0;i<N;i++) {
            if (!ps[i].done) {
                FD_SET(ps[i].fd, &rfds);
                if (ps[i].fd > maxfd) maxfd = ps[i].fd;
            }
        }

        int r = select(maxfd + 1, &rfds, NULL, NULL, NULL);
        if (r < 0) {
            if (errno == EINTR) continue;
            die("select");
        }

        for (int i=0;i<N;i++) {
            if (ps[i].done) continue;
            if (!FD_ISSET(ps[i].fd, &rfds)) continue;

            char tmp[1000];
            size_t want = (size_t)DataLen;
            if (want > sizeof(tmp)) want = sizeof(tmp);

            ssize_t n = read(ps[i].fd, tmp, want);
            if (n < 0) {
                if (errno == EINTR) continue;
                die("read");
            } else if (n == 0) {
                ps[i].done = 1;
                remaining--;
                close(ps[i].fd);
            } else {
                ps_append(&ps[i], tmp, (size_t)n);
                ps_consume_lines(&ps[i], &global);
            }
        }
    }

    for (int i=0;i<N;i++) {
        int st;
        waitpid(pids[i], &st, 0);
    }

    write_output(outname, &global);

    for (int i=0;i<N;i++) ps_free(&ps[i]);
    map_free(&global);

    return 0;
}