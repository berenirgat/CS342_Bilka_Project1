#ifndef COMMON_H
#define COMMON_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>

#define WORD_MAX 128

typedef struct {
    int file_idx;   
    int line_no;    
} Occ;

typedef struct {
    char word[WORD_MAX];
    Occ *occ;    
    size_t occ_sz;
    size_t occ_cap;
} WordEntry;

typedef struct {
    WordEntry *arr; 
    size_t sz;
    size_t cap;
} WordMap;

static inline void die(const char *msg) {
    perror(msg);
    exit(1);
}

static inline void wm_init(WordMap *m) {
    m->arr = NULL; m->sz = 0; m->cap = 0;
}

static inline void we_free(WordEntry *e) {
    free(e->occ);
    e->occ = NULL;
    e->occ_sz = e->occ_cap = 0;
}

static inline void wm_free(WordMap *m) {
    for (size_t i = 0; i < m->sz; i++) we_free(&m->arr[i]);
    free(m->arr);
    m->arr = NULL; m->sz = m->cap = 0;
}

static inline void occ_push(WordEntry *e, int file_idx, int line_no) {
    if (e->occ_sz == e->occ_cap) {
        size_t nc = e->occ_cap ? e->occ_cap * 2 : 8;
        Occ *no = (Occ*)realloc(e->occ, nc * sizeof(Occ));
        if (!no) die("realloc occ");
        e->occ = no;
        e->occ_cap = nc;
    }
    e->occ[e->occ_sz++] = (Occ){ .file_idx = file_idx, .line_no = line_no };
}

// simple linear search 
static inline WordEntry* wm_get_or_add(WordMap *m, const char *w) {
    for (size_t i = 0; i < m->sz; i++) {
        if (strcmp(m->arr[i].word, w) == 0) return &m->arr[i];
    }
    if (m->sz == m->cap) {
        size_t nc = m->cap ? m->cap * 2 : 64;
        WordEntry *na = (WordEntry*)realloc(m->arr, nc * sizeof(WordEntry));
        if (!na) die("realloc map");
        m->arr = na;
        m->cap = nc;
    }
    WordEntry *e = &m->arr[m->sz++];
    memset(e, 0, sizeof(*e));
    strncpy(e->word, w, WORD_MAX - 1);
    e->word[WORD_MAX - 1] = '\0';
    return e;
}

// spliting the line into words by whitespace
typedef void (*token_cb)(const char *token, void *ctx);

static inline void tokenize_ws(const char *line, token_cb cb, void *ctx) {
    const char *p = line;
    while (*p) {
        while (*p && isspace((unsigned char)*p)) p++;
        if (!*p) break;
        const char *s = p;
        while (*p && !isspace((unsigned char)*p)) p++;
        size_t len = (size_t)(p - s);
        char tmp[WORD_MAX];
        if (len >= WORD_MAX) len = WORD_MAX - 1;
        memcpy(tmp, s, len);
        tmp[len] = '\0';
        cb(tmp, ctx);
    }
}

#endif