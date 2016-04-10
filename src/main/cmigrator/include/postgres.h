#ifndef POSTGRES_H
#define POSTGRES_H

#include <cstdio>
#define __STDC_FORMAT_MACROS
#include <inttypes.h>

#include <inttypes.h>

#include "buffer.h"

class Postgres {
  public:
    static char* readHeader(FILE *fp);
    static void writeHeader(FILE *fp);
    static void skipHeader(FILE *fp);
    static short int readColNumber(FILE *fp);
    static void writeColNumber(FILE *fp, int16_t colNumber);
    static void writeFileTrailer(FILE *fp);
    static short int readColNumberBuffer(Buffer * buffer);
};

#endif // POSTGRES_H
