#ifndef POSTGRES_H
#define POSTGRES_H

#include <cstdio>

#include "buffer.h"

class Postgres 
{
 public:
  static char* readHeader(FILE *fp);
  static void skipHeader(FILE *fp);
  static short int readColNumber(FILE *fp);
  static short int readColNumberBuffer(Buffer * buffer);
};

#endif // POSTGRES_H
