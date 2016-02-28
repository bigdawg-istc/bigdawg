#ifndef POSTGRES_2_SCIDB
#define POSTGRES_2_SCIDB

#include <cstdio>
#include "attribute/attribute.h"

class Postgres2Scidb
{
 private:
  FILE* binIn;
  FILE* binOut;

 public:
  static int postgres2scidb(FILE* in, std::vector<std::shared_ptr<Attribute> > & attributes, FILE* out);
};

#endif // POSTGRES_2_SCIDB
