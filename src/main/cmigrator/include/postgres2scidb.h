#ifndef POSTGRES_2_SCIDB
#define POSTGRES_2_SCIDB

#include <cstdio>
#include "attribute.h"

class Postgres2Scidb {
  public:
    static int postgres2scidb(FILE* in, std::vector<std::shared_ptr<Attribute> > & attributes, FILE* out);
};

#endif // POSTGRES_2_SCIDB
