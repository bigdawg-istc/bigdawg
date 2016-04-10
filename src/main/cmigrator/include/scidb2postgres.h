#ifndef SCIDB_2_POSTGRES
#define SCIDB_2_POSTGRES

#include <cstdio>
#include "attribute/attribute.h"

class Scidb2Postgres {
  private:
    FILE* binIn;
    FILE* binOut;

  public:
    static int scidb2postgres(FILE* in, std::vector<std::shared_ptr<Attribute> > & attributes, FILE* out);
};

#endif // SCIDB_TO_POSTGRES
