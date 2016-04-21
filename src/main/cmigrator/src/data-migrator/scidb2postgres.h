#ifndef SCIDB_2_POSTGRES
#define SCIDB_2_POSTGRES

#include <cstdio>
#include <vector>
#include <memory>
#include "attribute.h"
#include "boost/smart_ptr/shared_ptr.hpp"

class Scidb2Postgres {
  private:
    FILE* binIn;
    FILE* binOut;

  public:
    static int scidb2postgres(FILE* in, std::vector<boost::shared_ptr<Attribute> > & attributes, FILE* out);
};

#endif // SCIDB_TO_POSTGRES
