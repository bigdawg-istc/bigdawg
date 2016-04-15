#ifndef POSTGRES_2_SCIDB
#define POSTGRES_2_SCIDB

#include <cstdio>
#include <vector>
#include <memory>
#include "boost/smart_ptr/shared_ptr.hpp"

#include "attribute.h"

class Postgres2Scidb {
  public:
    static int postgres2scidb(FILE* in, std::vector<boost::shared_ptr<Attribute> > & attributes, FILE* out);
};

#endif // POSTGRES_2_SCIDB
