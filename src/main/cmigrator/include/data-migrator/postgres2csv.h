#ifndef POSTGRES_2_CSV
#define POSTGRES_2_CSV

#include <cstdio>
#include <string>
#include <memory>
#include <vector>
#include "boost/smart_ptr/shared_ptr.hpp"

#include "attribute.h"

class Postgres2Csv {
  public:
    static int postgres2csv(FILE* in, std::vector<boost::shared_ptr<Attribute> > & attributes, const std::string & csvFileName);
    static int postgres2csv(const std::string & inputCsv, std::vector<boost::shared_ptr<Attribute> > & attributes, const std::string & outputCsv);
};

#endif // POSTGRES_2_CSV
