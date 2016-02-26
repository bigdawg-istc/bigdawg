#ifndef POSTGRES_2_CSV
#define POSTGRES_2_CSV

#include <cstdio>
#include <string>

#include "attribute/attribute.h"

class Postgres2Csv {

private:
  FILE *binInFp;
  FILE *csvOutFp;

 public:
  int postgres2csv(const std::string &  binFileName, std::vector<std::shared_ptr<Attribute> > & attributes, const std::string & csvFileName);
};

#endif // POSTGRES_2_CSV
