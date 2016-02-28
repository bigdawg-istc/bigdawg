#include <cstdio>

class Csv2Postgres {

private:
  size_t *elemSize;
  FILE *fp;

 public:
  int csv2postgres(int argc, char *argv[]);
  void writeHeader(FILE* fp);
  void writeTrailer(FILE* fp);
};
