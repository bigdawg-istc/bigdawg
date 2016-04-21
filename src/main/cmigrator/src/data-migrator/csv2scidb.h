#include <stdio.h>

class Csv2SciDB {

  private:
    size_t *elemSize;
    FILE *fp;

  public:
    int csv2scidb(int argc, char *argv[]);
    void writeData(int *i, int *j, double *val);
};
