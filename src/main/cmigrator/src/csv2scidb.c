#include <stdio.h>
#include <csv2scidb.h>

class Csv2Scidb {

int csv2scidb(int argc, char *argv[]) 
{
  printf("csv2scidb\n");
  FILE *fp;
  fp=fopen("/home/adam/data/int4.bin","w");

  // prepare columns
  int i;
  int j;
  double val;

  // field: 0,0
  i=0;j=0;val=0.0;
  
  fwrite(&i,elemSize_i,1,fp);
  fwrite(&j,elemSize_j,1,fp);
  fwrite(&val,elemSize_val,1,fp);

  // field: 0,1
  i=0;j=1;val=0.1;
  


  fclose(fp);
  return 0;
}

void writeData(int *i, int *j, double *val) {
}
};
