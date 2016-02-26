#include "csv2scidb.h"

int Csv2SciDB::csv2scidb(int argc, char *argv[]) 
{
  printf("csv2scidb\n");
  fp=fopen("/home/adam/data/int4.bin","w");

  // prepare columns
  int i;
  int j;
  double val;

  int elemNum=3;
  elemSize = new size_t[elemNum];
    
  elemSize[0]=sizeof(i);
  elemSize[1]=sizeof(j);
  elemSize[2]=sizeof(val);

  i=0;j=0;val=0.0;
  this->writeData(&i,&j,&val);

  i=0;j=1;val=0.1;
  this->writeData(&i,&j,&val);

  i=1;j=0;val=1.0;
  this->writeData(&i,&j,&val);

  i=1;j=1;val=1.1;
  this->writeData(&i,&j,&val);

  fclose(fp);
  delete[] elemSize;

  return 0;
}

void Csv2SciDB::writeData(int *i, int *j, double *val) {
  fwrite(i,elemSize[0],1,fp);
  fwrite(j,elemSize[1],1,fp);
  fwrite(val,elemSize[2],1,fp);
}

