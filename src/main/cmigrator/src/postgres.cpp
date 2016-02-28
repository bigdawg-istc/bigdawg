#include <stdint.h>
#include <iostream>
#include "postgres.h"
#include "endianness.h"

#define __STDC_FORMAT_MACROS
#include <inttypes.h>

static const char BinarySignature[11] = {'P','G','C','O','P','Y','\n','\377','\r','\n','\0'};

short int Postgres::readColNumber(FILE *fp) 
{
  // read 2 bytes representing number of columns stored in each line
  // fseek(this->fp,2,SEEK_CUR);
  // we have to read the colNumber and check if it is the end of the file
  // -1 represents the end of the binary data
  int16_t colNumber;
  fread(&colNumber,2,1,fp);
  colNumber = endianness::from_postgres<int16_t>(colNumber);
  //printf("%" PRId16,colNumber);
  return colNumber;
}

short int Postgres::readColNumberBuffer(Buffer * buffer) 
{
  // read 2 bytes representing number of columns stored in each line
  // fseek(this->fp,2,SEEK_CUR);
  // we have to read the colNumber and check if it is the end of the file
  // -1 represents the end of the binary data
  int16_t colNumber;
  BufferRead(&colNumber,2,1,buffer);
  colNumber = endianness::from_postgres<int16_t>(colNumber);
  //printf("%" PRId16,colNumber);
  return colNumber;
}


char* Postgres::readHeader(FILE *fp) 
{
  // simply read first 19 bytes
  unsigned int headerSize = 19;
  char *buffer = new char[headerSize];
  fread(buffer,headerSize,1,fp);
  std::cout << "header from Postgres: " << std::endl;
  for (unsigned int i=0; i<headerSize; ++i) {
    printf("%o ",buffer[i]);
  }
  std::cout << std::endl;
  return buffer;
}

void Postgres::skipHeader(FILE *fp)
{
  // simply skip first 19 bytes
  fseek(fp,19,SEEK_CUR);
}

