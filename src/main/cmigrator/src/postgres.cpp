#include <stdint.h>
#include <iostream>
#include "postgres.h"
#include "endianness.h"

#include <boost/log/trivial.hpp>

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

void Postgres::writeHeader(FILE *fp) 
{
  // std::cout << "Write PostgreSQL header\n";
  // write 19 bytes

  // 11 bytes for the signature
  fwrite(&BinarySignature,11,1,fp);

  // 4 bytes for flags
  int32_t flagsField = 0;
  int32_t flagsFieldFormatted = endianness::to_postgres<int32_t>(flagsField);
  fwrite(&flagsFieldFormatted,4,1,fp);

  // 4 bytes for the length of the header extension area
  int32_t headerExtensionLen = 0;
  int32_t headerExtensionLenFormatted = endianness::to_postgres<int32_t>(headerExtensionLen);
  fwrite(&headerExtensionLenFormatted,4,1,fp);
}

void Postgres::skipHeader(FILE *fp)
{
  // simply skip first 19 bytes
  fseek(fp,19,SEEK_CUR);
}

void Postgres::writeColNumber(FILE *fp, int16_t colNumber) 
{
  /* Each tuple begins with a 16-bit integer count of the number of fields 
     in the tuple. */
  int16_t colNumberPostgres = endianness::to_postgres<int16_t>(colNumber);
  fwrite(&colNumberPostgres,2,1,fp);
}

void Postgres::writeFileTrailer(FILE *fp) 
{
  /* The file trailer consists of a 16-bit integer word containing -1. 
     This is easily distinguished from a tuple's field-count word. */
  int16_t trailer = -1;
//BOOST_LOG_TRIVIAL(debug) << "trailer: " << trailer;
  int16_t trailerPostgres = endianness::to_postgres<int16_t>(trailer);
//BOOST_LOG_TRIVIAL(debug) << "trailerPostgres: " << trailerPostgres;
  fwrite(&trailerPostgres,2,1,fp);
}
