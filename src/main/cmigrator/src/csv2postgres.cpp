#include <iostream>
#include <cstring>

#include "csv2postgres.h"
#include "endianness.h"

int Csv2Postgres::csv2postgres(int argc, char *argv[]) 
{
  printf("csv2postgres\n");
  fp=fopen("/home/adam/data/postgres_int.bin","wb");

  std::cout << "My endianness is: " << endianness::host_endian << std::endl;

  // prepare columns
  int64_t i = 1;
  // number of elements (columns) can be more than one
  int elemNum=1;
  elemSize = new size_t[elemNum];
  elemSize[0]=sizeof(i);

  writeHeader(fp);
  // write the real data
  // number of columns
  unsigned short int colNumber = 1;
  colNumber=endianness::byte_swap<endianness::host_endian,endianness::big_endian,unsigned short int>(colNumber);
  fwrite(&colNumber,2,1,fp); // number of columnns is always unsigned int and 2 bytes

  // number of bytes in the field
  unsigned int byteNumber = sizeof(int64_t);
  std::cout << "Byte number: " << byteNumber << std::endl;
  byteNumber = endianness::byte_swap<endianness::host_endian,endianness::big_endian,unsigned int>(byteNumber);
  fwrite(&byteNumber,4,1,fp); // byte number is always 4 bytes

  // i is our only one column value
  i = endianness::byte_swap<endianness::host_endian,endianness::big_endian,int64_t>(i);
  fwrite(&i,elemSize[0],1,fp);

  writeTrailer(fp);
  fclose(fp);
  delete[] elemSize;

  return 0;
}

void Csv2Postgres::writeHeader(FILE* fp)
{
  const char * header = "PGCOPY\n\377\r\n\0";
  // flags - 32 bits (each byte is 0)
  const char * flags = "\0\0\0\0";
  // header extension area - 32 bits (each byte is 0)
  const char * extension = "\0\0\0\0";

  fwrite(header,11,1,fp);
  fwrite(flags,4,1,fp);
  fwrite(extension,4,1,fp);
}

void Csv2Postgres::writeTrailer(FILE* fp)
{
  // file trailer
  short int trailer= -1;

  // write trailer
  trailer = endianness::byte_swap<endianness::host_endian,endianness::big_endian,short int>(trailer);
  fwrite(&trailer,2,1,fp);
}
