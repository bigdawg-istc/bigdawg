#include "postgres2scidb.h"
#include "postgres.h"
#include "buffer.h"

#define BUFFER_SIZE 65536
//#define BUFFER_SIZE 104857600

int Postgres2Scidb::postgres2scidb(FILE* inFile, std::vector<std::shared_ptr<Attribute> > &attributes, FILE* outFile) 
{
  //Postgres::skipHeader(inFile);
  Postgres::readHeader(inFile);
  Buffer buffer;
  BufferNew(&buffer,inFile,BUFFER_SIZE);
  while (Postgres::readColNumberBuffer(&buffer) != -1)
    {
      for (std::vector<std::shared_ptr<Attribute> >::iterator it=attributes.begin(); it != attributes.end();++it) {
	(*it)->postgresReadBinaryBuffer(&buffer);
	(*it)->sciDBWriteBinary(outFile);
      }
    }
  BufferDispose(&buffer);
  return 0;
}
