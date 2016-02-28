#include "attribute/attribute.h"

#define __STDC_FORMAT_MACROS
#include <inttypes.h>

// this is the definition of null(s) for values that are empty (for example in SciDB binary format)
const char Attribute::nullValues[Attribute::nullValuesSize] = {'\0','\0','\0','\0','\0','\0','\0','\0','\0','\0','\0','\0','\0','\0','\0','\0','\0','\0','\0','\0'};

Attribute* new_clone(Attribute const& other)
{
  return other.clone();
}

Attribute::~Attribute() {}

GenericAttribute<char*>::GenericAttribute(bool isNullable)
{
  this->value=NULL;
  this->isNullable=isNullable;
  this->bytesNumber=-1; // this is specific for every string
}

void cleanGenericAttribute(char* value)
{
  if (value != NULL) 
    {
      delete [] value;
    }
}

GenericAttribute<char*>::~GenericAttribute()
{
  //fprintf(stderr,"%s","Destructor was called");
  cleanGenericAttribute(this->value);
}

int GenericAttribute<char*>::postgresReadBinary(FILE *fp)
{
  // string is handled differently than other types
  fread(&this->bytesNumber,4,1,fp);
  //std::cout << "value bytes number before endianness: " << valueBytesNumber << std::endl;
  this->bytesNumber = endianness::from_postgres<int32_t>(this->bytesNumber);
  //std::cout << "value bytes number after endianness: " << valueBytesNumber << std::endl;
  if (this->bytesNumber == -1)
    {
      // this is null and there are no bytes for the value
      this->isNull=true;
      return 0;
    }
  this->isNull=false;
  cleanGenericAttribute(this->value);
  this->value = new char[this->bytesNumber+1]; // +1 is for NULL \0 value at the end of the string: SciDB adds \0 at the end of each string
  this->value[this->bytesNumber]='\0';
  //std::cout << "value bytes number: " << valueBytesNumber << std::endl;
  fread(this->value,this->bytesNumber,1,fp);
  // std::cout << "value read: " << value << std::endl;
  return 0; // success
}

int GenericAttribute<char*>::writeCsv(std::ofstream & ofile)
{
  // write nothing if the value is NULL
  if (this->isNull) return 0;
  // write the value only if it's not NULL
  // fprintf(fp,"%" PRId64,this->value);
  //printf("%.*s",this->bytesNumber,value);
  ofile.width(this->bytesNumber);
  ofile << this->value;
  return 0;
}

int GenericAttribute<char*>::sciDBWriteBinary(FILE *fp) 
{
  if(this->isNullable) 
    { // we have to add one byte with info it it is null or not
      if(this->isNull)
	{
	  // we don't know the reason why it is null so we'll write byte 0
	  char nullReason=0;
	  fwrite(&nullReason,1,1,fp);
	  // this is indeed null, so only write -1 as the number of bytes 
	  // and don't write the NULL value
	  int32_t sciDBBytesNumber = 0;
	  fwrite(&sciDBBytesNumber,4,1,fp);
	  return 0;
	}
      else 
	{
	  // this value is not null so the byte of nullable should have value: -1
	  char notNull=-1;
	  fwrite(&notNull,1,1,fp);
	}
    }
  //std::cout << "Bytes number written for SciDB: " << this->bytesNumber << std::endl;
  //fflush(stdout);
  int32_t sciDBBytesNumber=this->bytesNumber+1;
  fwrite(&(sciDBBytesNumber),4,1,fp);
  //printf("Value written for SciDB: %.13s\n",this->value);
  //fflush(stdout);
  fwrite(this->value,this->bytesNumber+1,1,fp); // +1 is for NULL \0 value at the end of the string: SciDB adds \0 at the end of each string
  return 0;
}

int GenericAttribute<char*>::postgresReadBinaryBuffer(Buffer * buffer)
{
  // string is handled differently than other types
  BufferRead(&this->bytesNumber,4,1,buffer);
  //std::cout << "value bytes number before endianness: " << valueBytesNumber << std::endl;
  this->bytesNumber = endianness::from_postgres<int32_t>(this->bytesNumber);
  //std::cout << "value bytes number after endianness: " << this->bytesNumber << std::endl;
  if (this->bytesNumber == -1)
    {
      // this is null and there are no bytes for the value
      this->isNull=true;
      return 0;
    }
  this->isNull=false;
  cleanGenericAttribute(this->value);
  this->value = new char[this->bytesNumber+1]; // +1 is for NULL \0 value at the end of the string: SciDB adds \0 at the end of each string
  this->value[this->bytesNumber]='\0';
  //std::cout << "value bytes number: " << valueBytesNumber << std::endl;
  BufferRead(this->value,this->bytesNumber,1,buffer);
  //std::cout << "value read: " << value << std::endl;
  return 0; // success
}
