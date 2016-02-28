#include <cstdio>
#include <iostream>
#include <limits.h>
#include <type_traits>

#include "boost/cstdfloat.hpp"
#include "gtest/gtest.h"

#include "attribute/attribute.h"
#include "endianness.h"

// TEST(test_case_name, test_name) {
//  ... test body ...
// }

TEST(Platform,floatDouble)
{
  static_assert(std::numeric_limits<float>::is_iec559, "Only support IEC 559 (IEEE 754) float");
  static_assert(sizeof(float) * CHAR_BIT == 32, "Only support float => Single Precision IEC 559 (IEEE 754)");

  static_assert(std::numeric_limits<double>::is_iec559, "Only support IEC 559 (IEEE 754) double");
  static_assert(sizeof(double) * CHAR_BIT == 64, "Only support double => Double Precision IEC 559 (IEEE 754)");
  // std::size_t sizeDouble = sizeof(double);
  // printf("size of double from printf:\n");
  // printf("first time: %zu\n",sizeDouble);
  // printf("second time: %zx\n",sizeDouble);
  std::cout << "size of double: " << sizeof(double) << std::endl;
  std::cout << "size of short int: " << sizeof(short int) << std::endl;
  std::cout << "size of boost::float64_t : " << sizeof(boost::float64_t) << std::endl;
  std::cout << "size of long double: " << sizeof(long double) << std::endl;
  std::cout << "size of float: " << sizeof(float) << std::endl;
  std::cout << "is iec 559:" << std::numeric_limits< double >::is_iec559 << std::endl;
}

TEST(Postgres,PostgresReadBinaryInteger32Bit) 
{
  GenericAttribute<int32_t> attr = GenericAttribute<int32_t>(true);
  FILE * fp;
  fp = tmpfile();
  int32_t bytesNumber = sizeof(int32_t);
  // postgres writes the bytes in Big Endian format so change the value to Big Endian
  std::cout << "bytes number: " << bytesNumber << std::endl;
  bytesNumber = endianness::to_postgres<int32_t>(bytesNumber);
  fwrite(&bytesNumber,sizeof(int32_t),1,fp);
  int32_t testIntHostEndian = 31; // just arbitrary number 
  int32_t testIntBigEndian = 0;
  testIntBigEndian = endianness::to_postgres<int32_t>(testIntHostEndian);
  fwrite(&testIntBigEndian,bytesNumber,1,fp);
  // you wrote to a file and now would like to start reading from the beginnin of the file
  rewind(fp); 
  attr.postgresReadBinary(fp);
  std::cout << "attribute value: " << attr.getValue() << std::endl;
  EXPECT_EQ(testIntHostEndian,attr.getValue());
  fclose(fp);
}

TEST(Postgres,ReadBinaryTestFloat64_t) 
{
  GenericAttribute<boost::float64_t> attr = GenericAttribute<boost::float64_t>(true);
  FILE * fp;
  fp = tmpfile();
  int32_t bytesNumberRaw = sizeof(boost::float64_t);
  // postgres writes the bytes in Big Endian format so change the value to Big Endian
  std::cout << "test bytes number raw: " << bytesNumberRaw << std::endl;
  int32_t bytesNumber = endianness::to_postgres<int32_t>(bytesNumberRaw);
  std::cout << "test bytes number after endianness: " << bytesNumber << std::endl;
  fwrite(&bytesNumber,sizeof(int32_t),1,fp);

  bytesNumber = endianness::from_postgres<int32_t>(bytesNumber);
  std::cout << "test bytes number after endianness 2: " << bytesNumber << std::endl;
  
  boost::float64_t testIntHostEndian = 31.23;
  boost::float64_t testIntBigEndian = endianness::to_postgres<boost::float64_t>(testIntHostEndian);
  fwrite(&testIntBigEndian,bytesNumber,1,fp);
  // you wrote to a file and now would like to start reading from the beginnin of the file
  rewind(fp); 
  attr.postgresReadBinary(fp);
  std::cout << "attribute size: " << attr.getBytesNumber() << std::endl;
  EXPECT_EQ(bytesNumberRaw,attr.getBytesNumber());
  std::cout << "attribute value: " << attr.getValue() << std::endl;
  EXPECT_EQ(testIntHostEndian,attr.getValue());
  fclose(fp);
}

TEST(Postgres,ReadBinaryTestString) 
{
  GenericAttribute<char*> attr = GenericAttribute<char*>(true);
  FILE * fp;
  fp = tmpfile();
  const char* value = "Adam Dziedzic";
  int32_t bytesNumberRaw = 13;
  // postgres writes the bytes in Big Endian format so change the value to Big Endian
  std::cout << "bytes number: " << bytesNumberRaw << std::endl;
  int32_t bytesNumber = endianness::to_postgres<int32_t>(bytesNumberRaw);
  fwrite(&bytesNumber,sizeof(int32_t),1,fp);
  fwrite(value,bytesNumber,1,fp);
  // you wrote to a file and now would like to start reading from the beginnin of the file
  rewind(fp); 
  attr.postgresReadBinary(fp);
  std::cout << "attribute value: " << std::endl;
  printf("%.*s",bytesNumberRaw,attr.getValue());
  std::cout << std::endl;
  // check the number of bytes - the number of bytes changes for strings
  ASSERT_EQ(bytesNumberRaw,attr.getBytesNumber());
  // check the value: strncmp compares n first characters of the strings and returns 0 if they are equal
  ASSERT_EQ(0,strncmp(value,attr.getValue(),bytesNumberRaw));
  // also check if the null value was added at the end of the string
  ASSERT_EQ(0,strncmp(value,attr.getValue(),bytesNumberRaw+1));
  fclose(fp);
}

TEST(SciDB,WriteBinaryTestStringNotNull)
{
  GenericAttribute<char*> attr = GenericAttribute<char*>(false);
  int32_t setBytesNumber=13;
  char* value = new char[setBytesNumber+1];
  strcpy(value,"Adam Dziedzic"); // it add NULL at the end of the value
  attr.setValue(value);
  attr.setBytesNumber(setBytesNumber);
  std::cout << "attribute value before writing for SciDB: " << attr.getValue() << std::endl;
  FILE * fp;
  fp = tmpfile();
  attr.sciDBWriteBinary(fp);
  rewind(fp);
  
  // now check what is in the file after writing values for SciDB
  int32_t bytesNumber=-1;
  fread(&bytesNumber,sizeof(int32_t),1,fp);
  std::cout << "Bytes number found in the binary file for SciDB: " << bytesNumber << std::endl;
  ASSERT_EQ(setBytesNumber+1,bytesNumber); // SciDB requires one byte more for the string (the \0 (null) value at the end of the string
  char* receivedValue = new char[bytesNumber];
  fread(receivedValue,bytesNumber,1,fp); // received value shoule already have the \0 (null) at the end
  std::cout << "Final value found in the binary file for SciDB: " << receivedValue << std::endl;
  ASSERT_STREQ(value,receivedValue); // it checks if NULL values are at the end of the strings
  // check if this is the end of the file
  char stop;
  fread(&stop,1,1,fp);
  EXPECT_TRUE(feof(fp));
  //delete value; // this is removed by destructor of the GenericAttribute<char*> class
  delete [] receivedValue;
  fclose(fp);
}

TEST(SciDB,WriteBinaryTestStringNullableWithValue)
{
  GenericAttribute<char*> attr = GenericAttribute<char*>(true);
  int32_t setBytesNumber=13;
  char* value = new char[setBytesNumber+1];
  strcpy(value,"Adam Dziedzic");
  attr.setValue(value);
  attr.setBytesNumber(setBytesNumber);
  attr.setIsNull(false);
  std::cout << "attribute value before writing for SciDB: " << attr.getValue() << std::endl;
  FILE * fp;
  fp = tmpfile();
  attr.sciDBWriteBinary(fp);
  rewind(fp);

  // now check the written file for SciDB
  char isNull; // we start from the 1 byte which designates the null / not null for the whole value
  fread(&isNull,sizeof(char),1,fp);
  ASSERT_EQ(-1,isNull);
  int32_t bytesNumber=-1;
  fread(&bytesNumber,sizeof(int32_t),1,fp);
  std::cout << "Bytes number found in the binary file for SciDB: " << bytesNumber << std::endl;
  ASSERT_EQ(setBytesNumber+1,bytesNumber);
  char* receivedValue = new char[bytesNumber];
  fread(receivedValue,bytesNumber,1,fp); // received value shoule already have the \0 (null) at the end
  std::cout << "Final value found in the binary file for SciDB: " << receivedValue << std::endl;
  EXPECT_STREQ(value,receivedValue);
  // check if this is the end of the file
  char stop;
  fread(&stop,1,1,fp);
  EXPECT_TRUE(feof(fp));
  //delete value; // this is removed by destructor of the GenericAttribute<char*> class
  delete [] receivedValue;
  fclose(fp);
}
