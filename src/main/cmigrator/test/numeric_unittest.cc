#include "gtest/gtest.h"
#include "endianness.h"
#include "attribute.h"

TEST(Postgres,numeric)
{
  std::cout << "Test numeric from Postgres." << std::endl;
  FILE * fp;
  fp = tmpfile();
  int32_t bytesNumber = sizeof(int32_t);
  // postgres writes the bytes in Big Endian format so change the value to Big Endian
  std::cout << "bytes number: " << bytesNumber << std::endl;
  endianness::fromHostToBigEndian<int32_t>(bytesNumber);
  fwrite(&bytesNumber,sizeof(int32_t),1,fp);
  int32_t testIntHostEndian = 31; // just arbitrary number 
  int32_t testIntBigEndian = testIntHostEndian;
  endianness::fromHostToBigEndian<int32_t>(testIntBigEndian);
  fwrite(&testIntBigEndian,bytesNumber,1,fp);
  // you wrote to a file and now would like to start reading from the beginnin of the file
  rewind(fp); 
  GenericAttribute<int32_t> attr = new GenericAttribute<int32_t>(false);
  attr.postgresReadBinary(fp);
  std::cout << "attribute value: " << attr.getValue() << std::endl;
  EXPECT_EQ(testIntHostEndian,attr.getValue());
  fclose(fp);
}
