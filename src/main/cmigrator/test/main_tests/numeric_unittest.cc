//#include "../src/data-formatter/attributeOld.h"
//#include "../src/data-formatter/endianness.h"
#include "../../src/data-formatter/common/endianness.h"
#include "../../src/data-formatter/postgres/postgresAttribute.h"
#include "gtest/gtest.h"

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
  std::cout << "initial value: " << testIntHostEndian << std::endl;
  int32_t testIntBigEndian = testIntHostEndian;
  endianness::fromHostToBigEndian<int32_t>(testIntBigEndian);
  fwrite(&testIntBigEndian,bytesNumber,1,fp);
  // you wrote to a file and now would like to start reading from the beginnin of the file
  rewind(fp);
  //GenericAttribute<int32_t> attr = new GenericAttribute<int32_t>(false);
  PostgresAttribute<int32_t> attr = PostgresAttribute<int32_t>(fp, false);
  //attr.postgresReadBinary(fp);
  attr.read();
  int32_t * value = static_cast<int32_t*>(attr.getValue());
  std::cout << "attribute value: " << *value << std::endl;
  EXPECT_EQ(testIntHostEndian,*value);
  fclose(fp);
}
