#include "../../src/data-formatter/common/utils.h"
#include "../../src/data-formatter/vertica/vertica.h"
#include "../../src/data-formatter/common/endianness.h"
#include "gtest/gtest.h"
#include <iomanip>

TEST(Vertia,nullBits)
{
  int32_t bytesNumber = 12;
  endianness::fromHostToBigEndian<int32_t>(bytesNumber);

  std::cout << "Test bit setting for Vertica." << std::endl;
  const char * fileName = "test_bit_set.bin";
  std::cout << "File name in the test: " << fileName << std::endl;
  Vertica * vertica = new Vertica(fileName,"w");
  std::cout << "The Vertica object was initialized." << std::endl;
  std::vector<int32_t> nullPositions;
  nullPositions.push_back(0);
  nullPositions.push_back(5);
  nullPositions.push_back(19);
  std::cout << "Set bits to denote null attributes." << std::endl;
  vertica->setAttributesNull(nullPositions);
  delete vertica;

  FILE * fp = fopen(fileName, "r");
  moveFilePosition(fp, sizeof(int32_t));
  int8_t byte;
  size_t bytesRead = fread(&byte, sizeof(int8_t), 1, fp);
  std::cout << "Bytes read: " << bytesRead << std::endl;
  assert (bytesRead == sizeof(int8_t));
  std::cout << "Byte read: " << std::hex << byte << std::endl;
  printf("Byte read: %X\n", byte);
  EXPECT_EQ(33,byte);
  bytesRead = fread(&byte, sizeof(int8_t), 1, fp);
  assert (bytesRead == sizeof(int8_t));
  EXPECT_EQ(0, byte);
  bytesRead = fread(&byte, sizeof(int8_t), 1, fp);
  assert(bytesRead == sizeof(int8_t));
  EXPECT_EQ(8, byte);
  fclose(fp);

}
