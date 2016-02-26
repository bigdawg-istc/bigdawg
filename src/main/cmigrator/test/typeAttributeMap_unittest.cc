#include "gtest/gtest.h"

#include "typeAttributeMap.h"


TEST(TypeAttributeMapTest,getAttributesFromTypes)
{
  std::cout << "Testing typeAttributeMap" << std::endl;
  char* types = (char*)"int32_t,int32_t:null,double";
  std::cout << "types: " << types << std::endl;
  TypeAttributeMap map = TypeAttributeMap();
  std::vector<std::shared_ptr<Attribute> > attributes = std::vector<std::shared_ptr<Attribute> >();
  map.getAttributesFromTypes(attributes,types);
  for(std::vector<std::shared_ptr<Attribute> >::iterator it = attributes.begin(); it != attributes.end(); ++it) 
    {
      std::cout << "Bytes number: " << (*it)->getBytesNumber() << std::endl;
      std::cout << "Is nullable: " << (*it)->getIsNullable() << std::endl;
    }
  std::vector<std::shared_ptr<Attribute> >::iterator it = attributes.begin();
  std::cout << "size of int32_t: " << sizeof(int32_t) << std::endl;
  EXPECT_EQ(sizeof(int32_t),(*it)->getBytesNumber()); // sizeof int32_t should be 4
  EXPECT_FALSE((*it)->getIsNullable());
  ++it;
  std::cout << "size of int32_t: " << sizeof(int32_t) << std::endl;
  ASSERT_EQ(sizeof(int32_t),(*it)->getBytesNumber()); // sizeof int32_t should be 4
  ASSERT_TRUE((*it)->getIsNullable());
  ++it;
  std::cout << "size of double: " << sizeof(double) << std::endl;
  ASSERT_EQ(sizeof(double),(*it)->getBytesNumber()); // sizeof double should be 8
  ASSERT_FALSE((*it)->getIsNullable());
}




