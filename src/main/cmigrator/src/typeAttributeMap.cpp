#include "typeAttributeMap.h"
#include <boost/algorithm/string.hpp>
#include "dataMigratorExceptions.h"
#include <utility>

TypeAttributeMap::TypeAttributeMap() 
{
  std::string nullString = ":null";

  typeAttributeMap.insert(std::make_pair("int16",std::make_shared<GenericAttribute<int16_t> >(false)));
  typeAttributeMap.insert(std::make_pair("int16"+nullString,std::make_shared<GenericAttribute<int16_t> >(true)));

  typeAttributeMap.insert(std::make_pair("int16_t",std::make_shared<GenericAttribute<int16_t> >(false)));
  typeAttributeMap.insert(std::make_pair("int16_t"+nullString,std::make_shared<GenericAttribute<int16_t> >(true)));

  typeAttributeMap.insert(std::make_pair("int32_t",std::make_shared<GenericAttribute<int32_t> >(false)));
  typeAttributeMap.insert(std::make_pair("int32_t"+nullString,std::make_shared<GenericAttribute<int32_t> >(true)));

  typeAttributeMap.insert(std::make_pair("int32",std::make_shared<GenericAttribute<int32_t> >(false)));
  typeAttributeMap.insert(std::make_pair("int32"+nullString,std::make_shared<GenericAttribute<int32_t> >(true)));

  typeAttributeMap.insert(std::make_pair("int",std::make_shared<GenericAttribute<int32_t> >(false)));
  typeAttributeMap.insert(std::make_pair("int"+nullString,std::make_shared<GenericAttribute<int32_t> >(true)));

  typeAttributeMap.insert(std::make_pair("int64_t",std::make_shared<GenericAttribute<int64_t> >(false)));
  typeAttributeMap.insert(std::make_pair("int64_t"+nullString,std::make_shared<GenericAttribute<int64_t> >(true)));

  typeAttributeMap.insert(std::make_pair("int64",std::make_shared<GenericAttribute<int64_t> >(false)));
  typeAttributeMap.insert(std::make_pair("int64"+nullString,std::make_shared<GenericAttribute<int64_t> >(true)));

  typeAttributeMap.insert(std::make_pair("double",std::make_shared<GenericAttribute<double> >(false)));
  typeAttributeMap.insert(std::make_pair("double"+nullString,std::make_shared<GenericAttribute<double> >(true)));

  typeAttributeMap.insert(std::make_pair("float",std::make_shared<GenericAttribute<float> >(false)));
  typeAttributeMap.insert(std::make_pair("float"+nullString,std::make_shared<GenericAttribute<float> >(true)));

  typeAttributeMap.insert(std::make_pair("string",std::make_shared<GenericAttribute<char*> >(false)));
  typeAttributeMap.insert(std::make_pair("string"+nullString,std::make_shared<GenericAttribute<char*> >(true)));
}

TypeAttributeMap::~TypeAttributeMap() 
{
  //fprintf(stderr,"%s","Destructor of the TypeAttributeMap\n"); 
}

std::shared_ptr<Attribute> TypeAttributeMap::getAttribute(std::string type) 
{
  return typeAttributeMap.at(type);
}

void TypeAttributeMap::getAttributesFromTypes(std::vector<std::shared_ptr<Attribute> > & attributes,char *types) 
{
  //std::cout << "getAttributesFromTypes in typeAttributeMap.ccp" << std::endl;
  std::string typesString(types);
  //std::cout << "typesString in typeAttributeMap.cpp: " << typesString << std::endl;
  std::vector<std::string> typesDivided;
  boost::split(typesDivided,typesString,boost::is_any_of(","));
  //std::cout << "typesDivided vector: ";
  // for (std::vector<std::string>::const_iterator it = typesDivided.begin(); it != typesDivided.end(); ++it)
  //   {
  //     std::cout << *it << " ";
  //   }
  // std::cout << std::endl;
  for (std::vector<std::string>::iterator it=typesDivided.begin();it!=typesDivided.end();++it) 
    {
      try {
	attributes.push_back(this->getAttribute(*it));
      } catch (const std::out_of_range& oor) {
	std::string msg = "Unrecognized type: "+(*it)+"\n"+"Message from the original exception: Out of range error "+oor.what()+"\n";
	throw TypeAttributeMapException(msg);
      }
    }
}

void TypeAttributeMap::getSupportedTypes(std::vector<std::string> & supportedTypes) 
{
  for(std::unordered_map<std::string,std::shared_ptr<Attribute> >::iterator it=typeAttributeMap.begin();it != typeAttributeMap.end();++it) 
    {
      supportedTypes.push_back(it->first);
    }
}

