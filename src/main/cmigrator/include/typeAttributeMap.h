#ifndef TYPE_ATTRIBUTE_MAP_H
#define TYPE_ATTRBUTE_MAP_H

#include <vector>
#include <unordered_map>
#include <string>
#include <boost/ptr_container/ptr_vector.hpp>
#include "attribute/attribute.h"

class TypeAttributeMap
{

private:
  std::unordered_map<std::string,std::shared_ptr<Attribute> > typeAttributeMap;
  std::shared_ptr<Attribute> getAttribute(std::string type);

public:
  TypeAttributeMap();
  ~TypeAttributeMap();
  void getAttributesFromTypes(std::vector<std::shared_ptr<Attribute> > & attributes,char *types);
  void getSupportedTypes(std::vector<std::string> & supportedTypes);
};

#endif // TYPE_ATTRIBUTE_MAP_H
