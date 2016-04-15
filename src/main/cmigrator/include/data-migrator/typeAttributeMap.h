#ifndef TYPE_ATTRIBUTE_MAP_H
#define TYPE_ATTRBUTE_MAP_H

#include <vector>
#include <map>
#include <memory>
#include <string>

#include "attribute.h"
#include "boost/smart_ptr/shared_ptr.hpp"
#include "boost/smart_ptr/make_shared.hpp"

/** Map a given type (of an attribute) to the handler that will transform it to an internal representation and finally to any other desired format. */
class TypeAttributeMap {

  private:
    std::map<std::string,boost::shared_ptr<Attribute> > typeAttributeMap;
    boost::shared_ptr<Attribute> getAttribute(const std::string& type);

  public:
    TypeAttributeMap();
    ~TypeAttributeMap();
    void getAttributesFromTypesVector(std::vector<boost::shared_ptr<Attribute> > & attributes, const std::vector<std::string> & types);
    void getAttributesFromTypes(std::vector<boost::shared_ptr<Attribute> > & attributes, const char *types);
    void getSupportedTypes(std::vector<std::string> & supportedTypes);
};

#endif // TYPE_ATTRIBUTE_MAP_H
