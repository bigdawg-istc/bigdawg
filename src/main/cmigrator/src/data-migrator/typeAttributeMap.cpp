#include "typeAttributeMap.h"
#include "dataMigratorExceptions.h"
#include <utility>
#include "common/debuglog.h"

TypeAttributeMap::TypeAttributeMap() {
    std::string nullString = " null";

   typeAttributeMap.insert(std::make_pair("bool",boost::make_shared<GenericAttribute<bool> >(false)));
   typeAttributeMap.insert(std::make_pair("bool"+nullString,boost::make_shared<GenericAttribute<bool> >(true)));

   typeAttributeMap.insert(std::make_pair("int16",boost::make_shared<GenericAttribute<int16_t> >(false)));
   typeAttributeMap.insert(std::make_pair("int16"+nullString,boost::make_shared<GenericAttribute<int16_t> >(true)));

   typeAttributeMap.insert(std::make_pair("int16_t",boost::make_shared<GenericAttribute<int16_t> >(false)));
   typeAttributeMap.insert(std::make_pair("int16_t"+nullString,boost::make_shared<GenericAttribute<int16_t> >(true)));

   typeAttributeMap.insert(std::make_pair("int32_t",boost::make_shared<GenericAttribute<int32_t> >(false)));
   typeAttributeMap.insert(std::make_pair("int32_t"+nullString,boost::make_shared<GenericAttribute<int32_t> >(true)));

   typeAttributeMap.insert(std::make_pair("int32",boost::make_shared<GenericAttribute<int32_t> >(false)));
   typeAttributeMap.insert(std::make_pair("int32"+nullString,boost::make_shared<GenericAttribute<int32_t> >(true)));

   typeAttributeMap.insert(std::make_pair("int",boost::make_shared<GenericAttribute<int32_t> >(false)));
   typeAttributeMap.insert(std::make_pair("int"+nullString,boost::make_shared<GenericAttribute<int32_t> >(true)));
   
   typeAttributeMap.insert(std::make_pair("integer",boost::make_shared<GenericAttribute<int32_t> >(false)));
   typeAttributeMap.insert(std::make_pair("integer"+nullString,boost::make_shared<GenericAttribute<int32_t> >(true)));

   typeAttributeMap.insert(std::make_pair("int64_t",boost::make_shared<GenericAttribute<int64_t> >(false)));
   typeAttributeMap.insert(std::make_pair("int64_t"+nullString,boost::make_shared<GenericAttribute<int64_t> >(true)));

   typeAttributeMap.insert(std::make_pair("int64",boost::make_shared<GenericAttribute<int64_t> >(false)));
   typeAttributeMap.insert(std::make_pair("int64"+nullString,boost::make_shared<GenericAttribute<int64_t> >(true)));

   typeAttributeMap.insert(std::make_pair("double",boost::make_shared<GenericAttribute<double> >(false)));
   typeAttributeMap.insert(std::make_pair("double"+nullString,boost::make_shared<GenericAttribute<double> >(true)));

   typeAttributeMap.insert(std::make_pair("float",boost::make_shared<GenericAttribute<float> >(false)));
   typeAttributeMap.insert(std::make_pair("float"+nullString,boost::make_shared<GenericAttribute<float> >(true)));

   typeAttributeMap.insert(std::make_pair("string",boost::make_shared<GenericAttribute<char*> >(false)));
   typeAttributeMap.insert(std::make_pair("string"+nullString,boost::make_shared<GenericAttribute<char*> >(true)));

   typeAttributeMap.insert(std::make_pair("varchar",boost::make_shared<GenericAttribute<char*> >(false)));
   typeAttributeMap.insert(std::make_pair("varchar"+nullString,boost::make_shared<GenericAttribute<char*> >(true)));
}

TypeAttributeMap::~TypeAttributeMap() {
    //fprintf(stderr,"%s","Destructor of the TypeAttributeMap\n");
}

boost::shared_ptr<Attribute> TypeAttributeMap::getAttribute(const std::string& type) {
    //std::cout << "type: " << type << std::endl;
	VOLT_INFO("type name in typeAttributeMap: %s\n",type.c_str());
    return typeAttributeMap.at(type);
}

void TypeAttributeMap::getAttributesFromTypesVector(std::vector<boost::shared_ptr<Attribute> > & attributes, const std::vector<std::string> & types) {
    for (std::vector<std::string>::const_iterator it=types.begin(); it!=types.end(); ++it) {
        try {
            attributes.push_back(this->getAttribute(*it));
        } catch (const std::out_of_range& oor) {
            std::string msg = "Unrecognized type: "+(*it)+"\n"+"Message from the original exception: Out of range error "+oor.what()+"\n";
            throw TypeAttributeMapException(msg);
        }
    }
}

void TypeAttributeMap::getAttributesFromTypes(std::vector<boost::shared_ptr<Attribute> > & attributes, const char *types) {
    //std::cout << "getAttributesFromTypes in typeAttributeMap.ccp" << std::endl;
    const std::string typesString(types);
    VOLT_INFO("type string in TypeAttributeMap::getAttributesFromTypes: %s!END!\n",typesString.c_str());
    //std::cout << "typesString in typeAttributeMap.cpp: " << typesString << std::endl;
    const std::vector<std::string> typesDivided = splitString(typesString,',');
    //boost::split(typesDivided,typesString,boost::is_any_of(","));
    //std::cout << "typesDivided vector: ";
    for (std::vector<std::string>::const_iterator it = typesDivided.begin(); it != typesDivided.end(); ++it) {
         VOLT_INFO("type name in TypeAttributeMap::getAttributesFromTypes: %s!END!\n",(*it).c_str());
    }
    getAttributesFromTypesVector(attributes,typesDivided);
}

void TypeAttributeMap::getSupportedTypes(std::vector<std::string> & supportedTypes) {
    for(std::map<std::string,boost::shared_ptr<Attribute> >::iterator it=typeAttributeMap.begin(); it != typeAttributeMap.end(); ++it) {
        supportedTypes.push_back(it->first);
    }
}
