#include "scidb.h"

#include <boost/lexical_cast.hpp>
#include <endian.h>
#include <stdint.h>
#include <stdlib.h>
#include <cstdio>
#include <cstring>
#include <map>
#include <utility>

#define __STDC_FORMAT_MACROS
#include <inttypes.h>

#include "../common/formatterExceptions.h"
#include "attribute.h"
#include "scidbAttribute.h"

#define __STDC_FORMAT_MACROS

SciDB::~SciDB() {
}

SciDB::SciDB(const std::string & fileName, const char * fileMode) :
		Format(fileName, fileMode) {
	setTypeAttributeMap();
}

SciDB::SciDB() :
		Format() {
}

void SciDB::setTypeAttributeMap() {
	cleanTypeAttributeMap();
	printf("set type attribute map for SciDB\n");
	/* int32_t type */
	typeAttributeMap.insert(
			std::make_pair("int32_t", new SciDBAttribute<int32_t>(fp, false)));
	typeAttributeMap.insert(
			std::make_pair("int32_t" + nullString,
					new SciDBAttribute<int32_t>(fp, true)));
	/* string type */
	typeAttributeMap.insert(
			std::make_pair("string", new SciDBAttribute<char>(fp, false)));
	typeAttributeMap.insert(
			std::make_pair("string" + nullString,
					new SciDBAttribute<char>(fp, true)));
	/* bool type */
	typeAttributeMap.insert(
			std::make_pair("bool", new SciDBAttribute<bool>(fp, false)));
	typeAttributeMap.insert(
			std::make_pair("bool" + nullString,
					new SciDBAttribute<bool>(fp, true)));
}

bool SciDB::isTheEnd() {
	int c; // note: int, not char, required to handle EOF
	if ((c = fgetc(fp)) != EOF) { // standard C I/O file reading
		ungetc(c, fp);
		return false;
	} else {
		return true;
	}
}
