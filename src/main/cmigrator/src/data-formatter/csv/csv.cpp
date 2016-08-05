#include "csv.h"

#include <stdint.h>
#include <map>
#include <string>
#include <utility>

#include "csvAttribute.h"

const char Csv::defaultDelimiter = '|';

Csv::Csv(const std::string & fileName, const char * fileMode) :
		Format(fileName, fileMode) {
	setTypeAttributeMap();
}

Csv::Csv() :
		Format() {
}

void Csv::setTypeAttributeMap() {
	printf("set type attribute map for csv\n");
	cleanTypeAttributeMap();
	typeAttributeMap.insert(
			std::make_pair("int32_t", new CsvAttribute<int32_t>(fp, false)));
	typeAttributeMap.insert(
			std::make_pair("int32_t" + nullString,
					new CsvAttribute<int32_t>(fp, true)));
}
