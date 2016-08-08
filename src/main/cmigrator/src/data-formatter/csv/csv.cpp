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
	/* type: int32_t */
	typeAttributeMap.insert(
			std::make_pair("int32_t", new CsvAttribute<int32_t>(fp, false)));
	typeAttributeMap.insert(
			std::make_pair("int32_t" + nullString,
					new CsvAttribute<int32_t>(fp, true)));
	/* type: char (or string) */
	typeAttributeMap.insert(
			std::make_pair("string", new CsvAttribute<char>(fp, false)));
	typeAttributeMap.insert(
			std::make_pair("string" + nullString,
					new CsvAttribute<int32_t>(fp, true)));
	/* type: bool */
	typeAttributeMap.insert(
			std::make_pair("bool", new CsvAttribute<bool>(fp, false)));
	typeAttributeMap.insert(
			std::make_pair("bool" + nullString,
					new CsvAttribute<bool>(fp, true)));
}
