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
	/* type: int */
	typeAttributeMap.insert(
			std::make_pair("int", new CsvAttribute<int32_t>(fp, false)));
	typeAttributeMap.insert(
			std::make_pair("int" + nullString,
					new CsvAttribute<int32_t>(fp, true)));
	/* int64 type */
	typeAttributeMap.insert(
			std::make_pair("int64", new CsvAttribute<int64_t>(fp, false)));
	typeAttributeMap.insert(
			std::make_pair("int64" + nullString,
					new CsvAttribute<int64_t>(fp, true)));
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
	/*type: double */
	typeAttributeMap.insert(
			std::make_pair("double", new CsvAttribute<double>(fp, false)));
	typeAttributeMap.insert(
			std::make_pair("double" + nullString,
					new CsvAttribute<double>(fp, true)));
}
