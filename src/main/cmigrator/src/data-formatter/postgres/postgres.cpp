#include "postgres.h"

#include <boost/lexical_cast.hpp>
#include <endian.h>
#include <stdlib.h>
#include <cstdio>
#include <cstring>
#include <map>
#include <utility>
#include <stdint.h>

#define __STDC_FORMAT_MACROS
#include <inttypes.h>

#include "../common/formatterExceptions.h"
#include "attribute.h"
#include "postgresAttribute.h"

#define __STDC_FORMAT_MACROS

Postgres::~Postgres() {
}

Postgres::Postgres(const std::string & fileName, const char * fileMode) :
		Format(fileName, fileMode) {
	setTypeAttributeMap();
}

Postgres::Postgres() :
		Format() {
}

void Postgres::setTypeAttributeMap() {
	cleanTypeAttributeMap();
	printf("set type attribute map for Postgres\n");
	/* int32_t type */
	typeAttributeMap.insert(
			std::make_pair("int32_t",
					new PostgresAttribute<int32_t>(fp, false)));
	typeAttributeMap.insert(
			std::make_pair("int32_t" + nullString,
					new PostgresAttribute<int32_t>(fp, true)));
	/* int type */
	typeAttributeMap.insert(
			std::make_pair("int",
					new PostgresAttribute<int32_t>(fp, false)));
	typeAttributeMap.insert(
			std::make_pair("int" + nullString,
					new PostgresAttribute<int32_t>(fp, true)));
	/* string type */
	typeAttributeMap.insert(
			std::make_pair("string", new PostgresAttribute<char>(fp, false)));
	typeAttributeMap.insert(
			std::make_pair("string" + nullString,
					new PostgresAttribute<char>(fp, true)));
	/* bool type */
	typeAttributeMap.insert(
			std::make_pair("bool", new PostgresAttribute<bool>(fp, false)));
	typeAttributeMap.insert(
			std::make_pair("bool" + nullString,
					new PostgresAttribute<bool>(fp, true)));
	/* double type */
	typeAttributeMap.insert(
			std::make_pair("double", new PostgresAttribute<double>(fp, false)));
	typeAttributeMap.insert(
			std::make_pair("double" + nullString,
					new PostgresAttribute<double>(fp, true)));
}

static const char BinarySignature[19] =
		{ 'P', 'G', 'C', 'O', 'P', 'Y', '\n', '\377', '\r', '\n', '\0', '\0',
				'\0', '\0', '\0', '\0', '\0', '\0', '\0' };

bool Postgres::isTheEnd() {
	// read 2 bytes representing number of columns stored in each line
	// we have to read the colNumber and check if it is the end of the file
	// -1 represents the end of the binary data
	int16_t colNumber;
	size_t numberOfObjectsRead = fread(&colNumber, 2, 1, fp);
	if (numberOfObjectsRead != 1) {
		std::string message(
				"Failed to read from the binary file for PostgreSQL "
						"(isTheEnd function in postgres.cpp).");
		throw DataMigratorException(message);
	}
	colNumber = be16toh(colNumber);
	if (colNumber == -1) {
		return true;
	} else if (colNumber == attributesNumber) {
		return false;
	} else {
		std::string message(
				"PostgreSQL input file. Expected number of attributes: "
						+ boost::lexical_cast < std::string
						> (attributesNumber)
								+ "; The number of attributes in the file: "
								+ boost::lexical_cast < std::string
						> (colNumber) + ".");
		throw DataMigratorException(message);
	}
}

/* read first 19 bytes */
void Postgres::readFileHeader() {
	size_t headerSize = 19;
	char buffer[headerSize];
	size_t numberOfObjectsRead = fread(buffer, headerSize, 1, fp);
	if (numberOfObjectsRead != 1) {
		std::string message(
				"Failed to read from the binary file for PostgreSQL "
						"(readFileHeader function in postgres.cpp).");
		throw DataMigratorException(message);
	}
	if (strncmp(buffer, BinarySignature, 19) != 0) {
		throw DataMigratorException(
				"Unrecognized PostgreSQL binary signature!");
	}
}

void Postgres::writeFileHeader() {
	// 19 bytes for the signature
	fwrite(&BinarySignature, 19, 1, fp);
}

/* Each tuple begins with a 16-bit integer count of the number of fields in the tuple. */
void Postgres::writeRowHeader() {
	uint16_t colNumberPostgres = htobe16(attributesNumber);
	fwrite(&colNumberPostgres, 2, 1, fp);
}

/* The file trailer consists of a 16-bit integer word containing -1.
 This is easily distinguished from a tuple's field-count word. */
void Postgres::writeFileFooter() {
	uint16_t trailer = -1;
	uint16_t trailerPostgres = htobe16(trailer);
	fwrite(&trailerPostgres, 2, 1, fp);
}
