#include "vertica.h"

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
#include "verticaAttribute.h"
#include "../common/utils.h"

#define __STDC_FORMAT_MACROS

Vertica::~Vertica() {
}

Vertica::Vertica(const std::string & fileName, const char * fileMode) :
		Format(fileName, fileMode) {
	setTypeAttributeMap();
}

Vertica::Vertica() :
		Format() {
}

void Vertica::setTypeAttributeMap() {
	cleanTypeAttributeMap();
	printf("set type attribute map for Vertica\n");
	/* int32_t type */
	typeAttributeMap.insert(
			std::make_pair("int32_t",
					new VerticaAttribute<int32_t>(fp, false)));
	typeAttributeMap.insert(
			std::make_pair("int32_t" + nullString,
					new VerticaAttribute<int32_t>(fp, true)));
	/* int type */
	typeAttributeMap.insert(
			std::make_pair("int", new VerticaAttribute<int32_t>(fp, false)));
	typeAttributeMap.insert(
			std::make_pair("int" + nullString,
					new VerticaAttribute<int32_t>(fp, true)));
	/* int64 type */
	typeAttributeMap.insert(
			std::make_pair("int64", new VerticaAttribute<int64_t>(fp, false)));
	typeAttributeMap.insert(
			std::make_pair("int64" + nullString,
					new VerticaAttribute<int64_t>(fp, true)));
	/* string type */
	typeAttributeMap.insert(
			std::make_pair("string", new VerticaAttribute<char>(fp, false)));
	typeAttributeMap.insert(
			std::make_pair("string" + nullString,
					new VerticaAttribute<char>(fp, true)));
	/* bool type */
	typeAttributeMap.insert(
			std::make_pair("bool", new VerticaAttribute<bool>(fp, false)));
	typeAttributeMap.insert(
			std::make_pair("bool" + nullString,
					new VerticaAttribute<bool>(fp, true)));
	/* double type */
	typeAttributeMap.insert(
			std::make_pair("double", new VerticaAttribute<double>(fp, false)));
	typeAttributeMap.insert(
			std::make_pair("double" + nullString,
					new VerticaAttribute<double>(fp, true)));
}

bool Vertica::isTheEnd() {
	int c; // note: int, not char, required to handle EOF
	if ((c = fgetc(fp)) != EOF) { // standard C I/O file reading
		ungetc(c, fp);
		return false;
	} else {
		return true;
	}
}

/**
 * The null positions of the attributes should be set in bulk.
 */
void Vertica::setAttributesNull(const std::vector<int32_t> & nullPositions) {
	/* Precondition: the fp is set to the beginning of the line. */
	long int firstFilePosition = getCurrentFilePosition(fp);
	/* Skip the first 4 bytes for the row size (move forward). */
	moveFilePosition(fp, sizeof(int32_t));
	/* Which byte from the bitVector for nulls should be modified.
	 * Positions: 0-7 are in the first byte, positions 8-15 are in the second byte,
	 * and so on. */
	/* The last byte to which we set the position. */
	int32_t lastSetByte = 0;
	int8_t byte = 0; /* Byte in which we set the null values. */
	for (std::vector<int32_t>::const_iterator it = nullPositions.begin();
			it != nullPositions.end();) {
		/* fish out the value: (*it) represents the integer position. */
		int32_t nextPosition = *it;
		int32_t byteNumberForNextPosition = nextPosition / 8;
		if (byteNumberForNextPosition != lastSetByte) {
			/* The first column was set on the least significant bit of
			 * the byte so now we have to reverse the order. */
			byte = reverseBitsInByte(byte);
			/* Write the byte to the output file. */
			fwrite(&byte, sizeof(int8_t), 1, this->fp);
			/* Reinitialize the byte. */
			byte = 0;
			/* The fp was moved by 1 byte, so we move the lastSetByte by 1. */
			++lastSetByte;
			/* If next byte to be set is greater than lastSetByte then move fp. */
			if (byteNumberForNextPosition < lastSetByte) {
				std::string message =
						"Positions to set null in Vertica should be sorted "
								"in ascendent order.";
				throw DataMigratorException(message);
			}
			if (byteNumberForNextPosition > lastSetByte) {
				/* Move fp forward to byteNumberForNextPosition. */
				moveFilePosition(fp, (byteNumberForNextPosition - lastSetByte));
				lastSetByte = byteNumberForNextPosition;
			}
		}
		/* Normalize the position, e.g. bit 10 is set to (10-8=2) to 2nd
		 * position within a byte: 0,1,[2],3,4,5,6,7. */
		nextPosition -= (byteNumberForNextPosition) * 8;
		assert(nextPosition < 8);
		/* Set next bit. */
		byte = byte | (int8_t)(1 << nextPosition);
		++it;
		if (it == nullPositions.end()) {
			/* Write the last byte. */
			fwrite(&byte, sizeof(int8_t), 1, this->fp);
		}
	}
	/* Move file position to the beginning of the row. */
	moveToPreviousPosition(fp, firstFilePosition);
}

void Vertica::writeRowHeader() {

}

void Vertica::writeRowFooter() {

}



