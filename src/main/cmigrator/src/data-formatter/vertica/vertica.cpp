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
	/* Precondition: the fp is set to the beginning of the place where
	 * we can write the nullPositions, just after the field representing
	 * the size of the line. */

	/* Which byte from the bitVector for nulls should be modified.
	 * Positions: 0-7 are in the first byte, positions 8-15 are in the second byte,
	 * and so on. */
	/* The last byte to which we set the position. */
	int32_t lastSetByte = 0;
	uint8_t byte = 0; /* Byte in which we set the null values. */
	/* Prepare the array of bytes for the null values.
	 * The array will be written to the file at the end of the processing. */
	size_t nullVectorSize = (size_t)ceil((double)attributes.size()/8.0);
	printf("null vector size: %ld\n", nullVectorSize);
	uint8_t * nullVector = new uint8_t[nullVectorSize];

	for (std::vector<int32_t>::const_iterator it = nullPositions.begin();
			it != nullPositions.end();) {
		/* Fish out the value: (*it) represents the integer position. */
		int32_t nextPosition = *it;
		int32_t byteNumberForNextPosition = nextPosition / 8;
		if (byteNumberForNextPosition != lastSetByte) {
			/* The first column was set on the least significant bit of
			 * the byte so now we have to reverse the order. */
			//byte = reverseBitsInByte(byte);
			/* Write the byte to the output file. */
			//fwrite(&byte, sizeof(uint8_t), 1, this->fp);
			nullVector[lastSetByte] = byte;
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
				/* Move fp forward to byteNumberForNextPosition.
				 * We can move the file position forward even for pipes!
				 * */
				//moveFilePosition(fp, (byteNumberForNextPosition - lastSetByte));
				lastSetByte = byteNumberForNextPosition;
			}
		}
		/* Normalize the position, e.g. bit 10 is set to (15-10=5) the 2nd
		 * position within a byte: 0,[1],2,3,4,5,6,7. */
		uint8_t nextPositionNormalized = (uint8_t)(
				(((byteNumberForNextPosition + 1) * 8) - 1) - nextPosition);
		assert(nextPositionNormalized < 8);
		/* Set next bit. */
		byte = byte | (uint8_t)(1 << nextPositionNormalized);
		++it;
		if (it == nullPositions.end()) {
			/* Write the last byte. */
			//fwrite(&byte, sizeof(int8_t), 1, this->fp);
			nullVector[lastSetByte] = byte;
		}
	}
	fwrite(nullVector, nullVectorSize, 1, this->fp);
	delete [] nullVector;
	/* Post-condition: file position at the beginning of the row where the \
	 * the values are stored. */
}

void Vertica::writeRowFooter() {
	/* Prepare the header of the row. */
	int32_t rowSize = 0;

	/* Gather the null positions from the attributes. */
	std::vector < int32_t > nullPositions;

	/** Iterate through the attributes/columns:
	 * 1) Sum up the total size of the attributes.
	 * 2) Get positions of the null values for the attributes. */
	int positionCounter = 0;
	for (std::vector<AttributeWrapper*>::iterator it = attributes.begin();
			it != attributes.end(); ++it, ++positionCounter) {
		/* The iterator: it - represents a pointer to the attribute operator. */
		Attribute * destination = (*it)->getDest();
		/* Add the size of this attribute to the total size of the row. */
		rowSize += destination->getBufferSize();
		if (destination->getIsNull()) {
			nullPositions.push_back(positionCounter);
		}
	}

	/* The size of the space for the row size value. */
	int32_t rowSizeSize = sizeof(int32_t);
	/* Write the row size. */
	fwrite(&rowSize, rowSizeSize, 1, fp);

	/* Write the null vector. */
	setAttributesNull (nullPositions);

	/* Write the bytes for the attributes. */
	for (std::vector<AttributeWrapper*>::iterator it = attributes.begin();
			it != attributes.end(); ++it, ++positionCounter) {
		/* The iterator: it - represents a pointer to the attribute operator. */
		Attribute * destination = (*it)->getDest();
		if (!destination->getIsNull()) {
			char* buffer = destination->getBuffer();
			int32_t bufferSize = destination->getBufferSize();
			fwrite(buffer, bufferSize, 1, fp);
			/* Free the buffer (it was used) and set its size to 0. */
			std::cout << "Free the buffer in vertica.cpp." << std::endl;
			destination->freeBuffer();
			destination->setBufferSize(0);
		}
	}
}

void Vertica::writeFileHeader() {
	//printf("Write Vertica header.\n");
	/* Write the signature of the file. */
	int8_t signature[11] = { 'N', 'A', 'T', 'I', 'V', 'E', '\n', '\377', '\r',
			'\n', '\000' };
	//std::cout << "Bytes number for signature: " << bytesNumber << std::endl;
	//std::cout << "Signature: " << signature << std::endl;
	fwrite(&signature, sizeof(signature), 1, fp);

	/* Produce the header of the file. */

	/* Leave 4 bytes to write the number of bytes in the header once you write the header. */
	uint32_t bytesWritten = 0;
	int32_t bytesWrittenSize = sizeof(int32_t);

	/* Calculate the size of the header == bytesWritten. */
	/* 1) Version number. */
	bytesWritten += (uint32_t) sizeof(int16_t);
	/* 2) One byte filler. */
	int32_t fillerSize = sizeof(int8_t);
	bytesWritten += fillerSize;
	/* 3) Meta information: number of columns. */
	uint32_t numberOfColumnsSize = (uint32_t) sizeof(int16_t);
	bytesWritten += numberOfColumnsSize;
	/* 4) Number of attributes time 4. */
	bytesWritten += (uint32_t)(sizeof(int32_t) * attributes.size());
	/* Finally, write how many bytes will be in the header. */
	fwrite(&bytesWritten, bytesWrittenSize, 1, fp);

	/* 2 bytes - version number = 1. */
	int16_t versionNumber = 1;
	fwrite(&versionNumber, sizeof(int16_t), 1, fp);

	/* 1 byte filler. */
	int8_t filler = 0;
	fwrite(&filler, fillerSize, 1, fp);

	/* 2 bytes - number of columns. */
	uint16_t colNumber = (uint16_t) attributes.size();
	fwrite(&colNumber, numberOfColumnsSize, 1, fp);

	/** Iterate through the columns and set the size of them in the
	 * binary header. */
	for (std::vector<AttributeWrapper*>::iterator it = attributes.begin();
			it != attributes.end(); ++it) {
		/* it - represents an attribute operator. */
		Attribute * destination = (*it)->getDest();
		/* 4 bytes - the size of the column. */
		int32_t colSize = destination->getBytesNumber();
		fwrite(&colSize, sizeof(int32_t), 1, fp);
	}

	/* We left fp (file position) on the position when the first row can
	 * be written. */
}

