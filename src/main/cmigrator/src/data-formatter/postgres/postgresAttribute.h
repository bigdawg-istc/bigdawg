#ifndef POSTGRES_ATTRIBUTE_H
#define POSTGRES_ATTRIBUTE_H

#include <endian.h>
#include <stdint.h>
#include <cstdio>
#include <iostream>

#define __STDC_FORMAT_MACROS
#include <inttypes.h>

#include "../common/endianness.h"
#include "../common/formatterExceptions.h"
#include "attribute.h"

template<class T>
class PostgresAttribute: public GenericAttribute<T> {

public:
	PostgresAttribute(const PostgresAttribute & obj);
	virtual ~PostgresAttribute();
	PostgresAttribute(FILE * fp, bool isNullable = false);
	virtual PostgresAttribute * clone() const;

	/** read the attribute value from the source file */
	virtual Attribute * read();

	/** write the attribute value to a source file */
	virtual void write(Attribute * attribute);
};

template<class T>
PostgresAttribute<T>::PostgresAttribute(const PostgresAttribute &obj) :
		GenericAttribute<T>(obj) {
	printf("Copy constructor postgres attribute.\n");
}

template<class T>
PostgresAttribute<T>::~PostgresAttribute() {
	printf("Freeing memory postgres attribute!\n");
}

template<class T>
PostgresAttribute<T> * PostgresAttribute<T>::clone() const {
	printf("%s", "clone postgres\n");
	return new PostgresAttribute(*this);
}

template<class T>
PostgresAttribute<T>::PostgresAttribute(FILE * fp, bool isNullable) :
		GenericAttribute<T>(fp, isNullable) {
	printf("Create a brand new postgres attribute!\n");
}

template<class T>
Attribute * PostgresAttribute<T>::read() {
	//printf("this fp: %d", this->fp);
	/* The number of bytes read for this attribute from the file. */
	int32_t readBytesNumber;
	size_t numberOfObjectsRead = fread(&readBytesNumber, 4, 1, this->fp);
	if (numberOfObjectsRead != 1) {
		std::string message(
				"Failed to read from the binary file for PostgreSQL "
						"(read function 1st call in postgresAttribute.h).");
		throw DataMigratorException(message);
	}
	readBytesNumber = be32toh(readBytesNumber);
	if (readBytesNumber == -1) {
		if (this->isNullable != true) {
			std::string message(
					"This attribute is not nullable but the binary data from "
							"the file gives a null value.");
			throw DataMigratorException(message);
		}
		// this is null and there are no bytes for the value
		this->isNull = true;
		if (this->value != NULL) {
			delete this->value;
			this->value = NULL;
		}
		return this;
	}
	if (readBytesNumber != this->bytesNumber) {
		throw new DataMigratorException(
				"The current number of bytes in the file from PostgreSQL "
						"has different value than the declared "
						"number of bytes for the attribute!");
	}
	this->isNull = false;
	if (this->value == NULL) {
		this->value = new T;
	}
	numberOfObjectsRead = fread(this->value, this->bytesNumber, 1, this->fp);
	if (numberOfObjectsRead != 1) {
		std::string message(
				"Failed to read from the binary file for PostgreSQL "
						"(read function 2nd call in postgresAttribute.h).");
		throw DataMigratorException(message);
	}
	// change the endianness
	endianness::fromBigEndianToHost < T > (*(this->value));
	return this;
}

template<class T>
void PostgresAttribute<T>::write(Attribute * attr) {
	if (attr->getIsNullable()) {
		if (attr->getIsNull()) {
			/* -1 indicates a NULL field value */
			int32_t nullValue = -1;
			/* this works in place */
			endianness::fromHostToBigEndian < int32_t > (nullValue);
			fwrite(&nullValue, 4, 1, this->fp);
			/* No value bytes follow in the NULL case. */
		}
	}
	uint32_t bytesNumber = attr->getBytesNumber();
	uint32_t attrLengthPostgres = htobe32(bytesNumber);
	fwrite(&attrLengthPostgres, 4, 1, this->fp);
	/* copy only the value */
	std::cout << attr->getValue();
	//std::cout << *(attr->getValue());
	*(this->value) = *(static_cast<T*>(attr->getValue()));
	/* fromHostToBigEndian takes parameter by reference */
	endianness::fromHostToBigEndian < T > (*(this->value));
	//BOOST_LOG_TRIVIAL(debug) << "postgresWriteBinary bytes number: " << this->bytesNumber;
	fwrite(this->value, bytesNumber, 1, this->fp);
}

template<>
Attribute * PostgresAttribute<char>::read();

template<>
void PostgresAttribute<char>::write(Attribute * attribute);

template<>
Attribute * PostgresAttribute<bool>::read();

#endif // POSTGRES_ATTRIBUTE_H

