#ifndef POSTGRES_ATTRIBUTE_H
#define POSTGRES_ATTRIBUTE_H

#include <endian.h>
#include <stdint.h>
#include <cstdio>

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
	uint32_t valueBytesNumber;
	printf("in postgres read\n");
	if (this->fp == NULL) {
		printf("fp is NULL");
	}
	if (this->fp == 0) {
		printf("fp is zero!!");
	}
	//printf("this fp: %d", this->fp);
	fread(&valueBytesNumber, 4, 1, this->fp);
	valueBytesNumber = be32toh(valueBytesNumber);
	if (valueBytesNumber == -1) {
		// this is null and there are no bytes for the value
		this->isNull = true;
		return this;
	}
	this->isNull = false;
	if (valueBytesNumber != this->bytesNumber) {
		throw new DataMigratorException(
				"The current number of bytes in the file from PostgreSQL "
						"has more attributes than the declared number of bytes for the attribute!");
	}
	fread(this->value, this->bytesNumber, 1, this->fp);
	// change the endianness
	endianness::fromBigEndianToHost<T>(*(this->value));
	return this;
}

template<class T>
void PostgresAttribute<T>::write(Attribute * attr) {
	if (attr->getIsNullable()) {
		if (attr->getIsNull()) {
			/* -1 indicates a NULL field value */
			int32_t nullValue = -1;
			/* this works in place */
			endianness::fromHostToBigEndian<int32_t>(nullValue);
			fwrite(&nullValue, 4, 1, this->fp);
			/* No value bytes follow in the NULL case. */
		}
	}
	uint32_t bytesNumber = attr->getBytesNumber();
	uint32_t attrLengthPostgres = htobe32(bytesNumber);
	fwrite(&attrLengthPostgres, 4, 1, this->fp);
	this->value = static_cast<T*>(attr->getValue());
	endianness::fromHostToBigEndian<T>(*(this->value));
	//BOOST_LOG_TRIVIAL(debug) << "postgresWriteBinary bytes number: " << this->bytesNumber;
	fwrite(this->value, this->bytesNumber, 1, this->fp);
}

#endif // POSTGRES_ATTRIBUTE_H

