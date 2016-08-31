#ifndef VERTICA_ATTRIBUTE_H
#define VERTICA_ATTRIBUTE_H

#include <endian.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <iostream>

#define __STDC_FORMAT_MACROS
#include <inttypes.h>

#include "../common/endianness.h"
#include "../common/formatterExceptions.h"
#include "../attribute/attribute.h"

template<class T>
class VerticaAttribute: public GenericAttribute<T> {

private:
	/** Number of bytes in the file header. */
	int32_t headerSize;

	/** Number of bytes (for values in a row) stored in the row header. */
	int32_t rowSize;

public:
	VerticaAttribute(const VerticaAttribute & obj);
	virtual ~VerticaAttribute();
	VerticaAttribute(FILE * fp, bool isNullable = false);
	virtual VerticaAttribute * clone() const;

	/** read the attribute value from the source file */
	virtual Attribute * read();

	/** write the attribute value to a source file */
	virtual void write(Attribute * attribute);
};

template<class T>
VerticaAttribute<T>::VerticaAttribute(const VerticaAttribute &obj) :
		GenericAttribute<T>(obj) {
	printf("Copy constructor vertica T attribute.\n");
}

template<class T>
VerticaAttribute<T>::~VerticaAttribute() {
	printf("Freeing memory vertica attribute!\n");
}

template<class T>
VerticaAttribute<T> * VerticaAttribute<T>::clone() const {
	printf("%s", "clone vertica\n");
	return new VerticaAttribute(*this);
}

template<class T>
VerticaAttribute<T>::VerticaAttribute(FILE * fp, bool isNullable) :
		GenericAttribute<T>(fp, isNullable) {
	printf("Create a brand new vertica attribute!\n");
}

template<class T>
Attribute * VerticaAttribute<T>::read() {
	std::string message(
			"Vertica does not suppot export of data in native binary format.");
	throw DataMigratorException(message);
}

template<class T>
void VerticaAttribute<T>::write(Attribute * attr) {
	this->bytesNumber = attr->getBytesNumber();
	//	printf("attr is null: %d\n", attr->getIsNull());
	/* Reset the null value for the attribute. */
	this->isNull = false;
	if (attr->getIsNullable()) {
		if (attr->getIsNull()) {
			/* Columns containing NULL values do not have any data in the row.
			 * If column 3 has a NULL value, then column 4's data immediately
			 * follows the end of column 2's data. */
			this->isNull = true;
			/* This has to be the end of the writing to the binary
			 vertica. */
			return;
		}
	}
	/* It copies only the pointer. */
	T* value = static_cast<T*>(attr->getValue());
	printf("value of the int: %d\n", *value);
	/* Generate bytes that have to be written at the end. */
	this->bufferSize = sizeof(T);
	this->buffer = new char[this->bufferSize];
	memcpy(this->buffer, value, this->bufferSize);
	//	std::cout << "this buffer address: " << this->buffer << std::endl;
	//	std::cout << "this buffer value: " << *(this->buffer) << std::endl;
	//	std::cout << "this value address:  " << this->value << std::endl;
	//	std::cout << "this value: " << *(this->value) << std::endl;
	//fflush(stdout);
}

/* implementation of the template specialization can be found in
 * verticaAttribute.cpp file */

template<>
Attribute * VerticaAttribute<char>::read();

template<>
void VerticaAttribute<char>::write(Attribute * attr);

template<>
VerticaAttribute<char>::VerticaAttribute(const VerticaAttribute<char> &obj);

template<>
Attribute * VerticaAttribute<bool>::read();

template<>
void VerticaAttribute<bool>::write(Attribute * attr);

#endif // VERTICA_ATTRIBUTE_H

