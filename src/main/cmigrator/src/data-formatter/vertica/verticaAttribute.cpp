#include "vertica.h"

template<>
Attribute * VerticaAttribute<char>::read() {
	std::string message(
			"Vertica does not suppot export of data in native binary format.");
	throw DataMigratorException(message);
}

template<>
void VerticaAttribute<char>::write(Attribute * attr) {
	/** The total size of the attribute for Vertica char type is the size of
	 * the string and the 4 bytes that denote the size of the string.
	 */
	/* String size without final NULL \0. */
	int32_t stringSize = attr->getBytesNumber() - 1;
	this->bufferSize = (int32_t) sizeof(stringSize) + stringSize;
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
    /* Copy the value, not a pointer. */
	char* value = static_cast<char*>(attr->getValue());
	//	printf("value of the int: %d\n", *value);
	/* Generate bytes that have to be written at the end. */

	this->buffer = new char[this->bufferSize];
	memcpy(this->buffer, &stringSize, sizeof(stringSize));
	memcpy(this->buffer + sizeof(stringSize), value, stringSize);
}

template<>
Attribute * VerticaAttribute<bool>::read() {
	std::string message(
			"Vertica does not support export of data in native binary format.");
	throw DataMigratorException(message);
}

template<>
void VerticaAttribute<bool>::write(Attribute * attr) {
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
	assert(this->bytesNumber == attr->getBytesNumber());
	bool * value = static_cast<bool*>(attr->getValue());
	char boolBin;
	if (*(value) == false) {
		boolBin = '\0';
	} else {
		boolBin = 1;
	}
	this->bufferSize = sizeof(char);
	this->buffer = new char[this->bufferSize];
	this->buffer[0] = boolBin;
}

template<>
VerticaAttribute<char>::VerticaAttribute(const VerticaAttribute<char> &obj) :
		GenericAttribute<char>(obj) {
    //printf("Copy constructor vertica char attribute.\n");
	/* At the beginning the size of the string attribute is set to -1;
	 * The column width for a VARCHAR column is -1 to signal that
	 * it contains variable-length data. */
	this->bytesNumber = -1;
}
