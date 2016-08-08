#include "postgresAttribute.h"

template<>
Attribute * PostgresAttribute<char>::read() {
	printf("Handle strings for Postgres.\n");
	/* string is handled differently than other types, e.g., the number of
	 * bytes read for a string attribute determines how many bytes we have
	 * to currently read (in other cases we can just use the default value
	 * for an attribute) */
	int32_t readBytesNumber;
	size_t numberOfObjectsRead = fread(&readBytesNumber, 4, 1, fp);
	if (numberOfObjectsRead != 1) {
		std::string message(
				"Failed to read from the binary file for PostgreSQL "
						"(read function 1st call for string in "
						"postgresAttribute.cpp).");
		throw DataMigratorException(message);
	}
	//std::cout << "value bytes number before endianness: " << valueBytesNumber << std::endl;
	readBytesNumber = be32toh(readBytesNumber);
	/* The value can be null: readBytesNumber == -1. */
	if (readBytesNumber == -1) {
		/* This is null and there are no bytes for the value. */
		this->isNull = true;
		if (this->value != NULL) {
			delete this->value;
			this->value = NULL;
		}
		return this;
	}
	/* The declared number of bytes for the attribute char is irrelevant;
	 * it can be different for each value. */
	/* +1 is for NULL \0 value at the end of the string:
	 * Postgres does not store \0 (NULL) value at the end of string in the
	 * binary format but we do it in the memory. */
	this->bytesNumber = readBytesNumber + 1;
	this->isNull = false;
	/* Prepare buffer for the string. */
	if (this->value != NULL) {
		delete this->value;
	}
	this->value = new char[this->bytesNumber];
	this->value[this->bytesNumber] = '\0';
	//std::cout << "value bytes number: " << valueBytesNumber << std::endl;
	numberOfObjectsRead = fread(this->value, readBytesNumber, 1, fp);
	if (numberOfObjectsRead != 1) {
		std::string message(
				"Failed to read from the binary file for PostgreSQL "
						"(read function 2nd call in postgresAttribute.cpp).");
		throw DataMigratorException(message);
	}
	// std::cout << "value read: " << value << std::endl;
	return this; // success
}

template<>
void PostgresAttribute<char>::write(Attribute * attr) {
	int32_t bytesNumber = attr->getBytesNumber();
	/* We don't write the \0 null at the end of a string in PostgreSQL binary
	 * format. */
	--bytesNumber;
	if (attr->getIsNullable()) {
		if (attr->getIsNull()) {
			/* -1 indicates a NULL field value */
			int32_t nullValue = -1;
			int32_t nullValuePostgres = htobe32(nullValue);
			fwrite(&nullValuePostgres, 4, 1, fp);
			/* No value bytes follow in the NULL case. */
			return;
		}
	}
	uint32_t attrLengthPostgres = htobe32(bytesNumber);
	fwrite(&attrLengthPostgres, 4, 1, fp);
	char * value = static_cast<char*>(attr->getValue());
	fwrite(value, bytesNumber, 1, fp);
}
