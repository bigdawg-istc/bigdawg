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
						"postgresAttribute.h).");
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
	this->isNull = false;
	if (this->value != NULL) {
		delete this->value;
	}
	this->value = new char[this->bytesNumber + 1]; // +1 is for NULL \0 value at the end of the string: SciDB adds \0 at the end of each string
	this->value[this->bytesNumber] = '\0';
	//std::cout << "value bytes number: " << valueBytesNumber << std::endl;
	numberOfObjectsRead = fread(this->value, readBytesNumber, 1, fp);
	if (numberOfObjectsRead != 1) {
		std::string message(
				"Failed to read from the binary file for PostgreSQL "
						"(read function 2nd call in postgresAttribute.h).");
		throw DataMigratorException(message);
	}
	// std::cout << "value read: " << value << std::endl;
	return this; // success
}
