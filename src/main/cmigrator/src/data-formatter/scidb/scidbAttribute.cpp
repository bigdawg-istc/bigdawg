#include "scidbAttribute.h"

template<>
Attribute * SciDBAttribute<char>::read() {
	/* A string data type that allows nulls is always preceded by five bytes:
	 a null byte indicating whether a value is present
	 and four bytes indicating the string length. */
	if (this->isNullable) {
		/* read 1 byte that tells us if the attribute is null or not:
		 - If a nullable attribute contains a non-null value,
		 the preceding null byte is -1.
		 - If a nullable attribute contains a null value,
		 the preceding null byte will contain the missing reason code,
		 which must be between 0 and 127
		 */
		int8_t nullValue;
		size_t bytes_read;
		bytes_read = fread(&nullValue, 1, 1, fp);
		if (bytes_read != 1) {
			std::string message("Failed to read from the binary file for SciDB "
					"(read function 1st call for string in "
					"scidbAttribute.cpp).");
			throw DataMigratorException(message);
		}
		if (nullValue >= 0) {
			this->isNull = true;
			if (this->value != NULL) {
				delete this->value;
				this->value = NULL;
			}
			/* We don't need the reason why it is null so we'll write byte 0. */
			/* A fixed-length data type that allows null values
			 will always consume one more byte than the data type requires,
			 regardless of whether the actual value is null or non-null.
			 For example, an int8 will require 2 bytes and an int64
			 will require 9 bytes. (In the figure, see bytes 2-4 or 17-19.)
			 */
		} else {
			/* if nullValue != -1: it means that there was another unexpected value
			 different from [-1,127] */
			assert(nullValue == -1);
		}
	}
	/* Read the length of the string. */
	/* Number of bytes in the string. We don't use this->bytesNumber
	 * because the bytes number is not standard for strings. */
	int32_t readBytesNumber; // [re:d] bytes number
	size_t elements_read = fread(&(readBytesNumber), 4, 1, fp);
	if (elements_read != 1) {
		std::string message("Failed to read from the binary file for SciDB "
				"(read function 2nd call in scidbAttribute.cpp).");
		throw DataMigratorException(message);
	}
	if (this->isNull) {
		if (this->bytesNumber != 0) {
			std::string message(
					"The null byte indicated null value but the string size "
							"is different than zero!");
			std::cerr << message << std::endl;
			throw DataMigratorException(message);
		}
		return this;
	}
	/* Prepare string buffer for the new value. */
	if (this->value != NULL) {
		delete this->value;
	}
	this->value = new char[readBytesNumber];
	/* Set the read bytes number. */
	this->bytesNumber = readBytesNumber;
	elements_read = fread(this->value, readBytesNumber, 1, fp);
	if (elements_read != 1) {
		std::string message("Failed to read from the binary file for SciDB "
				"(read function 3nd call in scidbAttribute.cpp).");
		throw DataMigratorException(message);
	}
	return this;
}
