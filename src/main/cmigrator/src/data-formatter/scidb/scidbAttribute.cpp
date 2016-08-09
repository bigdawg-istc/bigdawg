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

template<>
void SciDBAttribute<char>::write(Attribute * attr) {
	if (attr->getIsNullable()) {
		/* We have to add one byte with info if it is null or not
		 * (before the info about the length of the attribute). */
		if (attr->getIsNull()) {
			/* We don't know the reason why it is null so we'll write byte 0. */
			char nullReason = 0;
			fwrite(&nullReason, 1, 1, fp);
			/* This is indeed null, so only write 0 as the number of bytes
			 and don't write the NULL value (after 5 bytes). */
			uint32_t bytesNumber = 0;
			fwrite(&bytesNumber, 4, 1, fp);
			return;
		} else {
			/* This value is not null so the byte of nullable should have
			 * value: -1 */
			char notNull = -1;
			fwrite(&notNull, 1, 1, fp);
		}
	}
//std::cout << "Bytes number written for SciDB: " << this->bytesNumber << std::endl;
//fflush(stdout);
	uint32_t bytesNumber = attr->getBytesNumber();
	fwrite(&(bytesNumber), 4, 1, fp);
//printf("Value written for SciDB: %.13s\n",this->value);
//fflush(stdout);
	char * value = static_cast<char*>(attr->getValue());
	fwrite(value, bytesNumber, 1, fp); // +1 is for NULL \0 value at the end of the string: SciDB adds \0 at the end of each string
}

template<>
Attribute * SciDBAttribute<bool>::read() {
	//std::cout << "this is type: " << boost::typeindex::type_id().pretty_name() << std::endl;
	if (this->isNullable) {
		/* read 1 byte that tells us if the attribute is null or not:
		 - If a nullable attribute contains a non-null value,
		 the preceding null byte is -1.
		 - If a nullable attribute contains a null value,
		 the preceding null byte will contain the missing reason code,
		 which must be between 0 and 127
		 */
		int8_t nullValue;
		size_t elementsRead = fread(&nullValue, 1, 1, fp);
		//BOOST_LOG_TRIVIAL(debug) << "bytes read: " << bytes_read;
		if (elementsRead != 1) {
			std::string message(
						"Failed to read from the binary file for SciDB "
								"(read function for bool 1st call in "
								"scidbAttribute.cpp).");
			throw DataMigratorException(message);
		}
		if (nullValue >= 0) {
			this->isNull = true;
			/* We don't need the reason why it is null so we'll write byte 0. */
			/* A fixed-length data type that allows null values
			 will always consume one more byte than the datatype requires,
			 regardless of whether the actual value is null or non-null.
			 For example, an int8 will require 2 bytes and an int64
			 will require 9 bytes. (In the figure, see bytes 2-4 or 17-19.)
			 */
			if (this->value != NULL) {
				delete this->value;
				this->value = NULL;
			}
			return this;
		} else {
			/* if nullValue != -1: it means that there was another unexpected value
			 different from [-1,127] */
			assert(nullValue == -1);
		}
	}
	char boolValue;
	size_t elements_read = fread(&boolValue, this->bytesNumber, 1, fp);
	if (elements_read != 1) {
		std::string message("Failed to read from the binary file "
				"for SciDB for a bool value "
				"(read function 3nd call in scidbAttribute.cpp).");
		throw DataMigratorException(message);
	}
	//BOOST_LOG_TRIVIAL(debug) << "elements_read: " << elements_read;
	//BOOST_LOG_TRIVIAL(debug) << "bytes number in the attribute: " << this->bytesNumber;
	//BOOST_LOG_TRIVIAL(debug) << "value in the binary file: " << boolValue;
	if (this->value == NULL) {
		this->value = new bool;
	}
	if (boolValue == 1) {
		*(this->value) = true;
	} else if (boolValue == '\0') {
		*(this->value) = false;
	} else {
		std::string message(
				"Unrecognized value for bool attribute in the binary format: "
						+ boolValue);
		std::cerr << message << std::endl;
		//BOOST_LOG_TRIVIAL(error) << msg;
		throw DataMigratorException(message);
	}
	return this; // everything is correct: success
}
