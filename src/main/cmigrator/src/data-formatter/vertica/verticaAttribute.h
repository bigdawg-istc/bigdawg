#ifndef VERTICA_ATTRIBUTE_H
#define VERTICA_ATTRIBUTE_H

#include <endian.h>
#include <stdint.h>
#include <cstdio>
#include <iostream>

#define __STDC_FORMAT_MACROS
#include <inttypes.h>

#include "../common/endianness.h"
#include "../common/formatterExceptions.h"
#include "../format/attribute.h"

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
	printf("Copy constructor vertica attribute.\n");
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
	uint32_t bytesNumber = attr->getBytesNumber();
//	printf("attr is null: %d\n", attr->getIsNull());
	if (attr->getIsNullable()) {
		if (attr->getIsNull()) {
			// we don't know the reason why it is null so we'll write byte 0
			char nullReason = 0;
			size_t numberOfElementsWritten = fwrite(&nullReason, 1, 1,
					this->fp);
			if (numberOfElementsWritten != 1) {
				std::string message("Could not write to the file.");
				throw DataMigratorException(message);
			}
			/* check if we can fill the size of the attribute with zeros */
			assert(Attribute::nullValuesSize >= bytesNumber);
			fwrite(Attribute::nullValues, bytesNumber, 1, this->fp);
			return; /* This has to be the end of the writting to the binary vertica. */
		} else {
			char notNull = -1;
			fwrite(&notNull, 1, 1, this->fp);
		}
	}
	/* copy only the value, not a pointer */
	T* value = static_cast<T*>(attr->getValue());
//	printf("value of the int: %d\n", *value);
	fwrite(value, bytesNumber, 1, this->fp);
}

/* implementation of the template specialization can be found in
 * verticaAttribute.cpp file */

template<>
Attribute * VerticaAttribute<char>::read();

template<>
void VerticaAttribute<char>::write(Attribute * attr);

template<>
Attribute * VerticaAttribute<bool>::read();

template<>
void VerticaAttribute<bool>::write(Attribute * attr);

#endif // VERTICA_ATTRIBUTE_H

