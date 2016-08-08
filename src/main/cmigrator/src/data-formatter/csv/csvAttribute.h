#ifndef CSV_ATTRIBUTE_H
#define CSV_ATTRIBUTE_H

#include <cstdio>

#include "../common/formatterExceptions.h"
#include "../format/attribute.h"
#include "csv.h"

template<class T>
class CsvAttribute: public GenericAttribute<T> {

public:

	CsvAttribute(FILE * fp, bool isNullable = false);
	CsvAttribute(const CsvAttribute &obj);
	virtual ~CsvAttribute();

	CsvAttribute * clone() const;

	/** read the attribute value from the source file */
	virtual Attribute * read();

	/** write the attribute value to a source file */
	virtual void write(Attribute * attribute);
};

template<class T>
CsvAttribute<T>::CsvAttribute(FILE * fp, bool isNullable) :
		GenericAttribute<T>(fp, isNullable) {
	printf("Construct brand new Csv attribute!\n");
}

template<class T>
CsvAttribute<T> * CsvAttribute<T>::clone() const {
	printf("Clone the csv attribute\n");
	return new CsvAttribute(*this);
}

template<class T>
CsvAttribute<T>::CsvAttribute(const CsvAttribute &obj) :
		GenericAttribute<T>(obj) {
	printf("Copy constructor csv attribute.\n");
}

template<class T>
CsvAttribute<T>::~CsvAttribute() {
	printf("Freeing memory csv attribute!\n");
}

template<class T>
Attribute * CsvAttribute<T>::read() {
	throw DataMigratorNotImplementedException(
			"The reading from csv source not implemented!");
}

template<class T>
void CsvAttribute<T>::write(Attribute * attr) {
	if (attr->getIsNull())
		return;
	// write the value only if it's not NULL
	// fprintf(fp,"%" PRId64,this->value);
	/* copy only the value, not a pointer */
	*(this->value) = *(static_cast<T*>(attr->getValue()));
	fprintf(this->fp, "%d", *(this->value));
	if (this->isLast) {
		fprintf(this->fp, "%c", '\n');
	} else {
		fprintf(this->fp, "%c", Csv::defaultDelimiter);
	}
}

#endif // CSV_ATTRIBUTE_H
