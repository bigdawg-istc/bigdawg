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

private:
	void handleLineEnd();
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

/**
 * This works for typical integer (signed) values as well as for bool values.
 */
template<class T>
void CsvAttribute<T>::write(Attribute * attr) {
	if (attr->getIsNull())
		return;
	// write the value only if it's not NULL
	// fprintf(fp,"%" PRId64,this->value);
	/* copy only the value, not a pointer */
	*(this->value) = *(static_cast<T*>(attr->getValue()));
	fprintf(this->fp, "%d", *(this->value));
	handleLineEnd();
}

template<>
void CsvAttribute<char>::write(Attribute * attr);

template<>
void CsvAttribute<double>::write(Attribute * attr);

template<>
void CsvAttribute<float>::write(Attribute * attr);

template<>
void CsvAttribute<unsigned short int>::write(Attribute * attr);

template<>
void CsvAttribute<unsigned int>::write(Attribute * attr);

template<>
void CsvAttribute<unsigned long int>::write(Attribute * attr);

template<>
void CsvAttribute<int64_t>::write(Attribute * attr);

template<class T>
void CsvAttribute<T>::handleLineEnd() {
	/* If this is the last attribute in the row. */
	if (this->isLast) {
		fprintf(this->fp, "%c", '\n');
	} else {
		fprintf(this->fp, "%c", Csv::defaultDelimiter);
	}
}

#endif // CSV_ATTRIBUTE_H
