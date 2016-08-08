#include "csvAttribute.h"

template<>
void CsvAttribute<char>::write(Attribute * attr) {
	//printf("Write attribute for CSV char.\n");
	char * value = static_cast<char*>(attr->getValue());
	fprintf(this->fp, "%s", value);
	handleLineEnd();
}

template<>
void CsvAttribute<double>::write(Attribute * attr) {
	*(this->value) = *(static_cast<double*>(attr->getValue()));
	fprintf(this->fp, "%f", *(this->value));
	handleLineEnd();
}

template<>
void CsvAttribute<float>::write(Attribute * attr) {
	*(this->value) = *(static_cast<float*>(attr->getValue()));
	fprintf(this->fp, "%f", *(this->value));
	handleLineEnd();
}

template<>
void CsvAttribute<unsigned short int>::write(Attribute * attr) {
	*(this->value) = *(static_cast<unsigned short int*>(attr->getValue()));
	fprintf(this->fp, "%u", *(this->value));
	handleLineEnd();
}

template<>
void CsvAttribute<unsigned int>::write(Attribute * attr) {
	*(this->value) = *(static_cast<unsigned int*>(attr->getValue()));
	fprintf(this->fp, "%u", *(this->value));
	handleLineEnd();
}

template<>
void CsvAttribute<unsigned long int>::write(Attribute * attr) {
	*(this->value) = *(static_cast<unsigned long int*>(attr->getValue()));
	fprintf(this->fp, "%lu", *(this->value));
	handleLineEnd();
}
