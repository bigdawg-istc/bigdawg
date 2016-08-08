#ifndef ATTRIBUTE_WRAPPER_H
#define ATTRIBUTE_WRAPPER_H

#include "attribute.h"

/**
 * Wrap the attributes between which you pass the value.
 */
class AttributeWrapper {

protected:
	/** the list of attributes for a row for a format from which we read the data */
	Attribute * source;

	/** the list of attributes for a row for a format to which we write the data */
	Attribute * dest;

public:
	AttributeWrapper(Attribute * source, Attribute * dest) {
		this->source = source;
		this->dest = dest;
	}

	/** read an attribute value from the source file and write it to the destination */
	inline void readWrite() {
		printf("Read write in the attribute wrapper.\n");
		dest->write(source->read());
	}

	Attribute * getSource() {
		return this->source;
	}

	Attribute * getDest() {
		return this->dest;
	}

	~AttributeWrapper() {
		printf("attributes wrapper destructor \n");
		fflush (stdout);
		delete this->dest;
		delete this->source;
	}

	void toString() {
		printf("AttributeWrapper ready.\n");
	}

};

#endif // ATTRIBUTE_WRAPPER_H
