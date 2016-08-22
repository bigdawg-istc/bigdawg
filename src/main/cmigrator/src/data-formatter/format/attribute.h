#ifndef ATTRIBUTE_H
#define ATTRIBUTE_H

#include <stdint.h>
#include <cstdio>

#define __STDC_FORMAT_MACROS
#include <inttypes.h>

/**
 * This class represents a generic attribute.
 */
class Attribute {

protected:
	/* Can the argument be a NULL value. */
	bool isNullable;

	/** Position of the attribute in a row (position starts from 0). */
	int32_t position;

	/* Is it the last attribute in a sequence of attributes for a line. */
	bool isLast;

	/** How many bytes the attribute requires
	 this is only the number of real values (without any NULL values, for example, for char*)
	 but PostgreSQL can set it to -1 so this is why it is int and not unsigned int. */
	uint32_t bytesNumber;

	/* is this instance of the argument a NULL value or not */
	bool isNull;

	/* null values have to have null char '\0' on each position */
	static const int16_t nullValuesSize = 20;

	/* this is the declaration of null(s) for values that are empty (for example in SciDB binary format) - this is a string with only '\0' (null) values */
	static const char nullValues[nullValuesSize];

	/** File pointer where the data is read/written. */
	FILE * fp;

public:
	Attribute(FILE * fp, bool isNullable) {
		this->isNullable = isNullable;
		this->fp = fp;
		this->isNull = false;
		this->bytesNumber = 0;
		this->isLast = false;
	}

	Attribute(const Attribute & obj) {
		printf("clone in attribute.h\n");
		this->isNullable = obj.isNullable;
		this->fp = obj.fp;
		this->isNull = obj.isNull;
		this->bytesNumber = obj.bytesNumber;
		this->isLast = obj.isLast;
		this->position = obj.position;
	}

	virtual ~Attribute() {
	}

	virtual Attribute* clone() const = 0;

	/** set/indicate if this attribute can be null */
	void setIsNullable(bool isNullable) {
		this->isNullable = isNullable;
	}

	/** can this attribute be a null */
	bool getIsNullable() const {
		return this->isNullable;
	}

	void setBytesNumber(int32_t bytesNumber) {
		this->bytesNumber = bytesNumber;
	}

	/** set that this attribute is a null attribute */
	void setIsNull(bool isNull) {
		this->isNull = isNull;
	}

	/** check if this attribute has a null value */
	bool getIsNull() const {
		return this->isNull;
	}

	/** set this attribute as the last in a sequence of attributes */
	void setIsLast(bool isLast) {
		this->isLast = isLast;
	}

	/** is the attribute the last one in a sequence */
	bool getIsLast() const {
		return isLast;
	}

	/** Set the position of the attribute in a row
	 * (position number starts from 0). */
	void setPosition(int32_t position) {
		this->position = position;
	}

	/** @retrun The position of the attribute in a row. */
	int32_t getPosition() {
		return position;
	}

	/** Get the initial size of the attribute or the final size of
	 * the attribute if the value of the attribute was read or written. */
	int32_t getBytesNumber() const {
		return this->bytesNumber;
	}

	/** get the value of this attribute */
	virtual void * getValue() = 0;

	/** read the attribute value from the source file */
	virtual Attribute * read() = 0;

	/** write the attribute value to a source file;
	 * it cannot be const attribute because we pass the value
	 *  */
	virtual void write(Attribute * attribute) = 0;
};

template<class T>
class GenericAttribute: public Attribute {

protected:
	/** The value of the attribute. */
	T * value;

public:

	GenericAttribute(FILE * fp, bool isNullable) :
			Attribute(fp, isNullable) {
		printf("Allocate memory generic attribute!\n");
		this->bytesNumber = sizeof(T);
		this->value = new T;
	}

	GenericAttribute(const GenericAttribute & obj) :
			Attribute(obj) {
		printf("copy constructor generic attribute!\n");
		fflush (stdout);
		this->value = new T;
		*(this->value) = *(obj.value);
	}

	virtual ~GenericAttribute() {
		printf("Delete the generic attribute (is nullable: %d )\n",
				this->isNullable);
		if (this->value != NULL) {
			delete this->value;
			this->value = NULL;
		}
		printf("Freeing memory generic attribute, the end!\n");
	}

	/** get the value of the attribute */
	void * getValue() {
		return value;
	}

	virtual GenericAttribute<T> * clone() const = 0;

};

#endif // ATTRIBUTE_H
