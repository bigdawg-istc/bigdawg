#ifndef FORMAT_H
#define FORMAT_H

#include <map>
#include <vector>
#include "boost/smart_ptr/shared_ptr.hpp"
#include "boost/smart_ptr/make_shared.hpp"
#include "attribute.h"
#include "attributeWrapper.h"
#define __STDC_FORMAT_MACROS
#include <inttypes.h>

/**
 * The interface for the supported formats.
 */
class Format {

protected:
	/** Just a value with only nulls to be written to a binary file where
	 * more than one null value is needed. */
	const static std::string nullString;

	/** The attributes for a given row. */
	std::vector<AttributeWrapper*> attributes;

	/** Map type names to attributes. */
	std::map<std::string, Attribute *> typeAttributeMap;

	/** File pointer where the data is read from / written to. */
	FILE * fp;

	/** Set the file for the formatter. */
	void setFile(const std::string & fileName, const char * fileMode);

public:

	/**
	 * Initialize the file format.
	 *
	 * @param fileName the name of the file for reading/writing
	 * @param fileMode file mode is either "r"(read) or "w"(write)
	 * */
	Format(const std::string & fileName, const char * fileMode);

	/** Construct format without any initialization - we simply know which
	 * Formatter should be used and initialize it later on. */
	Format();

	virtual ~Format();

	virtual inline void readFileHeader() {
	}

	virtual inline void writeFileHeader() {
	}

	virtual void readFileFooter() {
	}

	virtual void writeFileFooter() {
	}

	virtual inline void readRowHeader() {
	}

	virtual inline void writeRowHeader() {
	}

	virtual inline void readRowFooter() {
	}

	virtual inline void writeRowFooter() {
	}

	/** Check if it is the end of the input binary file. */
	virtual bool isTheEnd() = 0;

	virtual void setTypeAttributeMap() = 0;

	void inline setAttributes(std::vector<AttributeWrapper*> & attributes) {
		this->attributes = attributes;
	}

	void cleanTypeAttributeMap();

	/** Set the file for the formatter and update the attribute map which
	 * depends on the file. */
	void setFormat(const std::string & fileName, const char * fileMode);

	/** Get attribute for a given type name. */
	Attribute * getAttribute(const std::string& type);

	/** Get all types supported by this format.
	 *
	 * @param supportedTypes fill the vector with the names
	 * of the supported types
	 * */
	virtual void getSupportedTypes(std::vector<std::string> & supportedTypes);

};

#endif // FORMAT_H
