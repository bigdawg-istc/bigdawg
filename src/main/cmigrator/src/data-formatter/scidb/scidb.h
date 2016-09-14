#ifndef SCIDB_H
#define SCIDB_H

#include <string>

#include "../format/format.h"
#include "scidbAttribute.h"

class SciDB: public Format {

public:
	virtual ~SciDB();

	/**
	 * Initialize the SciDB format.
	 *
	 * @mode read or write (mode how to open the file)
	 * @fileName the name of the file
	 * @fileMode "r" read, "w" write
	 */
	SciDB(const std::string & fileName, const char* fileMode);

	SciDB();

	virtual void setTypeAttributeMap();

	virtual bool isTheEnd();
};

#endif // SCIDB_H
