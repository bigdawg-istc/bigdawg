#ifndef VERTICA_H
#define VERTICA_H

#include <string>
#include <vector>

#include "../format/format.h"
#include "verticaAttribute.h"
#include <inttypes.h>

class Vertica: public Format {

private:

	/** Position of the current row header in the file
	 * from the beginning of the file. */
	long int rowHeaderPosition;

public:
	virtual ~Vertica();

	/**
	 * Initialize the SciDB format.
	 *
	 * @mode read or write (mode how to open the file)
	 * @fileName the name of the file
	 * @fileMode "r" read, "w" write
	 */
	Vertica(const std::string & fileName, const char* fileMode);

	Vertica();

	/** Write the file header for Vertica binary format. */
	virtual void writeFileHeader();

	virtual void setTypeAttributeMap();

	virtual bool isTheEnd();

	virtual void writeRowHeader();

	virtual void writeRowFooter();

	/** Set the bit in the bit vector for a row to denote that
	 * this value (for this attribute) is null.
	 * @param nullPostitions - sorted numbers of the attributes which should
	 * be set to 1 - to denote null. */
	void setAttributesNull(const std::vector<int32_t> & nullPositions);
};

#endif // VERTICA_H
