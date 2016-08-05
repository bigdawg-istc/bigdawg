#ifndef CSV_H
#define CSV_H

#include <string>

#include "../common/formatterExceptions.h"
#include "../format/format.h"

class Csv: public Format {
public:
	const static char defaultDelimiter;

	~Csv() {
	}

	/**
	 * Initialize the Csv formatter.
	 *
	 * @mode read or write (mode how to open the file)
	 * @fileName the name of the file
	 * @fileMode "r" read, "w" write
	 */
	Csv(const std::string & fileName, const char * fileMode);

	Csv();

	virtual void setTypeAttributeMap();

	virtual bool isTheEnd() {
		throw DataMigratorNotImplementedException(
				"The reading from csv source not implemented!");
	}
};

#endif // CSV_H
