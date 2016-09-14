#ifndef POSTGRES_H
#define POSTGRES_H

#include <string>

#include "../format/format.h"
#include "postgresAttribute.h"

class Postgres: public Format {

public:
	virtual ~Postgres();

	/**
	 * Initialize the PostgreSQL formatter.
	 *
	 * @mode read or write (mode how to open the file)
	 * @fileName the name of the file
	 * @fileMode "r" read, "w" write
	 */
	Postgres(const std::string & fileName, const char* fileMode);

	Postgres();

	virtual void setTypeAttributeMap();

	void readFileHeader();
	void writeFileHeader();

	void writeFileFooter();

	void writeRowHeader();

	virtual bool isTheEnd();
};

#endif // POSTGRES_H
