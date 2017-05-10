/*
 * It takes input (source) and output (destination).
 * The the data is trasformed from the source binary format 
 * to the destination binary format.
 */
#ifndef FORMATTER_H
#define FORMATTER_H

#include <vector>

#include "../attribute/attributeWrapper.h"
#include "../format/format.h"

class Formatter {

public:
	/**
	 * @param source the source format
	 * @param attributes wrapper for the attributes which will be transformed
	 * @param dest the destination format
	 */
	static void format(Format & source,
			std::vector<AttributeWrapper*> & attributes, Format & dest);
};

#endif // FORMATTER_H
