#include "formatter.h"

#include <iterator>

const std::string Format::nullString(" null");

void Formatter::format(Format & source,
		std::vector<AttributeWrapper> attributes, Format & dest) {
	source.readFileHeader();
	dest.writeFileHeader();
	while (!source.isTheEnd()) {
		// process each column in a line
		source.readRowHeader();
		dest.writeRowHeader();
		for (std::vector<AttributeWrapper>::iterator it = attributes.begin();
				it != attributes.end(); ++it) {
			it->readWrite();
		}
		source.readRowFooter();
		source.writeRowFooter();
	}
	source.readFileFooter();
	dest.writeFileFooter();
}

