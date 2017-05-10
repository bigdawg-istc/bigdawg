#include "formatter.h"

#include <iterator>

const std::string Format::nullString(" null");

void Formatter::format(Format & source,
                       std::vector<AttributeWrapper*> & attributes, Format & dest) {
    source.readFileHeader();
    dest.writeFileHeader();
    /*
      Reading the rows one by one.
     */
    while (!source.isTheEnd()) {
        // process each column in a line/row/tuple
        source.readRowHeader();
        dest.writeRowHeader();

//		printf("Attributes size: %lu\n", attributes.size());
//		if (attributes.size() > 0) {
//			attributes.at(0)->toString();
//		}
        for (std::vector<AttributeWrapper*>::iterator it = attributes.begin();
             it != attributes.end(); ++it) {
//			printf("Attributes wrapper: ");
//			(*it)->toString();
            (*it)->readWrite();
        }
        source.readRowFooter();
        dest.writeRowFooter();
    }
    source.readFileFooter();
    dest.writeFileFooter();
}

