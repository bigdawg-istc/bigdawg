#include "attribute.h"

/* this is the definition of null(s) for values that are empty
 * (for example in SciDB binary format) */
const char Attribute::nullValues[Attribute::nullValuesSize] = { '\0', '\0',
		'\0', '\0', '\0', '\0', '\0', '\0', '\0', '\0', '\0', '\0', '\0', '\0',
		'\0', '\0', '\0', '\0', '\0', '\0' };
