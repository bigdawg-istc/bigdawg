#include "vertica.h"

template<>
Attribute * VerticaAttribute<char>::read() {

}

template<>
void VerticaAttribute<char>::write(Attribute * attr) {
	/** The total size of the attribute for Vertica char type is the size of
	 * the string and the 4 bytes that denote the size of the string.
	 */
	this->bytesNumber = (int32_t)sizeof(uint32_t) + attr->getBytesNumber();
}

template<>
Attribute * VerticaAttribute<bool>::read() {

}

template<>
void VerticaAttribute<bool>::write(Attribute * attr) {

}
