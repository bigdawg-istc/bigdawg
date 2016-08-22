#include "vertica.h"

template<>
Attribute * VerticaAttribute<char>::read() {

}

template<>
void VerticaAttribute<char>::write(Attribute * attr) {

}

template<>
Attribute * VerticaAttribute<bool>::read() {

}

template<>
void VerticaAttribute<bool>::write(Attribute * attr) {

}
