#ifndef ENDIANNESS_H
#define ENDIANNESS_H

#include "boost/detail/endian.hpp"
#include <algorithm>
/* endian.h is not used in this file but other file which include endianness.h expect it */
#include <endian.h>

namespace endianness {

/**
 it swaps the bytes in place!!!
 */
template<typename T>
inline void swap_bytes(T& value) {
	// could static assert that T is a POD - plain old data type
	char& raw = reinterpret_cast<char&>(value);
	std::reverse(&raw, &raw + sizeof(T));
}

#if defined(BOOST_LITTLE_ENDIAN)
//host_endian = little_endian
template<class T>
inline void fromBigEndianToHost(T& value) {
	swap_bytes(value);
}

template<class T>
inline void fromHostToBigEndian(T& value) {
	swap_bytes(value);
}
#elif defined(BOOST_big_endian)
//host_endian = big_endian
// this system works in the big endian order of bytes
template<class T> inline void fromBigEndianToHost(T& value) {}
template<class T> inline void fromHostToBigEndian(T& value) {}

#else
#error "unable to determine system endianness"
#endif

}
 // namespace endianness

#endif // #define ENDIANNESS_H
