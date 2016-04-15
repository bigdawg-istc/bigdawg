#ifndef ENDIANNESS_H
#define ENDIANNESS_H

#include <boost/type_traits.hpp>
#include <boost/static_assert.hpp>
#include <boost/detail/endian.hpp>
#include <stdexcept>
#include <stdint.h>
#include <endian.h>

namespace endianness {

enum ENDIANNESS_TYPE {
    little_endian,
    big_endian,
    network_endian = big_endian,

#if defined(BOOST_LITTLE_ENDIAN)
    host_endian = little_endian
#elif defined(BOOST_big_endian)
    host_endian = big_endian
#else
#error "unable to determine system endianness"
#endif
};

namespace detail {

template<typename T, size_t sz>
struct fromBigEndianToHost {
    inline T operator()(T val) {
        throw std::out_of_range("data size");
    }
};

template<typename T>
struct fromBigEndianToHost<T, 1> {
    inline T operator()(T val) {
    	return val;
    }
};

template<typename T>
struct fromBigEndianToHost<T, 2> {
    inline T operator()(T val) {
    	return reinterpret_cast<T>(be16toh(reinterpret_cast<uint16_t>(val)));
    }
};

template<typename T>
struct fromBigEndianToHost<T, 4> {
    inline T operator()(T val) {
    	return reinterpret_cast<T>(be32toh(reinterpret_cast<uint32_t>(val)));
    }
};

template<typename T>
struct fromBigEndianToHost<T, 8> {
    inline T operator()(T val) {
    	return reinterpret_cast<T>(be64toh(reinterpret_cast<uint64_t>(val)));
    }
};

template<typename T, size_t sz>
struct fromHostToBigEndian {
    inline T operator()(T val) {
        throw std::out_of_range("data size");
    }
};

template<typename T>
struct fromHostToBigEndian<T, 2> {
    inline T operator()(T val) {
    	return reinterpret_cast<T>(htobe16(reinterpret_cast<uint16_t>(val)));
    }
};

template<typename T>
struct fromHostToBigEndian<T, 4> {
    inline T operator()(T val) {
    	return reinterpret_cast<T>(htobe32(reinterpret_cast<uint32_t>(val)));
    }
};

template<typename T>
struct fromHostToBigEndian<T, 8> {
    inline T operator()(T val) {
    	return reinterpret_cast<T>(htobe64(reinterpret_cast<uint64_t>(val)));
    }
};

/**
 * check if the types and their sizes are supported
 */
template<class T>
inline void check() {
    // ensure the data is only 1, 2, 4 or 8 bytes
    // 1 was added to avoid handling special cases with if/else
    BOOST_STATIC_ASSERT(sizeof(T) == 1 || sizeof(T) == 2 || sizeof(T) == 4 || sizeof(T) == 8);
    // ensure we're only swapping arithmetic types
    BOOST_STATIC_ASSERT(boost::is_arithmetic<T>::value);
}

} // namespace detail

template<class T>
inline T from_postgres(T value) {
	detail::check<T>();
    return detail::fromBigEndianToHost<T,sizeof(T)>(value);
}

template<class T>
inline T to_postgres(T value) {
	detail::check<T>();
    return detail::fromHostToBigEndian<T,sizeof(T)>(value);
}

} // namespace endianness

#endif // #define ENDIANNESS_H
