#ifndef NVALUE_HPP_
#define NVALUE_HPP_

/**
   This is only a hookup (or mockup) that we use to be able to mirror our code in S-Store database 
   and avoid any errors during compilation as well as test our solutions separately.
 */
#include "dataMigratorExceptions.h"

namespace voltdb {
    class NValue {
    public:
	inline int getInteger() const { throw DataMigratorNotImplementedException(); }
	inline bool isNull() const { throw DataMigratorNotImplementedException(); }
    };
}

#endif /* NVALUE_HPP_ */
