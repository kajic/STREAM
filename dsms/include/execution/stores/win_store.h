#ifndef _WIN_STORE_
#define _WIN_STORE_

/**
 * @file         win_syn_store.h
 * @date         June 2, 2004
 * @brief        Store that supports window synopsis interface
 */

#ifndef _TUPLE_
#include "execution/internals/tuple.h"
#endif

#ifndef _TYPES_
#include "common/types.h"
#endif

namespace Execution {
	
	class WinStore {
	public:
		
		virtual int insertTuple_w (Tuple tuple, Timestamp timestamp,
								   unsigned int stubId) = 0;
		
		virtual bool isEmpty_w (unsigned int stubId) const = 0;
		
		virtual int getOldestTuple_w (Tuple &tuple,
									  Timestamp &timestamp, 
									  unsigned int stubId) const = 0;
		
		virtual int deleteOldestTuple_w (unsigned int stubId) = 0;
	};
}

#endif
