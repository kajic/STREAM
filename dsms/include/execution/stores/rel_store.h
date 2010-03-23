#ifndef _REL_STORE_
#define _REL_STORE_

#ifndef _TUPLE_
#include "execution/internals/tuple.h"
#endif

#ifndef _TUPLE_ITER_
#include "execution/internals/tuple_iter.h"
#endif

namespace Execution {
	class RelStore {
	public:		
		/**
		 * Insert a tuple into the synopsis stubId
		 */
		
		virtual int insertTuple_r (Tuple tuple, unsigned int stubId)
			= 0;
		
		/**
		 * Delete the indicated tuple from the synopsis stubId
		 */
		virtual int deleteTuple_r (Tuple tuple, unsigned int stubId)
			= 0;
		
		/**
		 * Scan the *entire* contents of stubId
		 */
		virtual int getScan_r (TupleIterator *&iter, unsigned int stubId)
			= 0;

		/**
		 * Release a scan that you previously got
		 */
		virtual int releaseScan_r (TupleIterator *iter, unsigned int stubId) = 0;
	};
}

#endif
