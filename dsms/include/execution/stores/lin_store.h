#ifndef _LIN_STORE_
#define _LIN_STORE_

#ifndef _TUPLE_
#include "execution/internals/tuple.h"
#endif

namespace Execution {
	class LinStore {
	public:
		
		/**
		 * Insert a tuple into the lineage synopsis stubId
		 */ 		
		virtual int insertTuple_l (Tuple tuple, Tuple *lineage,
								   unsigned int stubId) = 0;
		
		/**
		 * Delete a tuple from the lineage synopsis stubId
		 */
		
		virtual int deleteTuple_l (Tuple tuple, unsigned int stubId) = 0;
		
		/**
		 * Get the tuple with the specified lineage for the synopsis
		 * stubId
		 */		
		
		virtual int getTuple_l (Tuple *lineage, Tuple &tuple, 
								unsigned int stubId) = 0;		
		
	};
}

#endif
