#ifndef _TUPLE_ITER_
#define _TUPLE_ITER_

/**
 * @file      tuple_iter.h
 * @date      May 30, 2004
 * @brief     Interface of a tuple iterator.
 */

#ifndef _TUPLE_
#include "execution/internals/tuple.h"
#endif

namespace Execution {
	class TupleIterator {
	public:
		virtual ~TupleIterator () {}
		
		/**
		 * Get the next tuple in this iteration
		 *
		 * @param   tuple   (output) next tuple
		 * @return           true if next tuple present,
		 *                   false if we reached the end.
		 */ 
		virtual bool getNext (Tuple& tuple) = 0;
	};
}

#endif
