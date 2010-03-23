#ifndef _SINK_
#define _SINK_

/**
 * @file     sink.h
 * @date     Dec. 6, 2004
 * @brief    The sink operator - an operator that consumes its input
 *           but produces no output
 */

#include <ostream>
using std::ostream;

#ifndef _OPERATOR_
#include "execution/operators/operator.h"
#endif

#ifndef _QUEUE_
#include "execution/queues/queue.h"
#endif

#ifndef _STORE_ALLOC_
#include "execution/stores/store_alloc.h"
#endif

namespace Execution {
	class Sink : public Operator {
	private:
		/// System-wide unique id
		unsigned int   id;

		/// System log
		ostream &LOG;
		
		/// Input queue
		Queue *inputQueue;
		
		/// Storage allocator who allocated tuples in the input
		StorageAlloc *inStore;
		
	public:
		Sink (unsigned int id, ostream &LOG);
		virtual ~Sink ();

		//------------------------------------------------------------
		// Initialization
		//------------------------------------------------------------
		
		int setInputQueue (Queue *inputQueue);
		int setInStore (StorageAlloc *store);
		int run (TimeSlice timeSlice);
	};
}

#endif
