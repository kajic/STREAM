#ifndef _SELECT_
#define _SELECT_

/**
 * @file         select.h
 * @date         May 30, 2004
 * @brief        Selection operator
 */

#ifndef _OPERATOR_
#include "execution/operators/operator.h"
#endif

#ifndef _QUEUE_
#include "execution/queues/queue.h"
#endif

#ifndef _BEVAL_
#include "execution/internals/beval.h"
#endif

#ifndef _EVAL_CONTEXT_
#include "execution/internals/eval_context.h"
#endif

#ifndef _CPP_OSTREAM
#include <ostream>
#endif

#ifndef _STORE_ALLOC_
#include "execution/stores/store_alloc.h"
#endif

namespace Execution {
	class Select : public Operator {
	private:
		/// System-wide id
		unsigned int id;

		/// System log
		std::ostream &LOG;
		
		/// Input queue
		Queue *inputQueue;
		
		/// Output queue
		Queue *outputQueue;

		/// "Context" in which the selection predicate is evaluated. [[
		/// point to some general description of evaluation contexts ]]
		EvalContext *evalContext;
		
		/// The selection predicate
		BEval *predicate;

		/// Storeage alloc who allocs the input tuples
		StorageAlloc *inStore;
		
		/// Timestamp of the last element dequeued from input queue.  0 if
		/// no tuple has been dequeued.  Note: an element might correspond
		/// to a data tuple or a heartbeat.
		Timestamp lastInputTs;
		
		/// Timestamp of the last element enqueued in the output queue. 0
		/// if no such element has been enqueued.
		Timestamp lastOutputTs;
		
		/// [[ Explanation ]] Note: same as INPUT_CONTEXT in inst_select.cc
		static const unsigned int INPUT_CONTEXT = 2;
		
	public:
		Select(unsigned int id, std::ostream &LOG);
		virtual ~Select();
		
		//----------------------------------------------------------------------
		// Functions for initializing state
		//----------------------------------------------------------------------
		int setInputQueue (Queue *inputQueue);
		int setOutputQueue (Queue *outputQueue);
		int setInStore (StorageAlloc *inStore);		
		int setPredicate (BEval *predicate);
		int setEvalContext (EvalContext *evalContext);
		
		int run (TimeSlice timeSlice); 
	};
}

#endif
