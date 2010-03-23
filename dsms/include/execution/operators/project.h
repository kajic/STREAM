#ifndef _PROJECT_
#define _PROJECT_

/**
 * @file        project.h
 * @date        May 30, 2004
 * @brief       The projection operator
 *
 * Changes:
 *
 * Sept. 7, added a lineage synopsis for the output tuples.
 */

#ifndef _OPERATOR_
#include "execution/operators/operator.h"
#endif

#ifndef _QUEUE_
#include "execution/queues/queue.h"
#endif

#ifndef _STORE_ALLOC_
#include "execution/stores/store_alloc.h"
#endif

#ifndef _LIN_SYN_
#include "execution/synopses/lin_syn.h"
#endif

#ifndef _AEVAL_
#include "execution/internals/aeval.h"
#endif

#ifndef _EVAL_CONTEXT_
#include "execution/internals/eval_context.h"
#endif

#ifndef _CPP_OSTREAM_
#include <ostream>
#endif

namespace Execution {
	/**
	 * A generic projection operator.
	 *
	 * Each attribute in the output schema is an arbitrary arithmetic
	 * function over the input attributes and constants (ok, not
	 * arbitrary, but just those that can be expressed using *, /, +, -
	 * operators.)
	 *
	 */
	class Project : public Operator {
	private:
		/// System-wide unique id
		unsigned int id;
		
		/// System log
		std::ostream &LOG;
		
		/// Input queue
		Queue *inputQueue;
		
		/// Output queue 
		Queue *outputQueue;
				
		/// Storage allocator to allocate space for output tuples
		StorageAlloc *outStore;

		/// Storage allocator which allocatedspace for input tuples
		StorageAlloc *inStore;
		
		/// Synopsis storing the output of the project.  This synopsis is
		/// needed to generate the MINUS tuples in the output.
		LineageSynopsis *outSynopsis;
		
		/// Evaluation context
		EvalContext *evalContext;
		
		/// Arithmetic evaluator that computes the output tuple attributes
		/// from the input tuple
		AEval *projEval;
		
		/// Timestamp of the last element dequeued from input queue.  0 if
		/// no tuple has been dequeued.
		Timestamp lastInputTs;
		
		/// Timestamp of the last element enqueued in the output queue. 0
		// if no such element has been enqueued.
		Timestamp lastOutputTs;
		
		/// [[ Explanation ]]
		static const unsigned int INPUT_ROLE = 2;
		
		/// [[ Explanation ]]
		static const unsigned int OUTPUT_ROLE = 3;			
		
	public:
		Project(unsigned int id, std::ostream &LOG);
		virtual ~Project();
		
		//----------------------------------------------------------------------
		// Functions for initializing state
		//----------------------------------------------------------------------
		
		int setInputQueue (Queue *inputQueue);
		int setOutputQueue (Queue *outputQueue);
		int setOutStore (StorageAlloc *storeAlloc);
		int setInStore (StorageAlloc *storeAlloc);
		int setOutSynopsis (LineageSynopsis *outSynopsis);
		int setProjEvaluator (AEval *projEval);
		int setEvalContext (EvalContext *evalContext);
		
		int run(TimeSlice timeSlice);
	};
}

#endif
