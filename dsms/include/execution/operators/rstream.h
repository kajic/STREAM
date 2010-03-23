#ifndef _RSTREAM_
#define _RSTREAM_

/**
 * @file       rstream.h
 * @date       Sep 07, 2004
 * @brief      Rstream operator
 */

#ifndef _OPERATOR_
#include "execution/operators/operator.h"
#endif

#ifndef _QUEUE_
#include "execution/queues/queue.h"
#endif

#ifndef _REL_SYN_
#include "execution/synopses/rel_syn.h"
#endif

#ifndef _STORE_ALLOC_
#include "execution/stores/store_alloc.h"
#endif

#ifndef _EVAL_CONTEXT_
#include "execution/internals/eval_context.h"
#endif

#ifndef _AEVAL_
#include "execution/internals/aeval.h"
#endif

#include <ostream>

namespace Execution {
	class Rstream : public Operator {
	private:

		/// System-wide unique id
		unsigned int id;
		
		/// System-wide log
		std::ostream &LOG;
		
		/// Input queue
		Queue *inputQueue;
		
		/// Output queue
		Queue *outputQueue;
		
		/// Synopsis of the input relation
		RelationSynopsis *synopsis;
		
		/// Scan to get all the tuples in the input synopsis
		unsigned int scanId;
		
		/// Storage allocator for the output tuples
		StorageAlloc *outStore;

		/// Storage allocator which allocated input tuples
		StorageAlloc *inStore;
		
		/// Evaluation context for various computations
		EvalContext *evalContext;
		
		/// Evaluator that copies a tuple in the input synopsis to the
		/// output
		AEval *copyEval;
				
		/// Timestamp of the last input element
		Timestamp lastInputTs;
		
		/// Timestamp of the last output element
		Timestamp lastOutputTs;		
		
		/// Are we currently stalled? (output queue is full and we have
		/// some output to produce)
		bool bStalled;

		/// [[ Explanation ]]
		TupleIterator *scanWhenStalled;

		///
		Element stallElement;
		
		static const unsigned int INPUT_ROLE = 2;
		static const unsigned int OUTPUT_ROLE = 3;
		
	public:
		
		Rstream (unsigned int id, std::ostream &LOG);
		virtual ~Rstream ();
		
		//----------------------------------------------------------------------
		// Initialization routines
		//----------------------------------------------------------------------
		int setInputQueue (Queue *inputQueue);
		int setOutputQueue (Queue *outputQueue);
		int setSynopsis (RelationSynopsis *syn);
		int setScan (unsigned int scanId);
		int setOutStore (StorageAlloc *store);
		int setInStore (StorageAlloc *inStore);
		int setEvalContext (EvalContext *evalContext);
		int setCopyEval (AEval *eval);
		
		/// run ...
		int run (TimeSlice timeSlice);
		
	private:
		int clearStall ();
		int produceOutput ();
	};
}

#endif
		
