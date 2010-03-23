#ifndef _DSTREAM_
#define _DSTREAM_

/**
 * @file             Dstream.h
 * @date             Aug. 07, 2004
 * @brief            Dstream operator
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

#ifndef _EVAL_CONTEXT_
#include "execution/internals/eval_context.h"
#endif

#ifndef _STORE_ALLOC_
#include "execution/stores/store_alloc.h"
#endif

#ifndef _AEVAL_
#include "execution/internals/aeval.h"
#endif

#ifndef _BEVAL_
#include "execution/internals/beval.h"
#endif

#include <ostream>

namespace Execution {
	class Dstream : public Operator {
	private:
		
		/// System-wide unique id
		unsigned int id;
		
		/// System-wide log
		std::ostream &LOG;
				
		/// Input queue
		Queue *inputQueue;
		
		/// Output queue
		Queue *outputQueue;
		
		/// Synopsis that stores the tuples that appeared in the current
		/// time instant and a count for each tuple.  The count for a
		/// tuple is #copies of the tuple that appears in PLUS tuples
		/// minus the number of copies of the tuple in MINUS tuples
		RelationSynopsis *synopsis;
		
		/// Scan over the synopsis to retrieve a tuple and its count
		unsigned int countScanId;
		
		/// Scan to get all tuples in a synopsis
		unsigned int fullScanId;
		
		/// Storage allocator for the tuples in the synopsis
		StorageAlloc *synStore;
		
		/// Storage allocator for the output tuples
		StorageAlloc *outStore;

		/// Storage allocator which allocs input tuples
		StorageAlloc *inStore;
		
		/// Evaluation context in which all computations are performed
		EvalContext *evalContext;
		
		/// Evaluator to incr the count for a tuple
		AEval *incrEval;
		
		/// Evaluator to decr the count for a tuple
		AEval *decrEval;
		
		/// Evaluator that initializes count to 0
		AEval *initEval;
		
		/// Evaluator that checks if the counts for a tuple is 0
		BEval *zeroEval;
		
		/// Evaluator that checks if count < 0
		BEval *negEval;
		
		/// Evaluator that produces the output tuple
		AEval *outEval;
		
		/// Timestamp of the last input element
		Timestamp lastInputTs;
		
		/// Timestamp of the last output element
		Timestamp lastOutputTs;		

		/// "current" timestamp: timestamp of the tuples that are
		/// currently in the synopsis.
		Timestamp curTs;

		/// Are we currently stalled (output queue is full and we have
		/// some output to produce)
		bool bStalled;
		
		/// [[ Explanation ]]
		Tuple synTupleWhenStalled;
		
		/// [[ Explanation ]]
		TupleIterator *scanWhenStalled;

		///
		Element stallElement;
		
		static const unsigned int INPUT_ROLE = 2;
		static const unsigned int SYN_ROLE = 3;
		static const unsigned int OUTPUT_ROLE = 4;

	public:
		Dstream (unsigned int id, std::ostream &LOG);
		virtual ~Dstream ();
		
		//----------------------------------------------------------------------
		// Initialization functions
		//----------------------------------------------------------------------
		int setInputQueue (Queue *inputQueue);
		int setOutputQueue (Queue *outputQueue);
		int setSynopsis (RelationSynopsis *syn);
		int setCountScan (unsigned int scan);
		int setFullScan (unsigned int scan);
		int setSynStore (StorageAlloc *store);
		int setOutStore (StorageAlloc *store);
		int setInStore (StorageAlloc *store);
		int setEvalContext (EvalContext *evalContext);
		int setIncrEval (AEval *eval);
		int setDecrEval (AEval *eval);
		int setInitEval (AEval *eval);
		int setZeroEval (BEval *eval);
		int setNegEval (BEval *eval);
		int setOutEval (AEval *eval);		
		
		int run(TimeSlice timeSlice);

	private:
		
		int clearStall ();
		int produceOutput ();
		int handlePlus (Element elem);
		int handleMinus (Element elem);
	};
}

#endif
