#ifndef _EXCEPT_
#define _EXCEPT_

#ifndef _OPERATOR_
#include "execution/operators/operator.h"
#endif

#ifndef _QUEUE_
#include "execution/queues/queue.h"
#endif

#ifndef _CPP_OSTREAM
#include <ostream>
#endif

#ifndef _STORE_ALLOC_
#include "execution/stores/store_alloc.h"
#endif

#ifndef _REL_SYN_
#include "execution/synopses/rel_syn.h"
#endif

#ifndef _EVAL_CONTEXT_
#include "execution/internals/eval_context.h"
#endif

#ifndef _AEVAL_
#include "execution/internals/aeval.h"
#endif

namespace Execution {
	class Except : public Operator {
	private:
		/// System-wide id
		unsigned int id;

		/// System log
		std::ostream &LOG;
		
		/// Left input queue
		Queue *leftInQueue;
		
		/// Right input queue
		Queue *rightInQueue;		
		
		/// Output queue
		Queue *outputQueue;
		
		/// Storage allocator for the output
		StorageAlloc *outStore;

		/// Synopsis for storing the output
		RelationSynopsis *outSyn;

		/// Scan Id for scanning outSyn
		unsigned int outScanId;
		
		/// Storage allocator who allocs left input tuples
		StorageAlloc *leftInStore;
		
		/// Storage allocator who allocs right input tuples
		StorageAlloc *rightInStore;
		
		/// Count synopsis for keeping track of tuple counts
		RelationSynopsis *countSyn;

		/// Scan Id for scanning countSyn
		unsigned int countScanId;
		
		/// Storage allocator to alloc tuples for countSyn
		StorageAlloc *countStore;
		
		/// Evaluation context
		EvalContext *evalContext;
		
		/// Count column
		Column countCol;
		
		/// Evaluator that constructs the count tuple
		AEval *initEval;
		
		/// Evaluator that produces outputtuple
		AEval *outEval;

		/// Evaluator that copies attrs of left to scratch
		AEval *cplsEval;
		
		/// Evaluator that copies attrs of right to scratch
		AEval *cprsEval;
		
		/// Timestamp of last left element
		Timestamp lastLeftTs;
		
		/// Timestamp of the last right element
		Timestamp lastRightTs;
		
		/// Timestamp of last output element
		Timestamp lastOutputTs;
		
		static const unsigned int INPUT_ROLE  = 2;
		static const unsigned int LEFT_ROLE   = 2;
		static const unsigned int RIGHT_ROLE  = 3;
		static const unsigned int COUNT_ROLE  = 4;
		static const unsigned int OUTPUT_ROLE = 5;
		
	public:
		Except (unsigned int id, std::ostream &LOG);
		virtual ~Except();

		//----------------------------------------------------------------------
		// Functions for initializing state
		//----------------------------------------------------------------------
		int setLeftInputQueue (Queue *inputQueue);
		int setRightInputQueue (Queue *inputQueue);
		int setOutputQueue (Queue *outputQueue);
		int setOutStore (StorageAlloc *store);
		int setOutSyn (RelationSynopsis *outSyn);
		int setOutScanId (unsigned int scanId);
		int setLeftInputStore (StorageAlloc *store);
		int setRightInputStore (StorageAlloc *store);
		int setEvalContext (EvalContext *evalContext);
		int setCountSyn (RelationSynopsis *syn);
		int setCountScanId (unsigned int scanId);
		int setCountStore (StorageAlloc *store);
		int setInitEval (AEval *eval);
		int setCopyLeftToScratchEval (AEval *eval);
		int setCopyRightToScratchEval (AEval *eval);
		int setOutEval (AEval *eval);		
		int setCountCol (Column col);
		
		int run (TimeSlice timeSlice);
		
	private:
		int handleLeftPlus (Element element);
		int handleLeftMinus (Element element);
		int handleRightPlus (Element element);
		int handleRightMinus (Element element);
		int getCountTuple_l (Tuple inTuple, Tuple &countTuple);
		int getCountTuple_r (Tuple inTuple, Tuple &countTuple);
		int getOutTuple (Tuple inTuple, Tuple &outTuple);		
	};
}

#endif
