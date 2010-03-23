#ifndef _BIN_JOIN_
#define _BIN_JOIN_

/**
 * @file       bin_join.h
 * @date       Aug. 25, 2004
 * @brief      Binary join operator
 */

#ifndef _OPERATOR_
#include "execution/operators/operator.h"
#endif

#ifndef _QUEUE_
#include "execution/queues/queue.h"
#endif

#ifndef _AEVAL_
#include "execution/internals/aeval.h"
#endif

#ifndef _STORE_ALLOC_
#include "execution/stores/store_alloc.h"
#endif

#ifndef _LIN_SYN_
#include "execution/synopses/lin_syn.h"
#endif

#ifndef _REL_SYN_
#include "execution/synopses/rel_syn.h"
#endif

#ifndef _EVAL_CONTEXT_
#include "execution/internals/eval_context.h"
#endif

#ifndef _CPP_OSTREAM_
#include <ostream>
#endif

#ifdef _MONITOR_
#ifndef _JOIN_MONITOR_
#include "execution/monitors/join_monitor.h"
#endif
#endif

namespace Execution {
	
	/**
	 * Binary join is a symmetric operator that joins two relations and
	 * produces another relation.
	 */
	class BinaryJoin : public Operator {		
	private:
		/// System-wide unique identifier
		unsigned int id;
		
		/// System log
		std::ostream &LOG;
		
		/// Outer input relation
		Queue *outerInputQueue;
		
		/// Inner input relation
		Queue *innerInputQueue;
		
		/// Output queue
		Queue *outputQueue;
		
		/// Store for the outer tuples
		StorageAlloc *outerInputStore;
		
		/// Store for the inner tuples
		StorageAlloc *innerInputStore;
		
		/// Synopsis storing the current state of the inner relation
		RelationSynopsis *innerSynopsis;
		
		/// Scan identifier for scanning the inner tuples
		unsigned int innerScanId;
		
		/// Synopsis storing the current state of the outer relation
		RelationSynopsis *outerSynopsis;
		
		/// Scan identifier for scanning the outer tuples
		unsigned int outerScanId;		
		
		/// Synopsis storing the output of the join.  This synopsis is
		/// used to generate MINUS tuples on the output.  So it is set to
		/// null if both inputs are streams.
		LineageSynopsis *joinSynopsis;
		
		/// Tuple array used to represent lineages. 2 comes from "binary"
		Tuple lineage [2];
		
		/// Storage allocator for the output
		StorageAlloc *outStore;
		
		/// Evaluation context in which all the action takes place
		EvalContext *evalContext;
		
		/// Arithmetic evaluator to construct the output tuple
		AEval *outputConstructor;
		
		/// Timestamp of the last element dequeued from the outer
		Timestamp lastOuterTs;
		
		/// Timestamp of the last element dequeued from the inner
		Timestamp lastInnerTs;
		
		/// Timstamp of the last element enqueued in the output
		Timestamp lastOutputTs;
		
		//----------------------------------------------------------------------
		// Stall related state
		//----------------------------------------------------------------------
		
		/// Different types of stalls
		enum StallType {
			// We stalled while processing outer PLUS element
			OUTER_PLUS,
			
			// We stalled while processing inner MINUS element
			OUTER_MINUS,
			
			// We stalled while processing inner PLUS element 
			INNER_PLUS,
			
			// We stalled while processing inner MINUS element
			INNER_MINUS
		};
		
		/// Are we stalled?
		bool bStalled;
		
		/// If bStall = true, the type of stall
		StallType stallType;
		
		/// We stalled while processing stallElement
		Element stallElement;
		
		/// [[ Explanation ]]
		TupleIterator *scanWhenStalled;

		// [[ Consistency explanation ]]
		static const unsigned int OUTER_ROLE = 2;
		static const unsigned int INNER_ROLE = 3;
		static const unsigned int OUTPUT_ROLE = 4;
		
	public:
		BinaryJoin (unsigned int id, std::ostream &LOG);
		virtual ~BinaryJoin ();
		
		//----------------------------------------------------------------------
		// Initialization routines
		//----------------------------------------------------------------------
		int setOuterInputQueue (Queue *outerQueue);
		int setInnerInputQueue(Queue *innerQueue);
		int setOutputQueue (Queue *outputQueue);
		int setInnerSynopsis (RelationSynopsis *innerSynopsis);
		int setOuterSynopsis(RelationSynopsis *outerSynopsis);
		int setJoinSynopsis (LineageSynopsis *joinSynopsis);
		int setInnerScan (unsigned int innerScanId);
		int setOuterScan (unsigned int outerScanId);
		int setOuterInputStore (StorageAlloc *store);
		int setInnerInputStore (StorageAlloc *store);
		int setOutStore (StorageAlloc *storeAlloc);
		int setEvalContext (EvalContext *evalContext);
		int setOutputConstructor (AEval *outputConstructor);
		
		/// run ...
		int run (TimeSlice timeSlice);

	private:
		
		int processOuterPlus (Element outerElement);
		int processOuterMinus (Element outerElement);
		int processInnerPlus (Element innerElement);
		int processInnerMinus (Element innerElement);
		int clearStallOuterPlus ();
		int clearStallOuterMinus ();
		int clearStallInnerPlus ();
		int clearStallInnerMinus ();
		int clearStall ();
	};	
}

#endif
