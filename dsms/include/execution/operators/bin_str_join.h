#ifndef _BIN_STR_JOIN_
#define _BIN_STR_JOIN_

/**
 * @file           bin_str_join.h
 * @date           May 30, 2004
 * @brief          Binary Stream Join operator
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
	 * Binary stream join is an asymmetric operator that joins streams
	 * with relations and produces a stream.
	 *
	 * The "inner" input is a relation and the outer input a stream.  In
	 * CQL terms it computes "Rstream(Outer[Now] Join Inner)".
	 *
	 * 1. Synopses
	 * 2. Scans using synopses
	 * 3. Storage allocator
	 * 4. Evaluation context
	 * 5. Arithmetic evaluator
	 * 6. Stall logic
	 */

	class BinStreamJoin : public Operator {
	private:
		/// System wide id
		unsigned int id;

		/// System log
		std::ostream &LOG;
		
		/// Outer, stream input
		Queue *outerInputQueue;
		
		/// Inner, relation/stream input
		Queue *innerInputQueue;
		
		/// Output queue
		Queue *outputQueue;
				
		/// Synopsis storing the current state of the inner relation
		RelationSynopsis *innerSynopsis;
		
		/// Identifier for the scan that the operator performs to identify
		/// all joining  inner tuples.  This scan  has been pre-registered
		/// with  the synopsis at  the time  of its  construction -  so it
		/// knows from the scanId what the scanCondition is.		
		unsigned int scanId;
		
		/// Storage allocator for the output tuples
		StorageAlloc *outStore;

		/// Storage allocator that allocated inner input tuples
		StorageAlloc *innerInStore;

		/// Storage allocator that allocated outer input tuples
		StorageAlloc *outerInStore;
		
		/// Evaluation context in which the entire join condition
		/// evaluation and output tuple construction takes place
		EvalContext *evalContext;
		
		/// Arithmetic Evaluator that constructs the output tuple from the
		/// input tuples
		AEval *outputConstructor;
		
		/// Timestamp of the last element dequeued from outer queue
		Timestamp lastOuterTs;
		
		/// Timestamp of last element dequeued from the inner queue
		Timestamp lastInnerTs;
		
		/// Timestamp of the last element enqueued in the output
		Timestamp lastOutputTs;
		
		bool bStalled;
		TupleIterator *innerScanWhenStalled;
		Tuple stallTuple;

		static const unsigned int OUTER_ROLE = 2;
		static const unsigned int INNER_ROLE = 3;
		static const unsigned int OUTPUT_ROLE = 4;		
		
	public:
		BinStreamJoin(unsigned int id, std::ostream &LOG);
		virtual ~BinStreamJoin();
		
		int setOuterInputQueue (Queue *outerQueue);
		int setInnerInputQueue (Queue *innerQueue);
		int setOutputQueue (Queue *outputQueue);
		int setSynopsis (RelationSynopsis *innerSynopsis);
		int setScan (unsigned int scanId);
		int setOutputStore (StorageAlloc *storeAlloc);
		int setInnerInputStore (StorageAlloc *store);
		int setOuterInputStore (StorageAlloc *store);
		int setEvalContext (EvalContext *evalContext);
		int setOutputConstructor (AEval *outputConstructor);
		
		int run(TimeSlice timeSlice);
		
	private:
	    int clearStall();
		inline int processOuter (Element e);
		inline int processInner (Element e);
	};
}

#endif
