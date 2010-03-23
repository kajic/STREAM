#ifndef _DISTINCT_
#define _DISTINCT_

/**
 * @file       distinct.h
 * @date       Aug. 26, 2004
 * @brief      Distinct operator
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

#ifndef _AEVAL_
#include "execution/internals/aeval.h"
#endif

#ifndef _BEVAL_
#include "execution/internals/beval.h"
#endif

#ifndef _STORE_ALLOC_
#include "execution/stores/store_alloc.h"
#endif

#ifndef _CPP_OSTREAM_
#include <ostream>
#endif

namespace Execution {

	/**
	 * This is essentially a group by aggregation operator, where the
	 * grouping attributes are all the attributes, and the aggregationf
	 * function is a count.  (see the implementation of group by
	 * aggregation operator).
	 */ 
	class Distinct : public Operator {
	private:
		
		/// System-wide unique id
		unsigned int id;

		/// System log
		std::ostream &LOG;
		
		/// Input queue
		Queue *inputQueue;
		
		/// Output queue
		Queue *outputQueue;

        /// Synopsis for the input relation
		RelationSynopsis *outputSynopsis;
		
		/// Scan to retrieve the count information for a given tuple
		unsigned int outScanId;
		
		/// Storage allocator for output tuples
		StorageAlloc *outStore;
		
		/// Storage allocator which alloced input tuples
		StorageAlloc *inStore;
		
		/// Evaluation context in which all the action takes place
		EvalContext *evalContext;
		
		/// Updates the count information for a distinct tuple when a see
		/// PLUS element of the tuple
		AEval *plusEval;
		
		/// Initialize the count information when we see a tuple that is
		/// not in our synopsis now.
		AEval *initEval;
		
		/// Update the count information for a distinct tuple when we see
		/// a MINUS element of the tuple.
		AEval *minusEval;
		
		/// Check if we no longer have a tuple in our synopsis (used when
		/// we get a minus element)
		BEval *emptyEval;
		
		/// Timestamp of the last input element
		Timestamp lastInputTs;
		
		/// Timestamp of the last output element
		Timestamp lastOutputTs;
		
		static const unsigned int INPUT_ROLE = 2;
		static const unsigned int SYN_ROLE = 3;
		
	public:
		
		Distinct (unsigned int id, std::ostream &LOG);
		virtual ~Distinct ();
		
		//----------------------------------------------------------------------
		// Various initialization routines
		//----------------------------------------------------------------------
		int setInputQueue (Queue *inputQueue);
		int setOutputQueue (Queue *outputQueue);
		int setOutputSynopsis (RelationSynopsis *synopsis, unsigned int scanId);
		int setOutStore (StorageAlloc *store);
		int setInStore (StorageAlloc *store);
		int setEvalContext (EvalContext *evalContext);
		int setPlusEvaluator (AEval *plusEval);
		int setMinusEvaluator (AEval *minusEval);
		int setInitEvaluator (AEval *initEval);
		int setEmptyEvaluator (BEval *emptyGroupEval);
		
		int run (TimeSlice timeSlice);
		
	private:
		int processPlus (Element inputElement);
		int processMinus (Element inputElement);
	};
}

#endif
