#ifndef _GROUP_AGGR_
#define _GROUP_AGGR_

/**
 * @file       group_aggr.h
 * @date       May 30, 2004
 * @brief      Group by and aggregation operator.  [[ currently does not
 *             handle MAX & MIN ]]
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
	
	class GroupAggr : public Operator {
	private:
		
		/// System-wide unique id
		unsigned int id;

		/// System log
		std::ostream &LOG;
		
		/// Input queue
		Queue *inputQueue;
		
		/// Output queue
		Queue *outputQueue;
				
		/// Synopsis for the input relation (optional)
		RelationSynopsis *inputSynopsis;
		
		/// Scan over input synopsis for getting all tuples belonging to a
		/// group
		unsigned int inScanId;
		
		/// Synopsis storing the output
		RelationSynopsis *outputSynopsis;
		
		/// Scan over outputSynopsis to get the tuple corresponding to the
		/// "current" group - the group of the latest input tuple
		unsigned int outScanId;
		
		/// Storage allocator for output tuples
		StorageAlloc *outStore;
		
		/// Storage Allocator who allocatedinput tuples
		StorageAlloc *inStore;
		
		/// Evaluation context in which all the action takes place
		EvalContext *evalContext;
		
		/// Consider a group that currently exists in the output
		/// synopsis.  When a new PLUS tuple arrives we need to update
		/// this tuple to reflect the new tuple - the plusEval evaluator
		/// does this job.
		AEval *plusEval;
		
		/// This is identical in functionality to plusEval, except that
		/// the evaluation is done in-place [[ Explanation ]]
		AEval *updateEval;
		
		/// This  evaluator is  used  while processing  MINUS tuples.   It
		/// returns true  if the  aggregate values for  a group  cannot be
		/// incrementally updated (due to a MAX / MIN aggr) for the latest
		/// MINUS tuple.  If  it returns true, we need  to scan the entire
		/// input (in inputSynopsis) to update our groups aggregates.		
		BEval *bScanNotReq;
		
		/// minusEval is the anti-particle of the plusEval - it updates a
		/// groups tuple to reflect the deletion of a tuple.  minusEval is
		/// used only if bScanReq() returns false.
		AEval *minusEval;
		
		/// initEval creates the first aggr. tuple for a group.
		AEval *initEval;
		
		/// emptyGroupEval checks if the group has become empty, i.e.,
		/// count == 0.
		BEval *emptyGroupEval;
		
		/// Timestamp of the last input element.
		Timestamp lastInputTs;
		
		/// Timestamp of the last output element.
		Timestamp lastOutputTs;
		
		bool bStalled;
		Element stalledElement;

		static const unsigned int INPUT_ROLE = 4;
		static const unsigned int OLD_OUTPUT_ROLE = 3;
		static const unsigned int NEW_OUTPUT_ROLE = 2;
		
	public:		
		GroupAggr (unsigned int id, std::ostream &LOG);
		virtual ~GroupAggr();

		//----------------------------------------------------------------------
		// Various initialization routines
		//----------------------------------------------------------------------
		int setInputQueue (Queue *inputQueue);
		int setOutputQueue (Queue *outputQueue);
		int setOutputSynopsis (RelationSynopsis *synopsis, unsigned int scanId);
		int setInputSynopsis (RelationSynopsis *synopsis, unsigned int scanId);
		int setOutStore (StorageAlloc *store);
		int setInStore (StorageAlloc *store);
		int setEvalContext (EvalContext *evalContext);
		int setPlusEvaluator (AEval *plusEval);
		int setMinusEvaluator (AEval *minusEval);
		int setInitEvaluator (AEval *initEval);
		int setEmptyGroupEvaluator (BEval *emptyGroupEval);
		int setRescanEvaluator (BEval *bScanReq);
		int setUpdateEvaluator (AEval *updateEval);
		
		int run (TimeSlice timeSlice);
		
	private:
		
		int processPlus (Element inputElement);
		int processMinus (Element inputElement);
		int produceOutputTupleForMinus (Tuple newAggrTuple);
	};
}

#endif
