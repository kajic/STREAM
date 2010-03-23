/**
 * @file         group_aggr.cc
 * @date         May 30, 2004
 * @brief        Group by aggregation operator
 */

#ifndef _DEBUG_
#include "common/debug.h"
#endif

#ifndef _GROUP_AGGR_
#include "execution/operators/group_aggr.h"
#endif

#define LOCK_OUTPUT_TUPLE(t)   (outStore -> addRef ((t)))
#define LOCK_INPUT_TUPLE(t)    (inStore -> addRef ((t)))
#define UNLOCK_OUTPUT_TUPLE(t) (outStore -> decrRef ((t)))
#define UNLOCK_INPUT_TUPLE(t)  (inStore -> decrRef ((t)))


using namespace Execution;

GroupAggr::GroupAggr (unsigned int id, std::ostream &_LOG)
	: LOG (_LOG)
{
	this -> id                  = id;
	this -> inputQueue          = 0;
	this -> outputQueue         = 0;
	this -> inputSynopsis       = 0;
	this -> outputSynopsis      = 0;
	this -> outStore            = 0;
	this -> inStore             = 0;
	this -> evalContext         = 0;
	this -> plusEval            = 0;
	this -> updateEval          = 0;
	this -> minusEval           = 0;
	this -> bScanNotReq         = 0;
	this -> initEval            = 0;
	this -> emptyGroupEval      = 0;
	this -> lastInputTs         = 0;
	this -> lastOutputTs        = 0;
	this -> bStalled            = false;
}

GroupAggr::~GroupAggr ()
{
	if (evalContext)
		delete evalContext;
	if (plusEval)
		delete plusEval;
	if (updateEval)
		delete updateEval;
	if (bScanNotReq)
		delete bScanNotReq;
	if (minusEval)
		delete minusEval;
	if (initEval)
		delete initEval;
	if (emptyGroupEval)
		delete emptyGroupEval;	
}

int GroupAggr::setInputQueue (Queue *inputQueue)
{
	ASSERT (inputQueue);
	
	this -> inputQueue = inputQueue;
	return 0;
}

int GroupAggr::setOutputQueue (Queue *outputQueue)
{
	ASSERT (outputQueue);
	
	this -> outputQueue = outputQueue;
	return 0;
}

int GroupAggr::setOutputSynopsis (RelationSynopsis *synopsis,
								  unsigned int scanId)
{
	ASSERT (synopsis);
	
	this -> outputSynopsis = synopsis;
	this -> outScanId = scanId;
	return 0;
}

int GroupAggr::setInputSynopsis (RelationSynopsis *synopsis,
								 unsigned int scanId)
{
	this -> inputSynopsis = synopsis;
	this -> inScanId = scanId;
	return 0;
}

int GroupAggr::setOutStore (StorageAlloc *store)
{
	ASSERT (store);
	
	this -> outStore = store;
	return 0;
}

int GroupAggr::setInStore (StorageAlloc *store)
{
	ASSERT (store);
	
	this -> inStore = store;
	return 0;
}

int GroupAggr::setPlusEvaluator (AEval *plusEval)
{
	ASSERT (plusEval);
	
	this -> plusEval = plusEval;
	return 0;
}

int GroupAggr::setMinusEvaluator (AEval *minusEval)
{
	this -> minusEval = minusEval;
	return 0;
}

int GroupAggr::setEmptyGroupEvaluator (BEval *emptyGroupEval)
{
	this -> emptyGroupEval = emptyGroupEval;
	return 0;
}

int GroupAggr::setInitEvaluator (AEval *initEval)
{
	ASSERT (initEval);
	
	this -> initEval = initEval;
	return 0;
}

int GroupAggr::setUpdateEvaluator (AEval *updateEval)
{
	this -> updateEval = updateEval;
	return 0;
}

int GroupAggr::setRescanEvaluator (BEval *eval)
{
	this -> bScanNotReq = eval;
	return 0;
}

int GroupAggr::setEvalContext (EvalContext *evalContext)
{
	ASSERT (evalContext);

	this -> evalContext = evalContext;
	return 0;
}

int GroupAggr::run (TimeSlice timeSlice)
{
	int rc;
	unsigned int numElements;
	Element      inputElement;

#ifdef _MONITOR_
	startTimer ();
#endif							

	
	// We have a stall and cannot clear it. Note the use of short-circuit
	// evaluation ..
	if (bStalled) {
		if (!outputQueue -> enqueue (stalledElement)) {
#ifdef _MONITOR_
			stopTimer ();
			logOutTs (lastOutputTs);
#endif										
			return 0;
		}
		
		bStalled = false;
	}
	
	numElements = timeSlice;
	for (unsigned int e = 0 ; (e < numElements) && !bStalled ; e++) {
		
		// No space in output queue -- no scope for any processing
		if (outputQueue -> isFull())
			break;
		
		// Get the next input element
		if (!inputQueue -> dequeue (inputElement))
			break;
		
		lastInputTs = inputElement.timestamp;
		
		// Heartbeats can be ignored
		if (inputElement.kind == E_HEARTBEAT)
			continue;
		
		if (inputElement.kind == E_PLUS) {
			rc = processPlus (inputElement);
			if (rc != 0) return rc;
		}
		
		else {
			ASSERT (inputElement.kind == E_MINUS);
			rc = processMinus (inputElement);
			if (rc != 0) return rc;
		}
		
		UNLOCK_INPUT_TUPLE (inputElement.tuple);
	}
	
	// process heartbeats
	if (!outputQueue -> isFull() && (lastOutputTs < lastInputTs)) {
		outputQueue -> enqueue (Element::Heartbeat (lastInputTs));
		lastOutputTs = lastInputTs;
	}

#ifdef _MONITOR_
	stopTimer ();
	logOutTs (lastOutputTs);
#endif							
	
	return 0;
}

int GroupAggr::processPlus (Element inputElement)
{
	int rc;
	Tuple          inpTuple;        // input tuple
	Tuple          oldAggrTuple;    // existing aggr for the tuple's group
	Tuple          newAggrTuple;    // new aggr for the tuple's group
	TupleIterator *scan;
	bool           bGroupExists;
	Element        plusElement, minusElement;
	
	ASSERT (!bStalled);
	
	inpTuple = inputElement.tuple;
	
	// Bind the input tuple
	evalContext -> bind (inpTuple, INPUT_ROLE);
	
	// Get a scan that produces the group's aggregation tuple if it
	// exists. 
	if ((rc = outputSynopsis -> getScan (outScanId, scan)) != 0)
		return rc;
	
	bGroupExists = scan -> getNext (oldAggrTuple);
	
#ifdef _DM_
	// There should be only one aggregation tuple per group
	Tuple dummy;
	ASSERT (!scan -> getNext (dummy));
#endif

	if ((rc = outputSynopsis -> releaseScan (outScanId, scan)) != 0)
		return rc;
	
	// We have seen a tuple belonging to this group before
	if (bGroupExists) {
		
		// We will create a new aggregation tuple for this group.
		// We should not overwrite the oldAggrTuple since someone
		// downstream might be reading it ...
		rc = outStore -> newTuple (newAggrTuple);
		if (rc != 0) return rc;		
		
		// Plus evaluator does the job of computing the new aggregation
		// tuple from  the new input tuple and the old aggregation tuple.
		evalContext -> bind (oldAggrTuple, OLD_OUTPUT_ROLE);
		evalContext -> bind (newAggrTuple, NEW_OUTPUT_ROLE);
		plusEval -> eval ();
		
		// We insert the new aggr. tuple into the synopsis & delete the
		// old one 
		rc = outputSynopsis -> insertTuple (newAggrTuple);
		if (rc != 0) return rc;
		LOCK_OUTPUT_TUPLE (newAggrTuple);
		
		rc = outputSynopsis -> deleteTuple (oldAggrTuple);
		if (rc != 0) return rc;		
		
		// Send a plus element corr. to newAggrTuple, and a minus element
		// corr. to oldAggrTuple.  We will never be blocked on the first
		// enqueue, since we have ensured that the queue is non-full
		// before coming here.
		ASSERT (!outputQueue -> isFull());
		
		// Plus element:
		plusElement.kind      = E_PLUS;
		plusElement.tuple     = newAggrTuple;
		plusElement.timestamp = inputElement.timestamp;
		
		outputQueue -> enqueue (plusElement);
		lastOutputTs = inputElement.timestamp;
		
		// Minus element:
		minusElement.kind      = E_MINUS;
		minusElement.tuple     = oldAggrTuple;
		minusElement.timestamp = inputElement.timestamp;
		
		// We could get stalled now though ...
		if (!outputQueue -> enqueue (minusElement)) {			
			bStalled = true;
			stalledElement = minusElement;
		}		
	}
	
	// This is the first tuple belonging to this group
	else {
		// We create a new aggregation tuple for this new group.
		rc = outStore -> newTuple (newAggrTuple);
		if (rc != 0) return rc;
		
		evalContext -> bind (newAggrTuple, NEW_OUTPUT_ROLE);
		initEval -> eval ();
		
		// Insert the new aggregation tuple into the synopsis
		rc = outputSynopsis -> insertTuple (newAggrTuple);
		if (rc != 0) return rc;
		LOCK_OUTPUT_TUPLE (newAggrTuple);
		
		// Send a plus element corresponding to this tuple. We will never
		// be blocked, since we have ensured that the queue is non-full
		// before coming here.
		ASSERT (!outputQueue -> isFull());
		
		plusElement.kind      = E_PLUS;
		plusElement.tuple     = newAggrTuple;
		plusElement.timestamp = inputElement.timestamp;
		
		outputQueue -> enqueue (plusElement);
		lastOutputTs = inputElement.timestamp;
	}
	
	// Maintain the input synopsis if it exists
	if (inputSynopsis) {
		rc = inputSynopsis -> insertTuple (inpTuple);
		if (rc != 0) return rc;
		
		LOCK_INPUT_TUPLE (inpTuple);
	}
	
	return 0;
}

int GroupAggr::processMinus (Element inputElement)
{
	int rc;
	Tuple          inpTuple;        // input tuple
	Tuple          oldAggrTuple;    // existing aggr for the tuple's group
	Tuple          newAggrTuple;    // new aggr for the tuple's group
	TupleIterator *scan;
	bool           bGroupExists;
	Element        plusElement, minusElement;
	
	ASSERT (!bStalled);
	
	inpTuple = inputElement.tuple;

	evalContext -> bind (inpTuple, INPUT_ROLE);

	// Maintain the input synopsis if it exists.  This should be done
	// before the rest of the processing since later code assumes that the
	// input synopsis is up-to-date for its correctness.
	if (inputSynopsis) {
		rc = inputSynopsis -> deleteTuple (inpTuple);
		if (rc != 0) return rc;

		UNLOCK_INPUT_TUPLE (inpTuple);
	}
	
	// Perform a scan to locate the current aggregation tuple for
	// the group to which the inpTuple belongs to, if it exists
	rc = outputSynopsis -> getScan (outScanId, scan);
	if (rc != 0) return rc;
	
	// Since we have got a MINUS tuple now, the PLUS tuple for it should
	// have arrived earlier, implying that there should be an output entry
	// for this group.
	bGroupExists = scan -> getNext (oldAggrTuple);	
	ASSERT (bGroupExists);
	
	// There should be only one aggregation tuple per group
#ifdef _DM_
	Tuple dummy;
	ASSERT (!scan -> getNext (dummy));
#endif
	rc = outputSynopsis -> releaseScan (outScanId, scan);
	if (rc != 0) return rc;
	
	evalContext -> bind (oldAggrTuple, OLD_OUTPUT_ROLE);
	
	// There are two possibilities: the group becomes empty after this
	// minus tupls is processed, or otherwise.  emptyGroupEval (a slight
	// misnomer) checks if there is only one element in this group.		
	if (emptyGroupEval -> eval()) {
		
		// delete the old aggregation tuple from our synopsis
		rc = outputSynopsis -> deleteTuple (oldAggrTuple);
		if (rc != 0) return rc;
		
		// Minus element:
		minusElement.kind      = E_MINUS;
		minusElement.tuple     = oldAggrTuple;
		minusElement.timestamp = inputElement.timestamp;
		
		// We have ensured before coming here that the output queue is
		// nonfull 
		ASSERT (!outputQueue -> isFull());
		outputQueue -> enqueue (minusElement);
		lastOutputTs = minusElement.timestamp;
	}
	
	// The group remains active.  
	else {					
		
		// We will create a new aggregation tuple for this group.
		// We should not overwrite the oldAggrTuple since someone
		// downstream might be reading it ...
		rc = outStore -> newTuple (newAggrTuple);
		if (rc != 0) return rc;
		
		// Produce the new aggr. tuple for the group.
		rc = produceOutputTupleForMinus (newAggrTuple);
		if (rc != 0) return rc;
		
		// Insert the new aggr. tuple into the output synopsis and delete
		// the old one
		rc = outputSynopsis -> insertTuple (newAggrTuple);
		if (rc != 0) return rc;
		LOCK_OUTPUT_TUPLE (newAggrTuple);
		
		rc = outputSynopsis -> deleteTuple (oldAggrTuple);
		if (rc != 0) return rc;				
		
		// Send a plus element corr. to newAggrTuple, and a minus element
		// corr. to oldAggrTuple.  We will never be blocked on the first
		// enqueue, since we have ensured that the queue is non-full
		// before coming here.
		ASSERT (!outputQueue -> isFull());
		
		// Plus element:
		plusElement.kind      = E_PLUS;
		plusElement.tuple     = newAggrTuple;
		plusElement.timestamp = inputElement.timestamp;
		
		outputQueue -> enqueue (plusElement);
		lastOutputTs = inputElement.timestamp;
		
		// Minus element:
		minusElement.kind      = E_MINUS;
		minusElement.tuple     = oldAggrTuple;
		minusElement.timestamp = inputElement.timestamp;
		
		// We could get stalled now though ...
		if (!outputQueue -> enqueue (minusElement)) {			
			bStalled = true;
			stalledElement = minusElement;
		}
	}
	
	return 0;
}

int GroupAggr::produceOutputTupleForMinus (Tuple newAggrTuple)
{
	int rc;
	TupleIterator *inScan;
	Tuple inTuple;
	bool bNotEmpty;
	
	evalContext -> bind (newAggrTuple, NEW_OUTPUT_ROLE);
	
	// Assert: At this point, evalContext contains the new input tuple
	// (MINUS) and the old aggr. tuple for the input tuples group bound.
	
	// We first determine if we need to scan the entire inner relation to
	// update the new aggregation tuple (this can happen if we have MAX or
	// MIN aggregation functions) or if we can incrementatlly produce the
	// new Aggr. tuple for the group from the old aggr. tuple and the new
	// input tuple.
	
	// Scan required: We will essentially redo the steps that we used for
	// computing the aggr. tuple for the group in response to PLUS tuples.
	if (bScanNotReq -> eval ()) {		
		minusEval -> eval ();
	}

	else {
		// scan iterator that returns all tuples in input synopsis
		// corresponding to the present group.
		if ((rc = inputSynopsis -> getScan (inScanId, inScan)) != 0)
			return rc;

		// If we come here the synopsis can't be empty
		bNotEmpty = inScan -> getNext (inTuple);
		ASSERT (bNotEmpty);
		
		// initialize newAggrTuple
		evalContext -> bind (inTuple, INPUT_ROLE);
		initEval -> eval ();
		
		// Inplace update of newAggrTuple
		while (inScan -> getNext (inTuple)) {
			evalContext -> bind (inTuple, INPUT_ROLE);
			updateEval -> eval ();
		}
		
		if ((rc = inputSynopsis -> releaseScan (inScanId, inScan)) != 0)
			return rc;
	}
	
	return 0;
}
	
