/**
 * @file       istream.cc
 * @date       Sep. 7, 2004
 * @brief      Implementation of Istream operator
 */

#ifndef _ISTREAM_
#include "execution/operators/istream.h"
#endif

#ifndef _DEBUG_
#include "common/debug.h"
#endif

#define UNLOCK_INPUT_TUPLE(t) (inStore -> decrRef ((t)))
#define UNLOCK_SYN_TUPLE(t)   (synStore -> decrRef ((t)))

using namespace std;
using namespace Execution;

Istream::Istream (unsigned int id, ostream &_LOG)
	: LOG (_LOG)
{
	this -> id = id;
	this -> inputQueue = 0;
	this -> outputQueue = 0;
	this -> synopsis = 0;
	this -> synStore = 0;
	this -> outStore = 0;
	this -> inStore = 0;
	this -> evalContext = 0;
	this -> incrEval = 0;
	this -> initEval = 0;
	this -> decrEval = 0;
	this -> zeroEval = 0;
	this -> outEval = 0;
	this -> lastInputTs = 0;
	this -> lastOutputTs = 0;
	this -> curTs = 0;
	this -> scanWhenStalled = 0;
	this -> bStalled = false;
}

Istream::~Istream () {}

int Istream::setInputQueue (Queue *inputQueue)
{
	ASSERT (inputQueue);

	this -> inputQueue = inputQueue;
	return 0;
}

int Istream::setOutputQueue (Queue *outputQueue)
{
	ASSERT (outputQueue);

	this -> outputQueue = outputQueue;
	return 0;
}

int Istream::setSynopsis (RelationSynopsis *syn)
{
	ASSERT (syn);

	this -> synopsis = syn;
	return 0;
}

int Istream::setCountScan (unsigned int scanId)
{
	this -> countScanId = scanId;
	return 0;
}

int Istream::setFullScan (unsigned int scanId)
{
	this -> fullScanId = scanId;
	return 0;
}

int Istream::setSynStore (StorageAlloc *store)
{
	ASSERT (store);

	this -> synStore = store;
	return 0;
}
		
int Istream::setOutStore (StorageAlloc *store)
{
	ASSERT (store);

	this -> outStore = store;
	return 0;
}

int Istream::setInStore (StorageAlloc *store)
{
	ASSERT (store);

	this -> inStore = store;
	return 0;
}

int Istream::setEvalContext (EvalContext *evalContext)
{
	ASSERT (evalContext);

	this -> evalContext = evalContext;
	return 0;
}

int Istream::setIncrEval (AEval *eval)
{
	ASSERT (eval);

	this -> incrEval = eval;
	return 0;
}

int Istream::setDecrEval (AEval *eval)
{
	ASSERT (eval);

	this -> decrEval = eval;
	return 0;
}

int Istream::setInitEval (AEval *eval)
{
	ASSERT (eval);

	this -> initEval = eval;
	return 0;
}

int Istream::setZeroEval (BEval *eval)
{
	ASSERT (eval);

	this -> zeroEval = eval;
	return 0;
}

int Istream::setPosEval (BEval *eval)
{
	ASSERT (eval);

	this -> posEval = eval;
	return 0;
}

int Istream::setOutEval (AEval *eval)
{
	ASSERT (eval);
	
	this -> outEval = eval;
	return 0;
}

int Istream::run (TimeSlice timeSlice)
{
	int rc;
	unsigned int numElements;
	Element inputElement;

#ifdef _MONITOR_
	startTimer ();
#endif							

	
	// number of elements we will process before returning
	numElements = timeSlice;

	// We are stalled, need to clear it first
	if (bStalled && ((rc = clearStall()) != 0))
		return rc;
	
	// We did not succeed in clearing the stall, return without
	// further processing
	if (bStalled) {
#ifdef _MONITOR_
		stopTimer ();
		logOutTs (lastOutputTs);
#endif									
		return 0;
	}
	
	for (unsigned int e = 0 ; e < numElements ; e++) {
		
		// Get the next input element
		if (!inputQueue -> dequeue (inputElement))
			break;
		
		lastInputTs = inputElement.timestamp;
		
		// If the timestamp of the input element > curTs, we can produce
		// the output (data) elements with timestamp = curTs.
		if (curTs < lastInputTs) {
			if ((rc = produceOutput ()) != 0)
				return rc;
			
			// We stalled while producing output:
			if (bStalled) {
				stallElement = inputElement;
				break;
			}
		}
		
		// Process a plus tuple
		if (inputElement.kind == E_PLUS) {
			
			rc = handlePlus (inputElement);
			if (rc != 0) return rc;
			
			UNLOCK_INPUT_TUPLE (inputElement.tuple);
		}
		
		// Process a minus tuple
		else if (inputElement.kind == E_MINUS) {			
			rc = handleMinus (inputElement);
			if (rc != 0) return rc;

			UNLOCK_INPUT_TUPLE (inputElement.tuple);
		}
	}

	// Heartbeat generation: Assert to the operator above that we won't
	// produce any element with timestamp < lastInputTs
	if (!outputQueue -> isFull() && (lastInputTs > lastOutputTs)) {
		outputQueue -> enqueue (Element::Heartbeat(lastInputTs));
		lastOutputTs = lastInputTs;
	}
	
#ifdef _MONITOR_
	stopTimer ();
	logOutTs (lastOutputTs);
#endif							
	
	return 0;
}

int Istream::produceOutput ()
{
	int rc;
	TupleIterator *scan;
	Tuple synTuple;
	Tuple outTuple;
	Element outElement;

	ASSERT (!bStalled);
	
	if ((rc = synopsis -> getScan (fullScanId, scan)) != 0)
		return rc;
	
	while (scan -> getNext (synTuple)) {
		
		evalContext -> bind (synTuple, SYN_ROLE);
		
		// If the count of a synTuple is k, we produce k elements on the
		// output, each of whose tuple attribute is a copy of synTuple,
		// timestamp = curTs.
		while (!outputQueue -> isFull() && posEval -> eval()) {
			
			// Allocate space for a new output tuple
			if ((rc = outStore -> newTuple (outTuple)) != 0)
				return rc;
			
			// copy non-count fields of synTuple to outTuple
			evalContext -> bind (outTuple, OUTPUT_ROLE);			
			outEval -> eval();
			
			// generate the output element & enqueue it
			outElement.kind = E_PLUS;
			outElement.tuple = outTuple;
			outElement.timestamp = curTs;
			
			outputQueue -> enqueue (outElement);
			lastOutputTs = curTs;
			
			// decrement the count of the tuple
			decrEval -> eval();
		}
		
		// If the output queue is full, we take this as a stall.  We
		// remember the synTuple, since we might have to generate
		// one or more copies of this tuple in the output later.
		if (outputQueue -> isFull()) {
			bStalled = true;
			synTupleWhenStalled = synTuple;
			scanWhenStalled = scan;
			
			return 0;
		}
		
		// The output queue is not full ==> we have generated the correct
		// no of copies (possibly 0) of synTuple in the output.  We delete
		// the synTuple from the input synopsis.  Note that the synopsis
		// guarantees that the scan continues to be valid even after this
		// deletion.
		if ((rc = synopsis -> deleteTuple (synTuple)) != 0)
			return rc;
		UNLOCK_SYN_TUPLE (synTuple);
	}
	
	if ((rc = synopsis -> releaseScan (fullScanId, scan)) != 0)
		return rc;
	
	// Update the current timestamp.
	curTs = lastInputTs;
	
	return 0;
}

int Istream::clearStall ()
{
	int rc;
	Tuple synTuple;
	Tuple outTuple;
	Element outElement;
	TupleIterator *scan;
	
	ASSERT (bStalled && scanWhenStalled && synTupleWhenStalled);
	
	// First, we generate the required number of copies of the synTuple.
	synTuple = synTupleWhenStalled;
	evalContext -> bind (synTuple, SYN_ROLE);
	
	while (!outputQueue -> isFull() && posEval -> eval()) {
		
		// Allocate space for a new output tuple
		if ((rc = outStore -> newTuple (outTuple)) != 0)
			return rc;
		
		// copy non-count fields of synTuple to outTuple
		evalContext -> bind (outTuple, OUTPUT_ROLE);			
		outEval -> eval();
		
		// generate the output element & enqueue it
		outElement.kind = E_PLUS;
		outElement.tuple = outTuple;
		outElement.timestamp = curTs;
		
		outputQueue -> enqueue (outElement);
		lastOutputTs = curTs;
		
		// decrement the count of the tuple
		decrEval -> eval();
	}
	
	// If the output queue is full, we have failed to clear the stall.
	if (outputQueue -> isFull()) 
		return 0;
	
	// We managed to generate the required number of output tuples.
	// We delete the synTuple from our synopsis
	if ((rc = synopsis -> deleteTuple (synTuple)) != 0){
		return rc;
	}
	UNLOCK_SYN_TUPLE (synTuple);
	

	scan = scanWhenStalled;
	while (scan -> getNext (synTuple)) {
		
		evalContext -> bind (synTuple, SYN_ROLE);
		
		// If the count of a synTuple is k, we produce k elements on the
		// output, each of whose tuple attribute is a copy of synTuple,
		// timestamp = curTs.
		while (!outputQueue -> isFull() && posEval -> eval()) {
			
			// Allocate space for a new output tuple
			if ((rc = outStore -> newTuple (outTuple)) != 0)
				return rc;
			
			// copy non-count fields of synTuple to outTuple
			evalContext -> bind (outTuple, OUTPUT_ROLE);			
			outEval -> eval();
			
			// generate the output element & enqueue it
			outElement.kind = E_PLUS;
			outElement.tuple = outTuple;
			outElement.timestamp = curTs;
			
			outputQueue -> enqueue (outElement);
			lastOutputTs = curTs;
			
			// decrement the count of the tuple
			decrEval -> eval();
		}
		
		// We stalled again: remember synTuple & return
		if (outputQueue -> isFull()) {
			synTupleWhenStalled = synTuple;			
			return 0;
		}
		
		// We managed to generate the required no of output tuples.
		// Delete the synTuple from the synopsis
		if ((rc = synopsis -> deleteTuple (synTuple)) != 0)
			return rc;
		UNLOCK_SYN_TUPLE (synTuple);
	}

	if ((rc = synopsis -> releaseScan (fullScanId, scan)) != 0)
		return rc;
	bStalled = false;
	
	// Update the current timestamp.
	curTs = lastInputTs;
	
	// Process a plus tuple
	if (stallElement.kind == E_PLUS) {
		
		rc = handlePlus (stallElement);
		if (rc != 0) return rc;
		
		UNLOCK_INPUT_TUPLE (stallElement.tuple);
	}
	
	// Process a minus tuple
	else if (stallElement.kind == E_MINUS) {			
		rc = handleMinus (stallElement);
		if (rc != 0) return rc;
		
		UNLOCK_INPUT_TUPLE (stallElement.tuple);
	}	
	
	return 0;
}

int Istream::handlePlus (Element inputElement)
{
	int rc;
	Tuple inTuple;
	TupleIterator *scan;
	Tuple countTuple;
	
	inTuple = inputElement.tuple;
	evalContext -> bind (inTuple, INPUT_ROLE);
	
	// If we have seen a copy of inTuple before, we add 1 to the count
	// corresponding to inTuple.  Otherwise, we initialize the count
	// corresponding to inTuple to 1

	// Scan
	if ((rc = synopsis -> getScan (countScanId, scan)) != 0)
		return rc;

	// We have seen inTuple before
	if (scan -> getNext (countTuple)) {
		
#ifdef _DM_
		Tuple dummy;
		ASSERT (!scan -> getNext (dummy));
#endif
		
		evalContext -> bind (countTuple, SYN_ROLE);
		
#ifdef _DM_
		// count should be non-zero
		ASSERT (!zeroEval -> eval());
#endif

		// increment the count
		incrEval -> eval ();

		// If the count becomes zero, we delete the tuple
		if (zeroEval -> eval ()) { 
			if ((rc = synopsis -> deleteTuple (countTuple)) != 0)
				return rc;
			UNLOCK_SYN_TUPLE (countTuple);
		}
	}

	// We have not seen inTuple before
	else {
		
		// create a new countTuple
		if ((rc = synStore -> newTuple (countTuple)) != 0)
			return rc;

		// initialize the count to 1
		evalContext -> bind (countTuple, SYN_ROLE);
		initEval -> eval();
		incrEval -> eval();
		
		// insert the tuple to synopsis
		if ((rc = synopsis -> insertTuple (countTuple)) != 0)
			return rc;
	}

	if ((rc = synopsis -> releaseScan (countScanId, scan)) != 0)
		return rc;
	
	return 0;
}

int Istream::handleMinus (Element inputElement)
{
	int rc;
	Tuple inTuple;
	TupleIterator *scan;
	Tuple countTuple;
	
	inTuple = inputElement.tuple;
	evalContext -> bind (inTuple, INPUT_ROLE);
	
	// If we have seen a copy of inTuple before, we subtract 1 from the count
	// corresponding to inTuple.  Otherwise, we initialize the count
	// corresponding to inTuple to -1
	
	// Scan
	if ((rc = synopsis -> getScan (countScanId, scan)) != 0)
		return rc;

	// We have seen inTuple before
	if (scan -> getNext (countTuple)) {
		
#ifdef _DM_
		Tuple dummy;
		ASSERT (!scan -> getNext (dummy));
#endif
		
		evalContext -> bind (countTuple, SYN_ROLE);
		
#ifdef _DM_
		// count should be non-zero
		ASSERT (!zeroEval -> eval());
#endif

		// decrement the count
		decrEval -> eval ();

		// If the count becomes zero, we delete the tuple
		if (zeroEval -> eval ()){ 
			if ((rc = synopsis -> deleteTuple (countTuple)) != 0)
				return rc;
			UNLOCK_SYN_TUPLE (countTuple);
		}
	}

	// We have not seen inTuple before
	else {
		
		// create a new countTuple
		if ((rc = synStore -> newTuple (countTuple)) != 0)
			return rc;
		
		// initialize the count to -1
		evalContext -> bind (countTuple, SYN_ROLE);
		initEval -> eval();
		decrEval -> eval();
		
		// insert the tuple to synopsis
		if ((rc = synopsis -> insertTuple (countTuple)) != 0)
			return rc;
	}

	if ((rc = synopsis -> releaseScan (countScanId, scan)) != 0)
		return rc;
	
	return 0;
}
