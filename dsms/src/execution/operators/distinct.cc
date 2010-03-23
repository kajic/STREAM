/**
 * @file   distinct.cc
 * @date   Aug. 26, 2004
 * @brief  Implements the distinct operator
 */

#ifndef _DEBUG_
#include "common/debug.h"
#endif

#ifndef _DISTINCT_
#include "execution/operators/distinct.h"
#endif

#define LOCK_OUTPUT_TUPLE(t) (outStore -> addRef ((t)))
#define UNLOCK_INPUT_TUPLE(t) (inStore -> decrRef ((t)))

using namespace Execution;

Distinct::Distinct (unsigned int id, std::ostream &_LOG)
	: LOG (_LOG)
{
	this -> id                  = id;
	this -> inputQueue          = 0;
	this -> outputQueue         = 0;
	this -> outputSynopsis      = 0;
	this -> outStore            = 0;
	this -> inStore             = 0;
	this -> evalContext         = 0;
	this -> plusEval            = 0;
	this -> initEval            = 0;	
	this -> minusEval           = 0;
	this -> emptyEval           = 0;	
	this -> lastInputTs         = 0;
	this -> lastOutputTs        = 0;
}

Distinct::~Distinct ()
{
	if (evalContext)
		delete evalContext;
	if (plusEval)
		delete plusEval;
	if (initEval)
		delete initEval;
	if (minusEval)
		delete minusEval;
	if (emptyEval)
		delete emptyEval;
}

int Distinct::setInputQueue (Queue *inputQueue)
{
	ASSERT (inputQueue);
	
	this -> inputQueue = inputQueue;
	return 0;
}

int Distinct::setOutputQueue (Queue *outputQueue)
{
	ASSERT (outputQueue);
	
	this -> outputQueue = outputQueue;
	return 0;
}

int Distinct::setOutputSynopsis (RelationSynopsis *synopsis,
								 unsigned int scanId)
{
	ASSERT (synopsis);
	
	this -> outputSynopsis = synopsis;
	this -> outScanId = scanId;
	return 0;
}

int Distinct::setOutStore (StorageAlloc *store)
{
	ASSERT (store);
	
	this -> outStore = store;
	return 0;
}

int Distinct::setInStore (StorageAlloc *store)
{
	ASSERT (store);
	
	this -> inStore = store;
	return 0;
}

int Distinct::setEvalContext (EvalContext *evalContext)
{
	ASSERT (evalContext);
	
	this -> evalContext = evalContext;
	return 0;
}

// could be null
int Distinct::setPlusEvaluator (AEval *plusEval)
{	
	this -> plusEval = plusEval;
	return 0;
}

// could be null
int Distinct::setInitEvaluator (AEval *initEval)
{
	this -> initEval = initEval;
	return 0;
}

// could be null
int Distinct::setMinusEvaluator (AEval *minusEval)
{
	this -> minusEval = minusEval;
	return 0;
}

// could be null
int Distinct::setEmptyEvaluator (BEval *emptyEval)
{
	this -> emptyEval = emptyEval;
	return 0;
}

int Distinct::run (TimeSlice timeSlice)
{
	int rc;
	unsigned int numElements;
	Element      inputElement;

#ifdef _MONITOR_
	startTimer ();
#endif							
	
	numElements = timeSlice;
	for (unsigned int e = 0 ; e < numElements ; e++) {

		// No space in output queue -- we quit
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

int Distinct::processPlus (Element inputElement)
{
	int rc;

	// input tuple
	Tuple inTuple; 
	
	// The synopsis tuple for inputElement.tuple
	Tuple synTuple;
	
	// Scan iterator for retrieving synTuple from outputSynopsis
	TupleIterator *scan;

	// ...
	bool bSynTupleExists;

	// Output element
	Element plusElement;	
	
	inTuple = inputElement.tuple;
	
	evalContext -> bind (inTuple, INPUT_ROLE);

	// Get a scan that produces the syn tuple corresponding to the input
	// tuple
	if ((rc = outputSynopsis -> getScan (outScanId, scan)) != 0)
		return rc;
	
	bSynTupleExists = scan -> getNext (synTuple);

#ifdef _DM_
	Tuple dummy;
	ASSERT (!scan -> getNext (dummy));
#endif

	if ((rc = outputSynopsis -> releaseScan (outScanId, scan)) != 0)
		return rc;
	
	// We have seen an identical tuple before, so we should not output
	// this tuple.  But we update a counter so that we know how many
	// identical tuples we have seen.  This update is done in-place.  This
	// update needs to be done only if the input can produce minus tuples
	
	if (bSynTupleExists) {
		if (plusEval) {
			evalContext -> bind (synTuple, SYN_ROLE);
			plusEval -> eval();
		}
	}
	
	else {
		
		rc = outStore -> newTuple (synTuple);
		if (rc != 0) return rc;
		
		// syn tuple with count = 1
		evalContext -> bind (synTuple, SYN_ROLE);
		initEval -> eval ();
		
		// Insert the tuple to the output synopsis.
		rc = outputSynopsis -> insertTuple (synTuple);
		if (rc != 0) return rc;
		LOCK_OUTPUT_TUPLE (synTuple);
		
		// Send a PLUS element for this tuple.  btw, we know we are not
		// blocked because we checked.
		ASSERT (!outputQueue -> isFull());
		
		plusElement.kind = E_PLUS;
		plusElement.tuple = synTuple;
		plusElement.timestamp = inputElement.timestamp;
		
		outputQueue -> enqueue (plusElement);
	}
	
	return 0;
}

int Distinct::processMinus (Element inputElement)
{
	int rc;
	
	// Input tuple
	Tuple inTuple;
	
	// The syn tuple for all tuples identical to inputTuple
	Tuple synTuple;
	
	// Scan to get the count tuple
	TupleIterator *scan;
	
	// ...
	bool bSynTupleExists;

	Element minusElement;
	
	inTuple = inputElement.tuple;
	evalContext -> bind (inTuple, INPUT_ROLE);
	
	rc = outputSynopsis -> getScan (outScanId, scan);
	if (rc != 0) return rc;

	// Since we are seeing a MINUS tuple, we should have record of this
	// tuple.
	bSynTupleExists = scan -> getNext (synTuple);
#ifdef _DM_
	ASSERT (bSynTupleExists);
	Tuple dummy;
	ASSERT (!scan -> getNext (dummy));
#endif
	if ((rc = outputSynopsis -> releaseScan (outScanId, scan)) != 0)
		return rc;
	
	evalContext -> bind (synTuple, SYN_ROLE);
		
	// If count == 1, output a MINUS tuple
	if (emptyEval -> eval ()) {		
		minusElement.kind = E_MINUS;
		minusElement.tuple = synTuple;
		minusElement.timestamp = inputElement.timestamp;
		
		// We have ensured before coming here that the output queue is
		// nonfull 	
		ASSERT (!outputQueue -> isFull());
		outputQueue -> enqueue (minusElement);
		lastOutputTs = minusElement.timestamp;

		// delete this element from our synopsis
		rc = outputSynopsis -> deleteTuple (synTuple);
		if (rc != 0) return rc;
	}

	else {		
		// Update the count
		minusEval -> eval ();		
	}	
	
	return 0;
}
