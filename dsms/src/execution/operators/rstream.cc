/**
 * @file     rstream.cc
 * @date     Sept. 07, 2004
 * @brief    Implementation of Rstream operator
 */

#ifndef _RSTREAM_
#include "execution/operators/rstream.h"
#endif

#ifndef _DEBUG_
#include "common/debug.h"
#endif

using namespace Execution;
using namespace std;

#define UNLOCK_INPUT_TUPLE(t) (inStore -> decrRef ((t)))

Rstream::Rstream (unsigned int id, ostream &_LOG)
	: LOG (_LOG)
{
	this -> id = id;
	this -> inputQueue = 0;
	this -> outputQueue = 0;
	this -> synopsis = 0;
	this -> outStore = 0;
	this -> inStore = 0;
	this -> evalContext = 0;
	this -> copyEval = 0;
	this -> lastInputTs = 0;
	this -> lastOutputTs = 0;
	this -> bStalled = false;
	this -> scanWhenStalled = 0;
}
	
Rstream::~Rstream () {}

//----------------------------------------------------------------------
// Initialization routines
//----------------------------------------------------------------------
int Rstream::setInputQueue (Queue *inputQueue)
{
	ASSERT (inputQueue);

	this -> inputQueue = inputQueue;
	return 0;
}

int Rstream::setOutputQueue (Queue *outputQueue)
{
	ASSERT (outputQueue);

	this -> outputQueue = outputQueue;
	return 0;
}

int Rstream::setSynopsis (RelationSynopsis *syn)
{
	ASSERT (syn);

	this -> synopsis = syn;
	return 0;
}

int Rstream::setScan (unsigned int scanId)
{
	this -> scanId = scanId;
	return 0;
}

int Rstream::setOutStore (StorageAlloc *store)
{
	ASSERT (store);

	this -> outStore = store;
	return 0;
}

int Rstream::setInStore (StorageAlloc *store)
{
	ASSERT (store);

	this -> inStore = store;
	return 0;
}

int Rstream::setEvalContext (EvalContext *evalContext)
{
	ASSERT (evalContext);
	
	this -> evalContext = evalContext;
	return 0;
}

int Rstream::setCopyEval (AEval *eval)
{
	ASSERT (eval);
	
	this -> copyEval = eval;
	return 0;
}


int Rstream::run (TimeSlice timeSlice)
{
	int rc;
	unsigned int numElements;
	Element inputElement;

#ifdef _MONITOR_
	startTimer ();
#endif							

	
	// Clear stall, if we are stalled
	if (bStalled && ((rc = clearStall()) != 0))
		return rc;
	
	// We did not succeed in clearing the stall: return
	if (bStalled) {
#ifdef _MONITOR_
		stopTimer ();
		logOutTs (lastOutputTs);
#endif									
		return 0;
	}

	numElements = timeSlice;
	for (unsigned int e = 0 ; e < numElements && !bStalled ; e++) {
		
		// Get the next input element.
		if (!inputQueue -> dequeue (inputElement))
			break;
		
		// stream the relation in synopsis for timestamps lastInputTs,
		// lastInputTs + 1, ..., (inputElement.timestamp - 1)
		for (; lastInputTs < inputElement.timestamp ; lastInputTs ++) {
			
			if ((rc = produceOutput()) != 0)
				return rc;
			
			if (bStalled) {
				stallElement = inputElement;
#ifdef _MONITOR_
				stopTimer ();
				logOutTs (lastOutputTs);
#endif							
				
				return 0;
			}
		}
		
		if (inputElement.kind == E_PLUS) {
			if ((rc = synopsis -> insertTuple (inputElement.tuple)) != 0)
				return rc;			
		}
			
		else if (inputElement.kind == E_MINUS) {
			if ((rc = synopsis -> deleteTuple (inputElement.tuple)) != 0)
				return rc;
			
			// Yes, twice
			UNLOCK_INPUT_TUPLE (inputElement.tuple);
			UNLOCK_INPUT_TUPLE (inputElement.tuple);
		}
		
		// ignore heartbeats
	}
	
	if (!outputQueue -> isFull() && (lastInputTs > lastOutputTs)) {
		outputQueue -> enqueue (Element::Heartbeat (lastInputTs));
		lastOutputTs = lastInputTs;
	}

#ifdef _MONITOR_
	stopTimer ();
	logOutTs (lastOutputTs);
#endif							
	
	return 0;
}

int Rstream::produceOutput ()
{
	int rc;
	TupleIterator *scan;
	Tuple scanTuple, outTuple;	
	Element outElement;
	
	// Get a scan that returns the entire 
	if ((rc = synopsis -> getScan (scanId, scan)) != 0)
		return rc;

	while (!outputQueue -> isFull() && scan -> getNext (scanTuple)) {
		
		// Allocate space for the output tuple & copy the scanTuple to the
		// output tuple
		if ((rc = outStore -> newTuple (outTuple)) != 0)
			return rc;
		
		evalContext -> bind (scanTuple, INPUT_ROLE);
		evalContext -> bind (outTuple, OUTPUT_ROLE);
		copyEval -> eval();

		// Generate the output element
		outElement.timestamp = lastInputTs; // see run() method
		outElement.tuple = outTuple;
		outElement.kind = E_PLUS;

		outputQueue -> enqueue (outElement);
		lastOutputTs = lastInputTs;
	}
	
	// Output queue is full.  We take this as a stall
	if (outputQueue -> isFull()) {		
		// We shouldn't be here if we were already stalled
		ASSERT (!bStalled);		
		bStalled = true;
		scanWhenStalled = scan;
	}
	
	else {
		if ((rc = synopsis -> releaseScan (scanId, scan)) != 0)
			return rc;
	}
	
	return 0;
}

int Rstream::clearStall()
{
	int rc;
	TupleIterator *scan;
	Tuple scanTuple, outTuple;
	Element outElement;
	
	ASSERT (bStalled && scanWhenStalled);
	
	scan = scanWhenStalled;
	while (!outputQueue -> isFull() && scan -> getNext (scanTuple)) {
		
		// Allocate space for the output tuple & copy the scanTuple to the
		// output tuple
		if ((rc = outStore -> newTuple (outTuple)) != 0)
			return rc;
		
		evalContext -> bind (scanTuple, INPUT_ROLE);
		evalContext -> bind (outTuple, OUTPUT_ROLE);
		copyEval -> eval();
		
		// Generate the output element
		outElement.timestamp = lastInputTs; // see run() method
		outElement.tuple = outTuple;
		outElement.kind = E_PLUS;

		outputQueue -> enqueue (outElement);
		lastOutputTs = lastInputTs;
	}

	// Couldn't clear the stall
	if (outputQueue -> isFull()) {
		return 0;
	}

	if ((rc = synopsis -> releaseScan (scanId, scan)) != 0)
		return rc;
	
	bStalled = false;
	
	lastInputTs++;
	ASSERT (lastInputTs <= stallElement.timestamp);
	for (; lastInputTs < stallElement.timestamp; lastInputTs++) {

		if ((rc = produceOutput ()) != 0)
			return rc;

		if (bStalled)
			return 0;		
	}
	
	if (stallElement.kind == E_PLUS) {
		if ((rc = synopsis -> insertTuple (stallElement.tuple)) != 0)
			return rc;			
	}
			
	else if (stallElement.kind == E_MINUS) {
		if ((rc = synopsis -> deleteTuple (stallElement.tuple)) != 0)
			return rc;
			
		// Yes, twice
		UNLOCK_INPUT_TUPLE (stallElement.tuple);
		UNLOCK_INPUT_TUPLE (stallElement.tuple);
	}
	
	return 0;
}	
