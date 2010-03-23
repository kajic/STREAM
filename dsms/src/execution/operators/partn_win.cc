/**
 * @file        partn_win.cc
 * @date        May 30, 2004
 * @brief       Partition window operator
 */

#ifndef _PARTN_WIN_
#include "execution/operators/partn_win.h"
#endif

#ifndef _DEBUG_
#include "common/debug.h"
#endif

#define LOCK_OUTPUT_TUPLE(t) (outStore -> addRef ((t)))
#define UNLOCK_INPUT_TUPLE(t) (inStore -> decrRef ((t)))

using namespace Execution;

PartnWindow::PartnWindow (unsigned int id, std::ostream &_LOG)
	: LOG (_LOG)
{
	this -> id                  = id;
	this -> inputQueue          = 0;
	this -> outputQueue         = 0;
	this -> numRows             = 0;
	this -> partnWindow         = 0;
	this -> outStore            = 0;
	this -> inStore             = 0;
	this -> evalContext         = 0;
	this -> copyEval            = 0;
	this -> lastInputTs         = 0;
	this -> lastOutputTs        = 0;
	this -> bStalled            = false;
}

PartnWindow::~PartnWindow ()
{
	if (evalContext)
		delete evalContext;
	if (copyEval)
		delete copyEval;
}

int PartnWindow::setInputQueue (Queue *inputQueue)
{
	ASSERT (inputQueue);
	
	this -> inputQueue = inputQueue;
	return 0;
}

int PartnWindow::setOutputQueue (Queue *outputQueue)
{
	ASSERT (outputQueue);
	
	this -> outputQueue = outputQueue;
	return 0;
}

int PartnWindow::setWindowSize (unsigned int numRows)
{
	ASSERT (numRows > 0);
	
	this -> numRows = numRows;
	return 0;
}

int PartnWindow::setWindowSynopsis (PartnWindowSynopsis *partnWindow)
{
	ASSERT (partnWindow);
	
	this -> partnWindow = partnWindow;
	return 0;
}

int PartnWindow::setOutStore (StorageAlloc *store)
{
	ASSERT (store);

	this -> outStore = store;
	return 0;
}

int PartnWindow::setInStore (StorageAlloc *store)
{
	ASSERT (store);

	this -> inStore = store;
	return 0;
}

int PartnWindow::setEvalContext (EvalContext *evalContext)
{
	ASSERT (evalContext);

	this -> evalContext = evalContext;
	return 0;
}

int PartnWindow::setCopyEval (AEval *copyEval)
{
	ASSERT (copyEval);

	this -> copyEval = copyEval;
	return 0;
}

int PartnWindow::run (TimeSlice timeSlice)
{
	int rc;
	unsigned int numElements;
	Element      inputElement;
	Tuple        inTuple, inTupleCopy;
	Element      outPlusElement;
	unsigned int partnSize;

#ifdef _MONITOR_
	startTimer ();
#endif							
	
	// We have a stall & are not able to clear it: note the short circuit code
	if (bStalled && !outputQueue -> enqueue (stalledElement)) {
#ifdef _MONITOR_
		stopTimer ();
#endif									
		return 0;
	}
	
	bStalled = false;
	
	numElements = timeSlice;
	for (unsigned int e = 0 ; e < numElements ; e++) {
		
		// We cannot enqueue anything in the output queue - quit processing
		if (outputQueue -> isFull())
			break;
		
		// Get the next element
		if (!inputQueue -> dequeue (inputElement)) 
			break;
		
		lastInputTs = inputElement.timestamp;
		
		// Ignore heartbeats
		if (inputElement.kind == E_HEARTBEAT)
			continue;
		
		// We cannot have minus tuple in the input of a window
		ASSERT(inputElement.kind == E_PLUS);

		inTuple = inputElement.tuple;
		
		// We allocate storage to make a copy of this tuple
		if ((rc = outStore -> newTuple (inTupleCopy)) != 0)
			return rc;
		
		// Make a copy of the input element
		evalContext -> bind (inTuple, INPUT_ROLE);
		evalContext -> bind (inTupleCopy, COPY_ROLE);
		copyEval -> eval();
		
		// Insert the copy into partition window
		if ((rc = partnWindow -> insertTuple (inTupleCopy)) != 0)
			return rc;
		LOCK_OUTPUT_TUPLE (inTupleCopy);
		
		// Forward the copy onto the output
		outPlusElement.kind = E_PLUS;
		outPlusElement.tuple = inTupleCopy;
		outPlusElement.timestamp = inputElement.timestamp;
		
		outputQueue -> enqueue (outPlusElement);
		lastOutputTs = outPlusElement.timestamp;
		
		// After the first numRows elements in a partition, every new PLUS
		// tuple in  the partition should output a  MINUS tuple (Partition
		// window semantics).  
		rc = partnWindow -> getPartnSize (inTupleCopy, partnSize);
		if (rc !=  0) return rc;
		
		ASSERT(partnSize >= 1 && partnSize <= numRows + 1);
		
		if (partnSize == numRows + 1) {
			if ((rc = expireOldestTuple (inTupleCopy)) != 0) {
				return rc;
			}
		}
		
		UNLOCK_INPUT_TUPLE (inTuple);
	}
	
	// Heartbeat generation
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

int PartnWindow::expireOldestTuple (Tuple partnTuple)
{
	int rc;
	Tuple oldestTuple;
	Element minusElement;
	
	// get & delete the oldest tuple in the partition
	if ((rc = partnWindow -> deleteOldestTuple (partnTuple, oldestTuple))
		!= 0)
		return rc;
	
	// Create a minus element for the tuple
	minusElement.kind = E_MINUS;
	minusElement.tuple = oldestTuple;
	
	// The last element we sent out was a PLUS tuple with the same
	// timestamp.
	minusElement.timestamp = lastOutputTs;
	
	if (!outputQueue -> enqueue (minusElement)) {
		bStalled = true;
		stalledElement = minusElement;
	}
	
	return 0;
}	
