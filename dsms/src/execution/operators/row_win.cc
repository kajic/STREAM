/**
 * @file      row_win.cc
 * @date      Aug. 26, 2004
 * @brief     Implements the row window
 */

#ifndef _DEBUG_
#include "common/debug.h"
#endif

#ifndef _ROW_WIN_
#include "execution/operators/row_win.h"
#endif

using namespace Execution;
using std::endl;

#define LOCK_INPUT_TUPLE(t) (inStore -> addRef ((t)))

RowWindow::RowWindow (unsigned int id, std::ostream &_LOG)
	: LOG (_LOG)
{
	this -> id             = id;
	this -> inputQueue     = 0;
	this -> outputQueue    = 0;
	this -> windowSize     = 0;
	this -> winSynopsis    = 0;
	this -> lastInputTs    = 0;
	this -> lastOutputTs   = 0;
	this -> bStalled       = 0;
	this -> inStore        = 0;
	this -> numProcessed   = 0;
}

RowWindow::~RowWindow () {}

int RowWindow::setInputQueue (Queue *inputQueue) 
{
	ASSERT (inputQueue);
	
	this -> inputQueue = inputQueue;
	return 0;
}

int RowWindow::setOutputQueue (Queue *outputQueue)
{
	ASSERT (outputQueue);
	
	this -> outputQueue = outputQueue;
	return 0;
}

int RowWindow::setWindowSize (unsigned int windowSize)
{
	ASSERT (windowSize > 0);
	
	this -> windowSize = windowSize;
	return 0;
}

int RowWindow::setWindowSynopsis (WindowSynopsis *winSynopsis) 
{
	ASSERT (winSynopsis);
	
	this -> winSynopsis = winSynopsis;
	return 0;
}

int RowWindow::setInStore (StorageAlloc *store)
{
	ASSERT (store);

	this -> inStore = store;
	return 0;
}

int RowWindow::run (TimeSlice timeSlice)
{
	int rc;
	unsigned int numElements;
	Element inElement;
	Tuple oldestTuple;
	Timestamp oldestTupleTs;
	Element outElement;

#ifdef _MONITOR_
	startTimer ();
#endif							
	
	// We have a stall & aren't able to clear it
	if (bStalled && !clearStall()) {
#ifdef _MONITOR_
		stopTimer ();
		logOutTs (lastOutputTs);
#endif									
		return 0;
	}

	numElements = timeSlice;
	for (unsigned int e = 0 ; e < numElements ; e++) {

		// We skip processing if we find the output queue full
		if (outputQueue -> isFull())
			break;

		// Input queue empty?
		if (!inputQueue -> dequeue (inElement))
			break;
		
		lastInputTs = inElement.timestamp;

		// Ignore heartbeats
		if (inElement.kind == E_HEARTBEAT)
			continue;
		
		// Our input should be a stream, so can't have MINUSes		
		ASSERT (inElement.kind == E_PLUS);
		
		
		// Insert the input tuple into the synopsis
		if ((rc = winSynopsis -> insertTuple (inElement.tuple,
											  inElement.timestamp)) != 0) {
			return rc;
		}
		LOCK_INPUT_TUPLE (inElement.tuple);
		
		// forward the same element onwards ..
		outputQueue -> enqueue (inElement);
		lastOutputTs = inElement.timestamp;
		
		// We expire the oldest element from the window if we have
		// processed more than windowSize elements
		ASSERT (numProcessed <= windowSize);
		if (numProcessed == windowSize) {
			
			// get the oldest tuple
			rc = winSynopsis -> getOldestTuple (oldestTuple,
												oldestTupleTs);			
			if (rc != 0) return rc;
			
			outElement.kind = E_MINUS;
			outElement.tuple = oldestTuple;
			outElement.timestamp = inElement.timestamp;
			
			// delete the oldest element
			rc = winSynopsis -> deleteOldestTuple ();
			if (rc != 0) return rc;
			
			if (!outputQueue -> enqueue (outElement)) {
				bStalled = true;
				stalledElement = outElement;
				break;
			}
			
			// Note: no need to update lastOutputTs
		}
		
		else {
			numProcessed ++;
		}
	}
	
	// Hearbeat generation
	if (!outputQueue -> isFull() && (lastOutputTs < lastInputTs)) {
		outputQueue -> enqueue (Element::Heartbeat (lastInputTs));
	}

#ifdef _MONITOR_
	stopTimer ();
	logOutTs (lastOutputTs);
#endif
	
	return 0;
}

bool RowWindow::clearStall () {
	ASSERT (bStalled);
	
	return (bStalled = !outputQueue -> enqueue (stalledElement));
}
