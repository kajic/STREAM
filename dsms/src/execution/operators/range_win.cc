/**
 * @file       range_win.cc
 * @date       May 30, 2004
 * @brief      Range window operator
 */

#ifndef _DEBUG_
#include "common/debug.h"
#endif

#ifndef _RANGE_WIN_
#include "execution/operators/range_win.h"
#endif

#include <iostream>

using namespace Execution;
using namespace std;

#define LOCK_INPUT_TUPLE(t) (inStore -> addRef ((t)))

RangeWindow::RangeWindow(unsigned int id, std::ostream &_LOG)
	: LOG (_LOG)
{
	this -> id               = id;
	this -> inputQueue       = 0;
	this -> outputQueue      = 0;
	this -> winSynopsis      = 0;
    this -> windowStart      = 0;
	this -> windowSize       = 0;
    this -> strideSize       = 0;
	this -> inStore          = 0;
	this -> bStalled         = false;
	this -> lastInputTs      = 0;
	this -> lastOutputTs     = 0;
}

RangeWindow::~RangeWindow() {}

int RangeWindow::setInputQueue (Queue *inputQueue) 
{
	ASSERT (inputQueue);
	
	this -> inputQueue = inputQueue;
	return 0;
}

int RangeWindow::setOutputQueue (Queue *outputQueue)
{
	ASSERT (outputQueue);
	
	this -> outputQueue = outputQueue;
	return 0;
}

int RangeWindow::setWindowSize (unsigned int windowSize)
{
	ASSERT (windowSize > 0);
	
	this -> windowSize = windowSize;
	return 0;
}

int RangeWindow::setWindowStride (unsigned int strideSize)
{
	this -> strideSize = strideSize;
	return 0;
}


int RangeWindow::setWindowSynopsis (WindowSynopsis *winSynopsis) 
{
	ASSERT (winSynopsis);
	
	this -> winSynopsis = winSynopsis;
	return 0;
}

int RangeWindow::setInStore (StorageAlloc *store)
{
	ASSERT (store);

	this -> inStore = store;
	return 0;
}

int RangeWindow::run(TimeSlice timeSlice)
{
	int rc;
	unsigned int numElements;
	Element      inputElement;
	Tuple        inputTuple;
	Element      outputElement;
    Tuple oldestTuple;
    Timestamp oldestTupleTs;

#ifdef _MONITOR_
	startTimer ();
#endif							
	
	// We are stalled ...
	if (bStalled) {

		// we try clearing it ...
		if ((rc = clearStall()) != 0)
			return rc;
		
		// and return if we fail to.
		if (bStalled) {
#ifdef _MONITOR_
			stopTimer ();
			logOutTs (lastOutputTs);
#endif										
			return 0;
		}
	}
	
	for (unsigned int e = 0 ; e < numElements ; e++) {		
		
		// Get the next element
		if (!inputQueue -> dequeue (inputElement))
			break;
		
		// We should not get MINUS tuples in the input
		ASSERT (inputElement.kind != E_MINUS);
		
		// Input should be timestamp ordered
		ASSERT (lastInputTs <= inputElement.timestamp);

        lastInputTs = inputElement.timestamp;        
		inputTuple = inputElement.tuple;
		
		if (inputElement.kind == E_PLUS) {
			
			LOCK_INPUT_TUPLE (inputElement.tuple);
			
			if ((rc = winSynopsis -> insertTuple (inputTuple,
												  lastInputTs)) != 0)
				return rc;
			
            if (strideSize == 0) {
                // Expire tuples with timestamp <= (lastInputTs - windowSize)
                if (lastInputTs >= windowSize) {
                    if ((rc = expireTuples (lastInputTs - windowSize)) != 0)
                        return rc;
                }
            } 

            else if ((rc = strideExpireTuples()) != 0) {
                return rc;
            }
			
			// expireTuple method sets bStalled flag if it stalled when
			// sending out MINUS expired tuples
			if (bStalled) {
				stalledElement = inputElement;
				
#ifdef _MONITOR_
				stopTimer ();
				logOutTs (lastOutputTs);
#endif											
				return 0;
			}
				
			// We are blocked
			if (outputQueue -> isFull()) {
				bStalled = true;
				stalledElement = inputElement;

#ifdef _MONITOR_
				stopTimer ();
				logOutTs (lastOutputTs);
#endif											
				return 0;
			}
			
			// Generate the output element
			outputElement.kind      = E_PLUS;
			outputElement.timestamp = inputElement.timestamp;
			outputElement.tuple     = inputTuple;
			
			outputQueue -> enqueue (outputElement);
			lastOutputTs = outputElement.timestamp;						
		}
		
		else {
			// Expire tuples with timestamp < (lastInputTs - windowSize)
			if (lastInputTs >= windowSize) {
				if ((rc = expireTuples (lastInputTs - windowSize)) != 0)
					return rc;
            } 

            else if ((rc = strideExpireTuples()) != 0) {
                return rc;
            }
			
			// expireTuple method sets bStalled flag if it stalled when
			// sending out MINUS expired tuples
			if (bStalled) {
				stalledElement = inputElement;

#ifdef _MONITOR_
				stopTimer ();
				logOutTs (lastOutputTs);
#endif							
				
				return 0;
			}	
		}
	}       
	
	ASSERT (!bStalled);	
	if (!outputQueue -> isFull() && (lastOutputTs < lastInputTs))
		outputQueue -> enqueue (Element::Heartbeat(lastInputTs));

#ifdef _MONITOR_
	stopTimer ();
	logOutTs (lastOutputTs);
#endif					

	return 0;
}

int RangeWindow::strideExpireTuples()
{
    int rc;

    if (lastInputTs >= windowStart+windowSize) {
        windowStart += strideSize;
        if ((rc = expireTuples (windowStart-1)) != 0)
            return rc;
    }
    return 0;
}

int RangeWindow::clearStall() 
{ 
    int rc; 
    Element outputElement;

	ASSERT (bStalled);
	
	bStalled = false;
	if (lastInputTs >= windowSize) {
		if ((rc = expireTuples (lastInputTs - windowSize)) != 0)
			return rc;
    } 

    else if ((rc = strideExpireTuples()) != 0) {
        return rc;
    }

	// We stalled yet again trying to expire tuples
	if (bStalled)
		return 0;

	// Nothing more to do for heartbeats
	if (stalledElement.kind == E_HEARTBEAT)
		return 0;
	
	if (outputQueue -> isFull()) {
		bStalled = true;
		return 0;
	}
	
	outputElement.kind         = stalledElement.kind;
	outputElement.timestamp    = stalledElement.timestamp;
	outputElement.tuple        = stalledElement.tuple;
	
	outputQueue -> enqueue (outputElement);
	lastOutputTs = outputElement.timestamp;
	
	return 0;
}

int RangeWindow::expireTuples (Timestamp expTs)
{
	int rc;
	Tuple     oldestTuple;
	Timestamp oldestTupleTs;
	Element   outputElement;
	
	while (true) {	
		ASSERT(!bStalled);
		
		// Window is empty: no tuples to expire
		if (winSynopsis -> isEmpty())
			return 0;
		
		// If the oldest tuple has a timestamp <= expTs, delete it from
		// the window and send a MINUS tuple in the output queue
		rc = winSynopsis -> getOldestTuple (oldestTuple, oldestTupleTs);

		if (rc != 0) return rc;
		
		// No tuples to expire
		if (oldestTupleTs > expTs)
			return 0;
		
		// Output queue is full, we cannot send the MINUS tuple
		if (outputQueue -> isFull()) {
			bStalled = true;
			return 0; 
		}

		// Construct the element corresponding to the MINUS tuples, and
		// send it.
        outputElement.kind      = E_MINUS;
        outputElement.tuple     = oldestTuple;
        if (strideSize == 0) 
            outputElement.timestamp = oldestTupleTs + windowSize;
        else
            outputElement.timestamp = lastInputTs;
        
        outputQueue -> enqueue (outputElement);
        lastOutputTs = outputElement.timestamp;
		
		winSynopsis -> deleteOldestTuple();
	}
	
	return 0;
}
