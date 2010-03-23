/**
 * @file       select.cc
 * @date       May 30, 2004
 * @brief      Implementation of the select operator
 */

#ifndef _SELECT_
#include "execution/operators/select.h"
#endif

#ifndef _DEBUG_
#include "common/debug.h"
#endif

using namespace Execution;
using namespace std;

#define UNLOCK_INPUT_TUPLE(t) (inStore -> decrRef ((t)))

Select::Select (unsigned int _id, ostream &_LOG)
	: LOG (_LOG)
{
	id           = _id;
	inputQueue   = 0;
	outputQueue  = 0;
	evalContext  = 0;
	predicate    = 0;
	lastInputTs  = 0;
	lastOutputTs = 0;
	inStore      = 0;
}

Select::~Select() {
	
	if (evalContext)
		delete evalContext;
	
	if (predicate)
		delete predicate;
	
}

int Select::setInputQueue (Queue *inputQueue) 
{
	ASSERT (inputQueue);
	
	this -> inputQueue = inputQueue;
	return 0;
}

int Select::setOutputQueue (Queue *outputQueue) 
{
	ASSERT (outputQueue);
	
	this -> outputQueue = outputQueue;
	return 0;
}

int Select::setInStore (StorageAlloc *store)
{
	ASSERT (store);

	this -> inStore = store;
	return 0;
}

int Select::setPredicate (BEval *predicate) 
{
	ASSERT (predicate);
	
	this -> predicate = predicate;
	return 0;
}

int Select::setEvalContext (EvalContext *evalContext)
{
	ASSERT (evalContext);
	
	this -> evalContext = evalContext;
	return 0;
}

int Select::run (TimeSlice timeSlice) 
{	
	unsigned int   numElements;
	Element        inputElement;

#ifdef _MONITOR_
	startTimer ();
#endif   
	
	// Number of input elements to process
	numElements = timeSlice;

	for (unsigned int e = 0 ; e < numElements ; e++) {
		
		// We are blocked @ output queue
		if (outputQueue -> isFull())
			break;
		
		// No more tuples to process
		if (!inputQueue -> dequeue (inputElement))
			break;
		
		// Timestamp of last input tuple: used in heartbeat generation
		lastInputTs = inputElement.timestamp;
		
		// Heartbeat: no filtering to be done
		if (inputElement.kind == E_HEARTBEAT)
			continue;
		
		evalContext -> bind (inputElement.tuple, INPUT_CONTEXT);
		
		// The tuple satisfies the predicate: enqueue it to the output,
		// remember the timestamp (used in heartbeat generation)		
		if (predicate -> eval()) {			
			outputQueue -> enqueue (inputElement);
			lastOutputTs = inputElement.timestamp;
		}
		
		// Tuple fails to satisfy the predicate: discard the tuple in the
		// input element.
		else {
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
