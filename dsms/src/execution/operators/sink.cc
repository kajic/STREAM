#ifndef _SINK_
#include "execution/operators/sink.h"
#endif

#ifndef _DEBUG_
#include "common/debug.h"
#endif

using namespace Execution;

#define UNLOCK_INPUT_TUPLE(t) (inStore -> decrRef ((t)))

Sink::Sink (unsigned int id, ostream &_LOG)
	: LOG (_LOG)
{
	this -> id = id;
	this -> inputQueue = 0;
	this -> inStore = 0;
}

Sink::~Sink () {}

int Sink::setInputQueue (Queue *inputQueue)
{
	ASSERT (inputQueue);

	this -> inputQueue = inputQueue;
	return 0;
}

int Sink::setInStore (StorageAlloc *store)
{
	ASSERT (store);

	this -> inStore = store;
	return 0;
}

int Sink::run (TimeSlice timeSlice)
{
	Element inputElement;
	unsigned int numElements;	
	
#ifdef _MONITOR_
	startTimer ();
#endif
	
	numElements = timeSlice;
	
	for (unsigned int e = 0 ; e < numElements ; e++) {
		// Get the next element
		if (!inputQueue -> dequeue (inputElement))
			break;
		
		// Ignore heartbeats
		if (inputElement.kind != E_HEARTBEAT)
			UNLOCK_INPUT_TUPLE (inputElement.tuple);
	}

#ifdef _MONITOR_
	stopTimer ();
#endif
	
	return 0;
}
