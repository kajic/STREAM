/**
 * @file         project.cc
 * @date         May 30, 2004
 * @brief        The projection operator
 */

#ifndef _DEBUG_
#include "common/debug.h"
#endif

#ifndef _PROJECT_
#include "execution/operators/project.h"
#endif

using namespace Execution;
using namespace std;

#define LOCK_OUTPUT_TUPLE(t) (outStore -> addRef ((t)))
#define UNLOCK_INPUT_TUPLE(t) (inStore -> decrRef ((t)))

Project::Project(unsigned int id, ostream &_LOG)
	: LOG (_LOG)
{
	this -> id               = id;
	this -> inputQueue       = 0;
	this -> outputQueue      = 0;
	this -> evalContext      = 0;
	this -> projEval         = 0;
	this -> outStore         = 0;
	this -> inStore          = 0;
	this -> outSynopsis      = 0;
	this -> lastInputTs      = 0;
	this -> lastOutputTs     = 0;
}

Project::~Project() {
	if (evalContext)
		delete evalContext;
	if (projEval)
		delete projEval;
}

int Project::setInputQueue (Queue *inputQueue) 
{
	ASSERT (inputQueue);
	
	this -> inputQueue = inputQueue;
	return 0;
}

int Project::setOutputQueue (Queue *outputQueue) 
{
	ASSERT (outputQueue);
	
	this -> outputQueue = outputQueue;
	return 0;
}

int Project::setProjEvaluator (AEval *projEval)
{
	ASSERT (projEval);
	
	this -> projEval = projEval;
	return 0;
}

int Project::setEvalContext (EvalContext *evalContext)
{
	ASSERT (evalContext);
	
	this -> evalContext = evalContext;
	return 0;
}

int Project::setOutStore (StorageAlloc *store)
{
	ASSERT (store);
	
	this -> outStore = store;
	return 0;
}

int Project::setInStore (StorageAlloc *store)
{
	ASSERT (store);
	
	this -> inStore = store;
	return 0;
}

int Project::setOutSynopsis (LineageSynopsis *outSynopsis)
{
	this -> outSynopsis = outSynopsis;
	return 0;
}

int Project::run (TimeSlice timeSlice) 
{
	unsigned int numElements;
	Element      inputElement;
	Tuple        outputTuple;
	Element      outputElement;
	int rc;

#ifdef _MONITOR_
	startTimer ();
#endif
		
	// Number of elements to process
	numElements = timeSlice;
	
	for (unsigned int e = 0 ; e < numElements ; e++) {
		
		// We are blocked @ output queue
		if (outputQueue -> isFull())
			break;
		
		// Get the next input element
		if (!inputQueue -> dequeue(inputElement)) 
			break;		
		
		// Timestamp of last input tuple: used in heartbeat generation
		lastInputTs = inputElement.timestamp;

		// Plus tuple
		if (inputElement.kind == E_PLUS) {
			
			// Allocate space for output tuple
			if ((rc = outStore -> newTuple (outputTuple)) != 0) {				
				return rc;
			}
			
			// Produce the output tuple
			evalContext -> bind (outputTuple, OUTPUT_ROLE);
			evalContext -> bind (inputElement.tuple, INPUT_ROLE);
			projEval -> eval();
			
			// Produce the output element
			outputElement.kind = E_PLUS;
			outputElement.tuple = outputTuple;
			outputElement.timestamp = inputElement.timestamp;		
			
			// enqueue the output element
			outputQueue -> enqueue (outputElement);
			lastOutputTs = outputElement.timestamp;
			
			if (outSynopsis) {
				rc = outSynopsis -> insertTuple (outputTuple, &inputElement.tuple);
				if (rc != 0) return rc;
				
				// Lock tuple to ensure memory manager does not deallocate
				// its space
				LOCK_OUTPUT_TUPLE(outputTuple);
			}
			
			// Discard tuple of input element
			UNLOCK_INPUT_TUPLE(inputElement.tuple);
		}
		
		else if (inputElement.kind == E_MINUS) {
			ASSERT (outSynopsis);
			
			// Get the tuple that we produced for the corresponding PLUS tuple
			rc = outSynopsis -> getTuple (&inputElement.tuple,
										  outputTuple);
			if (rc != 0) return rc;
			
			outputElement.kind = E_MINUS;
			outputElement.tuple = outputTuple;
			outputElement.timestamp = inputElement.timestamp;
			
			outputQueue -> enqueue (outputElement);
			lastOutputTs = outputElement.timestamp;
			
			// Delete the tuple from the synopsis
			rc = outSynopsis -> deleteTuple (outputTuple);
			if (rc != 0) return rc;
			
			// Discard tuple of input element
			UNLOCK_INPUT_TUPLE(inputElement.tuple);
		}
		
		// heartbeats: do nothing
	}
	
	// Generate heartbeat if necessary (and possible without blocking) 
	if (!outputQueue -> isFull() && (lastOutputTs < lastInputTs)) {
		outputQueue -> enqueue (Element::Heartbeat(lastInputTs));
		lastOutputTs = lastInputTs;
	}
	
#ifdef _MONITOR_
	stopTimer ();
	logOutTs (lastOutputTs);
#endif
	
	return 0;
}
	
