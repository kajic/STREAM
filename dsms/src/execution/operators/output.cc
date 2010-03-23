#ifndef _OUTPUT_
#include "execution/operators/output.h"
#endif

#ifndef _DEBUG_
#include "common/debug.h"
#endif

/// debug
#include <iostream>
using namespace std;

using namespace Execution;

#define UNLOCK_INPUT_TUPLE(t) (inStore -> decrRef ((t)))

Output::Output(unsigned int _id, std::ostream& _LOG)
	: LOG (_LOG)
{
	id              = _id;
	inputQueue      = 0;
	inStore         = 0;
	numAttrs        = 0;
	output          = 0;
#ifdef _DM_
	lastInputTs     = 0;
#endif
}

Output::~Output() {}

int Output::setInputQueue (Queue *inputQueue)
{
	ASSERT (inputQueue);
	
	this -> inputQueue = inputQueue;
	return 0;
}

int Output::setInStore (StorageAlloc *store)
{
	ASSERT (store);

	this -> inStore = store;
	return 0;
}

int Output::setQueryOutput (Interface::QueryOutput *output)
{
	ASSERT (output);

	this -> output = output;
	return 0;
}

int Output::addAttr (Type type, unsigned int len, Column inCol)
{
	if (numAttrs == MAX_ATTRS)
		return -1;
	
	attrs [numAttrs].type = type;
	attrs [numAttrs].len = len;	
	inCols [numAttrs] = inCol;
	
	numAttrs ++;
	return 0;
}

int Output::initialize()
{
	int rc;
	
	ASSERT (output);
	ASSERT (numAttrs);
	
	if ((rc = output -> setNumAttrs (numAttrs)) != 0)
		return rc;
	
	for (unsigned int a = 0 ; a < numAttrs ; a++) {
		if ((rc = output -> setAttrInfo (a, attrs[a].type, attrs[a].len))
			!= 0)
			return rc;
	}
	
	if ((rc = output -> start()) != 0)
		return rc;
	
	// compute the offsets of the attributes
	unsigned int offset = DATA_OFFSET;
	for (unsigned int a = 0 ; a < numAttrs ; a++) {
		offsets [a] = offset;
		
		switch (attrs [a].type) {
		case INT:   offset += INT_SIZE;      break;
		case FLOAT: offset += FLOAT_SIZE;    break;
		case BYTE:  offset += BYTE_SIZE;     break;
		case CHAR:  offset += attrs [a].len; break;
		default:
			return -1;
		}
	}	
	tupleLen = offset;
	
	return 0;
}

int Output::run (TimeSlice timeSlice)
{
	int rc;
	Element inputElement;
	Tuple inputTuple;
	unsigned int numElements;	
	char effect;

#ifdef _MONITOR_
	startTimer ();
#endif
	
	numElements = timeSlice;

	for (unsigned int e = 0 ; e < numElements ; e++) {
		
		// Get the next element
		if (!inputQueue -> dequeue (inputElement))
			break;
		
		ASSERT (lastInputTs <= inputElement.timestamp);
		
#ifdef _DM_
		lastInputTs = inputElement.timestamp;
#endif
		
		// Ignore heartbeats
		if (inputElement.kind == E_HEARTBEAT)
			continue;
		
		inputTuple = inputElement.tuple;
		
		// Output timestamp
		memcpy(buffer, &inputElement.timestamp, TIMESTAMP_SIZE);
				
		// Output effect: integer 1 for PLUS, integer 2 for MINUS
		effect = (inputElement.kind == E_PLUS)? '+' : '-';		
		memcpy(buffer + EFFECT_OFFSET, &effect, 1);
		
		// Output the remaining attributes
		for (unsigned int a = 0 ; a < numAttrs ; a++) {
			switch (attrs[a].type) {
			case INT:							
				memcpy (buffer + offsets[a],
						&(ICOL(inputTuple, inCols [a])),
						INT_SIZE);
				break;
				
			case FLOAT:
				memcpy (buffer + offsets[a],
						&(FCOL(inputTuple, inCols [a])),
						FLOAT_SIZE);
				break;
				
			case BYTE:
				buffer [offsets[a]] = BCOL(inputTuple,
										   inCols[a]);
				break;
				
			case CHAR:
				
				strncpy (buffer + offsets [a],
						 (CCOL(inputTuple, inCols [a])),
						 attrs [a].len);
				
				break;
				
			default:
				return -1;
			}
		}
		
		UNLOCK_INPUT_TUPLE(inputTuple);
		
		// Output the tuple
		if ((rc = output -> putNext (buffer, tupleLen)) != 0)
			return rc;
	}

#ifdef _MONITOR_
	stopTimer ();
#endif
	
	return 0;
}

