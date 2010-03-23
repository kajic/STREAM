#ifndef _STREAM_SOURCE_
#include "execution/operators/stream_source.h"
#endif

#ifndef _DEBUG_
#include "common/debug.h"
#endif

#include <string.h>

using namespace Execution;
using namespace std;

StreamSource::StreamSource (unsigned int _id, std::ostream& _LOG)
	: LOG (_LOG)
{
	id             = _id;
	outputQueue    = 0;
	storeAlloc     = 0;
	numAttrs       = 0;
	source         = 0;
	lastInputTs    = 0;
	lastOutputTs   = 0;
}

StreamSource::~StreamSource() {}

int StreamSource::setOutputQueue (Queue *outputQueue)
{
	ASSERT (outputQueue);

	this -> outputQueue = outputQueue;
	return 0;
}

int StreamSource::setStoreAlloc (StorageAlloc *storeAlloc)
{
	ASSERT (storeAlloc);

	this -> storeAlloc = storeAlloc;
	return 0;
}

int StreamSource::setTableSource (Interface::TableSource *source)
{
	ASSERT (source);

	this -> source = source;
	return 0;
}

int StreamSource::addAttr (Type type, unsigned int len, Column outCol)
{
	// We do not have space
	if (numAttrs == MAX_ATTRS)
		return -1;

	attrs [numAttrs].type = type;
	attrs [numAttrs].len = len;
	outCols [numAttrs] = outCol;
	
	numAttrs ++;
	return 0;
}

int StreamSource::initialize ()
{
	int rc;
	unsigned int offset;
	
	ASSERT (numAttrs > 0);
	ASSERT (source);
	
	offset = DATA_OFFSET;
	for (unsigned int a = 0 ; a < numAttrs ; a++) {
		offsets [a] = offset;
		
		switch (attrs [a].type) {
		case INT:   offset += INT_SIZE; break;
		case FLOAT: offset += FLOAT_SIZE; break;
		case BYTE:  offset += BYTE_SIZE; break;
		case CHAR:  offset += attrs[a].len; break;
		default:
			return -1;
		}
	}
	
	if ((rc = source -> start ()) != 0)
		return -23;
	
	return 0;
}

int StreamSource::run (TimeSlice timeSlice)
{
	int rc;
	unsigned int  numElements;
	char         *inputTuple;
	Timestamp     inputTs;
	unsigned int  inputTupleLen;
	Tuple         outputTuple;
	bool          bHeartbeat;
	
#ifdef _MONITOR_
	startTimer ();
#endif							
	
	numElements = timeSlice;	
	for (unsigned int e = 0 ; e < numElements ; e++) {
		
		// We are blocked @ the output queue
		if (outputQueue -> isFull()) 
			break;
		
		// Get the next input tuple
		if ((rc = source -> getNext (inputTuple,
									 inputTupleLen,
									 bHeartbeat)) != 0)
			return rc;
		
		// We do not have an input tuple yet
		if (!inputTuple)
			break;
		
		// Get the timestamp: which is the first field
		memcpy (&inputTs, inputTuple, TIMESTAMP_SIZE);
		
		// We should have a progress of time.
		if (lastInputTs > inputTs) {
			LOG << "StreamSource: input not in timestamp order" << endl;
			return -1;
		}
		
		lastInputTs = inputTs;
		
		// Ignore heartbeats
		if (bHeartbeat) {
			LOG << "Heartbeat received" << endl;
			continue;
		}
		
		// Get the storage for the output tuple
		if ((rc = storeAlloc -> newTuple (outputTuple)) != 0)
			return rc;
		
		// Get the attributes
		for (unsigned int a = 0 ; a < numAttrs ; a++) {
			switch (attrs [a].type) {				
			case INT:
				memcpy (&ICOL(outputTuple, outCols[a]), 
						inputTuple + offsets[a], INT_SIZE);
				break;
				
			case FLOAT:
				memcpy (&FCOL(outputTuple, outCols[a]),
						inputTuple + offsets[a], FLOAT_SIZE);
				break;
				
			case BYTE:
				BCOL(outputTuple, outCols[a]) = inputTuple[offsets[a]];
				break;
				
			case CHAR:
				strncpy (CCOL(outputTuple, outCols[a]),
						 inputTuple + offsets[a],
						 attrs[a].len);
				break;
				
			default:
				// Should not come
				return -1;
			}
		}
		
		outputQueue -> enqueue (Element(E_PLUS, outputTuple, inputTs));
		lastOutputTs = inputTs;
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
								
   
