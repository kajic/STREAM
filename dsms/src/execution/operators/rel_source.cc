/**
 * @file     rel_source.cc
 * @date     Sep 8, 2004
 * @brief    Relation source implementation
 */

#ifndef _REL_SOURCE_
#include "execution/operators/rel_source.h"
#endif

#ifndef _DEBUG_
#include "common/debug.h"
#endif

using namespace Execution;
using namespace std;

#define LOCK_OUTPUT_TUPLE(t) (store -> addRef ((t)))

RelSource::RelSource (unsigned int id, ostream &_LOG)
	: LOG (_LOG) 
{
	this -> id = id;
	this -> outputQueue = 0;
	this -> source = 0;
	this -> store = 0;
	this -> rel = 0;
	this -> evalContext = 0;
	this -> lastOutputTs = 0;
	this -> lastInputTs = 0;
	this -> numAttrs = 0;
}

RelSource::~RelSource ()
{
	if (evalContext)
		delete evalContext;
	
}

//----------------------------------------------------------------------
// Initialization routines
//----------------------------------------------------------------------

int RelSource::setOutputQueue (Queue *outputQueue)
{
	ASSERT (outputQueue);

	this -> outputQueue = outputQueue;
	return 0;
}

int RelSource::setSource (Interface::TableSource *source)
{
	ASSERT (source);

	this -> source = source;
	return 0;
}

int RelSource::setStore (StorageAlloc *store)
{
	ASSERT (store);

	this -> store = store;
	return 0;
}

int RelSource::setSynopsis (RelationSynopsis *syn)
{
	ASSERT (syn);
	
	this -> rel = syn;
	return 0;
}

int RelSource::setScan (unsigned int scanId)
{
	this -> scanId = scanId;
	return 0;
}

int RelSource::setEvalContext (EvalContext *evalContext)
{
	ASSERT (evalContext);
	
	this -> evalContext = evalContext;
	return 0;
}

int RelSource::addAttr (Type type, unsigned int len, Column outCol)
{
	ASSERT (numAttrs < MAX_ATTRS);
	
	attrs [numAttrs].type = type;
	attrs [numAttrs].len = len;
	
	offsets [numAttrs] = (numAttrs > 0)?
		(offsets [numAttrs - 1] + attrs [numAttrs - 1].len) : DATA_OFFSET;		
	
	outCols [numAttrs] = outCol;
	
	numAttrs++;
	return 0;
}

int RelSource::setMinusTuple (Tuple tuple)
{
	this -> minusTuple = tuple;
	return 0;
}

int RelSource::initialize ()
{
	int rc;
	
	if ((rc = source -> start()) != 0)
		return rc;
	return 0;
}

int RelSource::run (TimeSlice timeSlice)
{
	int rc;
	unsigned int numElements;
	char *inputTuple;
	unsigned int inputTupleLen;
	Timestamp inputTupleTs;
	char inputTupleSign;
	Tuple outTuple;
	Element outElement;
	bool bHeartbeat;

#ifdef _MONITOR_
	startTimer ();
#endif							

	numElements = timeSlice;
	
	for (unsigned int e = 0 ; e < numElements ; e++) {
		
		// We are stalled at the output queue.
		if (outputQueue -> isFull())
			break;
		
		// Get the next input tuple
		if ((rc = source -> getNext (inputTuple,
									 inputTupleLen,
									 bHeartbeat)) != 0)
			return rc;
		
		// We do not have an input tuple (element) to process
		if (!inputTuple)
			break;
		
		// Timestamp 
		memcpy(&inputTupleTs, inputTuple, TIMESTAMP_SIZE);

		// We should have a progress of time.
		if (lastInputTs > inputTupleTs) {
			LOG << "RelationSource: input not in timestamp order" << endl;
			return -1;
		}		
		lastInputTs = inputTupleTs;		
		
		// ignore heartbeats
		if (bHeartbeat) {
			LOG << "Relationsource: Heartbeat received" << endl;
			continue;
		}
		LOG << "Relationsource: Tuple received" << endl;
		
		// All data tuple lengths are fixed 
		ASSERT (inputTupleLen ==
				offsets[numAttrs-1] + attrs[numAttrs-1].len);		
		
		// Sign
		inputTupleSign = inputTuple [SIGN_OFFSET];
		
		if ((inputTupleSign != PLUS) && (inputTupleSign != MINUS)) {
			LOG << "RelationSource: Invalid sign for tuple" << endl;
			return -1;
		}
		
		if (inputTupleSign == PLUS) { 
			if ((rc = store -> newTuple (outTuple)) != 0) 
				return rc;
			
			if ((rc = decodeAttrs (inputTuple, outTuple)) != 0) 
				return rc;
			
			if ((rc = rel -> insertTuple (outTuple)) != 0) 
				return rc;
			LOCK_OUTPUT_TUPLE (outTuple);
		}
		
		else {			
			
			if ((rc = decodeAttrs (inputTuple, minusTuple)) != 0) 
				return rc;
			
			if ((rc = getSynTuple (minusTuple, outTuple)) != 0) 
				return rc;

			if ((rc = rel -> deleteTuple (outTuple)) != 0)
				return rc;
			
		}
		
		outElement.kind = (inputTupleSign == '+')? E_PLUS : E_MINUS;
		outElement.tuple = outTuple;
		outElement.timestamp = inputTupleTs;
		
		outputQueue -> enqueue (outElement);
		lastOutputTs = inputTupleTs;
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

int RelSource::decodeAttrs (char *inTuple, Tuple outTuple)
{	      
	for (unsigned int a = 0 ; a < numAttrs ; a++) {
		switch (attrs [a].type) {
		case INT:
			memcpy (&ICOL(outTuple, outCols[a]),
					inTuple + offsets[a], INT_SIZE);				
			break;
				
		case FLOAT:
			memcpy (&FCOL(outTuple, outCols[a]),
					inTuple + offsets[a], FLOAT_SIZE);						
			break;
				
		case CHAR:
			strncpy (CCOL(outTuple, outCols[a]),
					 inTuple + offsets[a], attrs [a].len);
			break;
			
		case BYTE:
			BCOL (outTuple, outCols[a]) = inTuple [offsets[a]];
			break;
			
		default:
			ASSERT (0);
			break;
		}
	}
	
	return 0;
}

int RelSource::getSynTuple (Tuple inTuple, Tuple &outTuple)
{
	int rc;
	TupleIterator *scan;
	
	evalContext -> bind (inTuple, INPUT_ROLE);
	
	if ((rc = rel -> getScan (scanId, scan)) != 0)
		return rc;
	
	if (!scan -> getNext (outTuple)) {
		if ((rc = rel -> releaseScan (scanId, scan)) != 0)
			return rc;
		return -1;
	}
	
	if ((rc = rel -> releaseScan (scanId, scan)) != 0)
		return rc;	
	
	return 0;
}
