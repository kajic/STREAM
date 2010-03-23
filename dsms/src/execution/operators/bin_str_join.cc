/**
 * @file           bin_str_join.cc
 * @date           May 30, 2004
 * @brief          Binary stream join
 */

#ifndef _DEBUG_
#include "common/debug.h"
#endif

#ifndef _BIN_STR_JOIN_
#include "execution/operators/bin_str_join.h"
#endif

#define MIN(a,b) (((a) < (b))? (a) : (b))

#define LOCK_INNER_TUPLE(t) (innerInStore -> addRef ((t)))
#define LOCK_OUTPUT_TUPLE(t) (outStore -> addRef ((t)))
#define UNLOCK_OUTPUT_TUPLE(t) (outStore -> decrRef ((t)))
#define UNLOCK_INNER_TUPLE(t) (innerInStore -> decrRef ((t)))
#define UNLOCK_OUTER_TUPLE(t) (outerInStore -> decrRef ((t)))
#define LOCK_OUTER_TUPLE(t) (outerInStore -> addRef ((t)))


using namespace Execution;
using namespace std;
BinStreamJoin::BinStreamJoin(unsigned int id, std::ostream &_LOG)
	: LOG (_LOG)
{
	this -> id                      = id;
	this -> outerInputQueue         = 0;
	this -> innerInputQueue         = 0;
	this -> outputQueue             = 0;	
	this -> innerSynopsis           = 0;
	this -> scanId                  = 0;
	this -> outStore                = 0;
	this -> outerInStore            = 0;
	this -> innerInStore            = 0;
	this -> evalContext             = 0;
	this -> outputConstructor       = 0;	
	this -> lastOuterTs             = 0;
	this -> lastInnerTs             = 0;
	this -> lastOutputTs            = 0;
	
	this -> bStalled                = false;
	this -> innerScanWhenStalled    = 0;
}

BinStreamJoin::~BinStreamJoin()
{
	if (evalContext)
		delete evalContext;
	if (outputConstructor)
		delete outputConstructor;	
}

int BinStreamJoin::setOuterInputQueue (Queue *outerQueue) 
{
	ASSERT (outerQueue);
	
	this -> outerInputQueue = outerQueue;
	return 0;
}

int BinStreamJoin::setInnerInputQueue (Queue *innerQueue)
{
	ASSERT (innerQueue);
	
	this -> innerInputQueue = innerQueue;
	return 0;
}

int BinStreamJoin::setOutputQueue (Queue *outputQueue)
{
	ASSERT (outputQueue);
	this -> outputQueue = outputQueue;
	return 0;
}

int BinStreamJoin::setSynopsis (RelationSynopsis *innerSynopsis)
{
	ASSERT (innerSynopsis);
	
	this -> innerSynopsis = innerSynopsis;
	return 0;
}

int BinStreamJoin::setEvalContext (EvalContext *evalContext)
{
	ASSERT (evalContext);
	
	this -> evalContext = evalContext;
	return 0;
}

int BinStreamJoin::setScan (unsigned int scanId)
{	
	this -> scanId   = scanId;
	return 0;
}

int BinStreamJoin::setOutputStore (StorageAlloc *store)
{
	ASSERT (store);
	
	this -> outStore = store;
	return 0;
}

int BinStreamJoin::setOuterInputStore (StorageAlloc* store)
{
	ASSERT (store);

	this -> outerInStore = store;
	return 0;
}

int BinStreamJoin::setInnerInputStore (StorageAlloc* store)
{
	ASSERT (store);

	this -> innerInStore = store;
	return 0;
}

int BinStreamJoin::setOutputConstructor (AEval *outputConstructor)
{
	ASSERT (outputConstructor);
	
	this -> outputConstructor = outputConstructor;
	return 0;
}

int BinStreamJoin::run(TimeSlice timeSlice)
{
	int rc;
	unsigned int numElements;
	Timestamp    innerMinTs, outerMinTs;
	Element      outerPeekElement, innerPeekElement;	
	Element      outerElement, innerElement;


#ifdef _MONITOR_
	startTimer ();
#endif
	
	// I have a stall 
	if (bStalled) {
		// Try clearing it ...
		if ((rc = clearStall()) != 0)
			return rc;
		
		// ... I failed 
		if (bStalled) {

#ifdef _MONITOR_
			stopTimer ();
			logOutTs (lastOutputTs);
#endif
			
			return 0;
		}
	}
	
	// Minimum timestamp possible on the next outer element
	outerMinTs = lastOuterTs;
	
	// Minimum timestamp possible on the next inner element
	innerMinTs = lastInnerTs;

	numElements = timeSlice;
	
	for (unsigned int e = 0 ; (e < numElements) && !bStalled ; e++) {		
		
		// Peek to revise min timestamp estimate
		if (outerInputQueue -> peek (outerPeekElement))
			outerMinTs = outerPeekElement.timestamp;
		
		// Peek to revise min inner timestamp estimate
		if (innerInputQueue -> peek (innerPeekElement))
			innerMinTs = innerPeekElement.timestamp;
		
		if (outerMinTs < innerMinTs) {
			
			// If outer has an element, then I know that all future
			// elements of inner have a timestamp *strictly* greater than
			// the timestamp of this (next outer) element.  Note that
			// "strictly greater" is necessary for correct semantics						
			if (outerInputQueue -> dequeue(outerElement)) {
				lastOuterTs = outerElement.timestamp;
				
				// Heartbeats require no processing
				if (outerElement.kind == E_HEARTBEAT)
					continue;
				
				// We might stall inside processOuter, in which case
				// bStalled is set.  Note that the for loop terminates in
				// that case.
				if ((rc = processOuter (outerElement)) != 0)
					return rc;
				
				UNLOCK_OUTER_TUPLE (outerElement.tuple);
			}
			
			// If outer does not have an element, I cannot do any
			// processing ... 
			else {				
				break;
			}
		}
		
		else {
			
			// If inner has an element, then I know that all future
			// elements of outer have a timestamp at least as much as that
			// of this (next inner) element.  
			if (innerInputQueue -> dequeue (innerElement)) {
				lastInnerTs = innerElement.timestamp;
				
				// Heartbeats require no processing
				if (innerElement.kind == E_HEARTBEAT)
					continue;
				
				// processInner does not generate any output -- so we will
				// never stall here.
				if ((rc = processInner (innerElement)) != 0)
					return rc;
				
				UNLOCK_INNER_TUPLE (innerElement.tuple);
			}
			
			// Else I am stuck ...
			else {
				break;
			}
		}
	}
	
	// Heartbeat generation
	if (!outputQueue -> isFull() && (lastOutputTs < innerMinTs) &&
		(lastOutputTs < outerMinTs)) {
		ASSERT (!bStalled);
		
		lastOutputTs = MIN (outerMinTs, innerMinTs);
		
		outputQueue -> enqueue (Element::Heartbeat (lastOutputTs));
	}
	
#ifdef _MONITOR_
	stopTimer ();
	logOutTs (lastOutputTs);
#endif							
	
	return 0;
}

int BinStreamJoin::processInner (Element innerElement)
{		
	int rc;
	
	if (innerElement.kind == E_PLUS) {

		LOCK_INNER_TUPLE (innerElement.tuple);
		
		rc = innerSynopsis -> insertTuple (innerElement.tuple);
		if (rc != 0) return rc;
		
	}
	else {
		
		ASSERT (innerElement.kind == E_MINUS);
		
		rc = innerSynopsis -> deleteTuple (innerElement.tuple);
		if (rc != 0) return rc;
		
		UNLOCK_INNER_TUPLE (innerElement.tuple);
	}
	
	return 0;
}

int BinStreamJoin::processOuter (Element outerElement)
{
	int rc;
	TupleIterator *innerScan;
	Tuple joinTuple;
	Tuple outputTuple;
	Element outputElement;

#ifdef _MONITOR_
	logInput ();
#endif
	
	ASSERT (!bStalled && !innerScanWhenStalled);
	
	// Recall: outer is a stream
	ASSERT (outerElement.kind == E_PLUS);
	
	evalContext -> bind (outerElement.tuple, OUTER_ROLE); 
	
	if ((rc = innerSynopsis -> getScan (scanId, innerScan)) != 0)
		return rc;
	
	while (!outputQueue -> isFull() && innerScan -> getNext (joinTuple)) {
		
		if ((rc = outStore -> newTuple (outputTuple)) != 0)
			return rc;
		
		evalContext -> bind (joinTuple, INNER_ROLE);
		evalContext -> bind (outputTuple, OUTPUT_ROLE);
		outputConstructor -> eval();
		
		outputElement.kind      = E_PLUS;
		outputElement.tuple     = outputTuple;
		outputElement.timestamp = outerElement.timestamp;
		
		outputQueue -> enqueue (outputElement);
		lastOutputTs = outputElement.timestamp;

#ifdef _MONITOR_
		logJoin ();
#endif

	}
	
	// We stalled: It is possible that innerScan is also over, in which
	// case we are strictly not stalled, but we will discover that late
	// any way ...
	if (outputQueue -> isFull()) {
		bStalled = true;
		innerScanWhenStalled = innerScan;
		stallTuple = outerElement.tuple;
		
		LOCK_OUTER_TUPLE (stallTuple);
	}
	
	else {
		if ((rc = innerSynopsis -> releaseScan (scanId, innerScan)) != 0)
			return rc;
	}
	
	return 0;
}

int BinStreamJoin::clearStall ()
{
	int rc;
	TupleIterator *innerScan;
	Tuple joinTuple;
	Tuple outputTuple;
	Element outputElement;
	
	ASSERT (bStalled);
	ASSERT (innerScanWhenStalled);	
	
	innerScan = innerScanWhenStalled;	
	while (!outputQueue -> isFull () && innerScan -> getNext (joinTuple)) {
		
		if ((rc = outStore -> newTuple (outputTuple)) != 0)
			return rc;
		
		evalContext -> bind (joinTuple, INNER_ROLE);
		evalContext -> bind (outputTuple, OUTPUT_ROLE);
		outputConstructor -> eval();
		
		outputElement.kind      = E_PLUS;
		outputElement.tuple     = outputTuple;
		outputElement.timestamp = lastOuterTs;
		
		outputQueue -> enqueue (outputElement);
		lastOutputTs = lastOuterTs;

#ifdef _MONITOR_
		logJoin ();
#endif
		
	}
	
	// We cleared the stall
	if (!outputQueue -> isFull()) {
		bStalled = false;
		innerScanWhenStalled = 0;
		
		if ((rc = innerSynopsis -> releaseScan (scanId, innerScan)) != 0)
			return rc;
		
		UNLOCK_OUTER_TUPLE (stallTuple);
	}
	
	return 0;
}
