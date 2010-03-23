#ifndef _BIN_JOIN_
#include "execution/operators/bin_join.h"
#endif

#ifndef _DEBUG_
#include "common/debug.h"
#endif

#define MIN(a,b) (((a) < (b))? (a) : (b))

#define LOCK_OUT_TUPLE(t) (outStore -> addRef ((t)))
#define LOCK_INNER_TUPLE(t) (innerInputStore -> addRef ((t)))
#define LOCK_OUTER_TUPLE(t) (outerInputStore -> addRef ((t)))
#define UNLOCK_INNER_TUPLE(t) (innerInputStore -> decrRef ((t)))
#define UNLOCK_OUTER_TUPLE(t) (outerInputStore -> decrRef ((t)))

using namespace Execution;
using namespace std;

BinaryJoin::BinaryJoin (unsigned int id, ostream &_LOG)
	: LOG (_LOG)
{
	this -> id = id;
	this -> outerInputQueue = 0;
	this -> innerInputQueue = 0;
	this -> outputQueue = 0;
	this -> innerSynopsis = 0;
	this -> outerSynopsis = 0;
	this -> joinSynopsis = 0;
	this -> outStore = 0;
	this -> innerInputStore = 0;
	this -> outerInputStore = 0;
	this -> evalContext = 0;
	this -> outputConstructor = 0;
	this -> lastOuterTs = 0;
	this -> lastInnerTs = 0;
	this -> lastOutputTs = 0;
	this -> bStalled = false;
	this -> scanWhenStalled = 0;
}

BinaryJoin::~BinaryJoin () {
	if (evalContext)
		delete evalContext;
	if (outputConstructor)
		delete outputConstructor;
}

int BinaryJoin::setOuterInputQueue (Queue *outerQueue)
{
	ASSERT (outerQueue);

	this -> outerInputQueue = outerQueue;
	return 0;
}

int BinaryJoin::setInnerInputQueue(Queue *innerQueue)
{
	ASSERT (innerQueue);

	this -> innerInputQueue = innerQueue;
	return 0;
}

int BinaryJoin::setOutputQueue (Queue *outputQueue)
{
	ASSERT (outputQueue);

	this -> outputQueue = outputQueue;
	return 0;
}

int BinaryJoin::setInnerSynopsis (RelationSynopsis *innerSynopsis)
{
	ASSERT (innerSynopsis);

	this -> innerSynopsis = innerSynopsis;
	return 0;
}

int BinaryJoin::setOuterSynopsis(RelationSynopsis *outerSynopsis)
{
	ASSERT (outerSynopsis);

	this -> outerSynopsis = outerSynopsis;
	return 0;
}

int BinaryJoin::setJoinSynopsis (LineageSynopsis *joinSynopsis)
{
	this -> joinSynopsis = joinSynopsis;
	return 0;
}

int BinaryJoin::setInnerScan (unsigned int innerScanId)
{
	this -> innerScanId = innerScanId;
	return 0;
}

int BinaryJoin::setOuterScan (unsigned int outerScanId)
{
	this -> outerScanId = outerScanId;
	return 0;
}

int BinaryJoin::setOutStore (StorageAlloc *outStore)
{
	ASSERT (outStore);
	
	this -> outStore = outStore;
	return 0;
}

int BinaryJoin::setOuterInputStore (StorageAlloc *store)
{
	ASSERT(store);

	this -> outerInputStore = store;
	return 0;
}

int BinaryJoin::setInnerInputStore (StorageAlloc *store)
{
	ASSERT (store);
	
	this -> innerInputStore = store;
	return 0;
}

int BinaryJoin::setEvalContext (EvalContext *evalContext)
{
	ASSERT (evalContext);
	
	this -> evalContext = evalContext;
	return 0;
}

int BinaryJoin::setOutputConstructor (AEval *outputConstructor)
{
	ASSERT (outputConstructor);
	
	this -> outputConstructor = outputConstructor;
	return 0;
}

int BinaryJoin::run (TimeSlice timeSlice)
{
	int rc;
	unsigned int numElements;
	Timestamp outerMinTs, innerMinTs;
	Element outerPeekElement, innerPeekElement;
	Element outerElement, innerElement;
	
#ifdef _MONITOR_
	startTimer ();
#endif
	
	// I have a stall ... 
	if (bStalled) {
		
		if ((rc = clearStall()) != 0) 
			return rc;		
		
		// ... and can't clear it
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
		
		// We have to process the outer if it has an element waiting in
		// the queue.  Otherwise we cannot do any processing
		if (outerMinTs < innerMinTs) {
			
			if (!outerInputQueue -> dequeue (outerElement))
				break;
			
			// update last outer ts
			lastOuterTs = outerElement.timestamp;
			
			// Processing of  data tuples varies depending  on whether the
			// element is a PLUS or MINUS.  The functions processOuterPlus
			// and processOuterMinus could  lead to stalling (output queue
			// is  full while the  operator has  elements to  output).  In
			// this case, the variable bStalled is set, which terminates
			// the forloop.
			
			if (outerElement.kind == E_PLUS) {
				
				rc = processOuterPlus (outerElement);
				if (rc != 0)
					return rc;
				
			}
			
			else if (outerElement.kind == E_MINUS) {
				
				rc = processOuterMinus (outerElement);
				if (rc != 0)
					return rc;
				
				// Discard the tuple
				UNLOCK_OUTER_TUPLE(outerElement.tuple);
			}
			
			// Heartbeats require no processing
#ifdef _DM_
			else {
				ASSERT (outerElement.kind == E_HEARTBEAT);
			}
#endif			
			
		}
		
		// This is symmetric to the outer case: We have to process the
		// outer if it has an element waiting in the queue.  Otherwise we
		// cannot do any processing		
		else if (innerMinTs < outerMinTs) {

			if (!innerInputQueue -> dequeue (innerElement))
				break;

			lastInnerTs = innerElement.timestamp;
			
			if (innerElement.kind == E_PLUS) {
				
				rc = processInnerPlus (innerElement);
				if (rc != 0)
					return rc;
				
			}
			else if (innerElement.kind == E_MINUS) {
				
				rc = processInnerMinus (innerElement);
				if (rc != 0)
					return rc;
				
				UNLOCK_INNER_TUPLE(innerElement.tuple);
			}
			
#ifdef _DM_
			else {
				ASSERT (innerElement.kind == E_HEARTBEAT);
			}
#endif

		}
		
		// innerMinTs == outerMinTs: In this case, we can pick any queue
		// that has an element.
		else if (outerInputQueue -> dequeue (outerElement)) {

			lastOuterTs = outerElement.timestamp;
			
			if (outerElement.kind == E_PLUS) {
				
				rc = processOuterPlus (outerElement);
				if (rc != 0)
					return rc;

			}
			else if (outerElement.kind == E_MINUS) {
				
				rc = processOuterMinus (outerElement);
				if (rc != 0)
					return rc;

				UNLOCK_OUTER_TUPLE(outerElement.tuple);
			}
			
			// Heartbeats require no processing
#ifdef _DM_
			else {
				ASSERT (outerElement.kind == E_HEARTBEAT);
			}
#endif
						
		}
		
		else if (innerInputQueue -> dequeue (innerElement)) {

			lastInnerTs = innerElement.timestamp;
			
			if (innerElement.kind == E_PLUS) {

				rc = processInnerPlus (innerElement);
				if (rc != 0)
					return rc;

			}
			else if (innerElement.kind == E_MINUS) {
				
				rc = processInnerMinus (innerElement);
				if (rc != 0)
					return rc;

				UNLOCK_INNER_TUPLE(innerElement.tuple);
			}
			
#ifdef _DM_
			else {
				ASSERT (innerElement.kind == E_HEARTBEAT);
			}
#endif
			
		}
		
		else {
			// both queues are empty
			break;
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

/**
 * Join an outer tuple with the synopsis of the inner.  The outer tuple is
 * part of an PLUS element.  The join of the outer tuple and an inner
 * tuple if produced as a PLUS element.
 */

int BinaryJoin::processOuterPlus (Element outerElement)
{	
	int rc;

#ifdef _MONITOR_
	logInput ();
#endif
	
	// Iterator that scans the inner
	TupleIterator *innerScan;
	
	// Tuple of inner that joins with outerElement.tuple
	Tuple innerTuple;
	
	// Joined + possibly projected output tuple
	Tuple outputTuple;
	
	// The output element
	Element outputElement;
	
	evalContext -> bind (outerElement.tuple, OUTER_ROLE);
	
	// Insert the outer tuple into outerSynopsis
	if ((rc = outerSynopsis -> insertTuple (outerElement.tuple)) != 0)
		return rc;
	
	// Scan of inner tuples that join with outer tuple
	if ((rc = innerSynopsis -> getScan (innerScanId, innerScan)) != 0)
		return rc;
	
	while (!outputQueue -> isFull() && innerScan -> getNext(innerTuple)) {
		
		// allocate space for the output tuple
		if ((rc = outStore -> newTuple (outputTuple)) != 0)
			return rc;
		
		// construct the output tuple
		evalContext -> bind (innerTuple, INNER_ROLE);
		evalContext -> bind (outputTuple, OUTPUT_ROLE);		
		outputConstructor -> eval ();
		
		// We have to insert this tuple in joinSynopsis (if it exists) to
		// enable us to produce MINUS tuples (see processOuterMinus & 
		// processInnerMinus)
		
		if (joinSynopsis) {
			
			// Lineage for a tuple is the set of tuples that produced it. 
			lineage [0] = outerElement.tuple;
			lineage [1] = innerTuple;
			
			rc = joinSynopsis -> insertTuple (outputTuple, lineage);
			if (rc != 0)									  
				return rc;

			LOCK_OUT_TUPLE (outputTuple);
		}
		
		// construct output element
		outputElement.kind = E_PLUS;
		outputElement.tuple = outputTuple;
		outputElement.timestamp = outerElement.timestamp;

		outputQueue -> enqueue (outputElement);
		lastOutputTs = outputElement.timestamp;
		
#ifdef _MONITOR_
		logJoin ();
#endif
		
	}
	
	// We stalled: It is possible that innerScan is also over, in which
	// case we are strictly not stalled, but ignoring this case simplifies
	// code.  We will discover that we are not really stalled later ...
	if (outputQueue -> isFull()) {
		bStalled = true;
		stallType = OUTER_PLUS;
		scanWhenStalled = innerScan;
		stallElement = outerElement;		
	}
	
	else {
		if ((rc = innerSynopsis -> releaseScan (innerScanId, innerScan)) != 0)
			return rc;
	}
	
	return 0;
}

int BinaryJoin::processOuterMinus (Element outerElement)
{
	int rc;

	// Tuple iterator of the inner
	// Iterator that scans the inner
	TupleIterator *innerScan;
	
	// Tuple of inner that joins with outerElement.tuple
	Tuple innerTuple;
	
	// Joined + possibly projected output tuple
	Tuple outputTuple;
	
	// The output element
	Element outputElement;
	
	evalContext -> bind (outerElement.tuple, OUTER_ROLE);

	// Delete the tuple from the outer synopsis
	if ((rc = outerSynopsis -> deleteTuple (outerElement.tuple)) != 0)
		return rc;
	UNLOCK_OUTER_TUPLE (outerElement.tuple);
	
	// Scan of inner tuples that join with outer tuple
	if ((rc = innerSynopsis -> getScan (innerScanId, innerScan)) != 0)
		return rc;
	
	while (!outputQueue -> isFull() && innerScan -> getNext(innerTuple)) {
		
		// If we are maintaining the joinSynopsis, we can use it to
		// directly retrieve the output tuple.  The output tuple would
		// have been created when the PLUS tuples were joined [[
		// Explanation ]]
		
		if (joinSynopsis) {
			
			lineage [0] = outerElement.tuple;
			lineage [1] = innerTuple;
			
			rc = joinSynopsis -> getTuple (lineage, outputTuple);
			if (rc != 0) return rc;			
			
			rc = joinSynopsis -> deleteTuple (outputTuple);
			if (rc != 0) return rc;
		}

		// Otherwise  we  freshly create  a  new  tuple.   Note taht  this
		// violates  the   requirement  that   a  PLUS  element   and  its
		// corresponding MINUS element refer to the same data tuple - this
		// requirement is  needed only when  the tuples are  inserted into
		// synopses upstream.  So the fact that we are here means that
		// there are no synopses upstream for these tuples ...		
		
		else {
			
			// allocate space for the output tuple
			if ((rc = outStore -> newTuple (outputTuple)) != 0)
				return rc;
			
			// construct the output tuple
			evalContext -> bind (innerTuple, INNER_ROLE);
			evalContext -> bind (outputTuple, OUTPUT_ROLE);		
			outputConstructor -> eval ();
		}
		
		// output element
		outputElement.kind = E_MINUS;
		outputElement.tuple = outputTuple;
		outputElement.timestamp = outerElement.timestamp;

		outputQueue -> enqueue (outputElement);
		lastOutputTs = outputElement.timestamp;		
	}
	
	// We stalled: It is possible that innerScan is also over, in which
	// case we are strictly not stalled, but ignoring this case simplifies
	// code.  We will discover that we are not really stalled later ...
	
	if (outputQueue -> isFull()) {
		bStalled = true;
		stallType = OUTER_MINUS;
		scanWhenStalled = innerScan;
		stallElement = outerElement;
		LOCK_OUTER_TUPLE (stallElement.tuple);
	}
	
	else {
		if ((rc = innerSynopsis -> releaseScan (innerScanId, innerScan)) != 0)
			return rc;
	}
	
	return 0;
}

/**
 * Join an inner tuple with the synopsis of the outer.  The inner tuple is
 * part of an PLUS element.  The join of the inner tuple and an outer
 * tuple if produced as a PLUS element.
 */

int BinaryJoin::processInnerPlus (Element innerElement)
{	
	int rc;

#ifdef _MONITOR_
	logInput ();
#endif
	
	// Iterator that scans the outer
	TupleIterator *outerScan;
	
	// Tuple of outer that joins with innerElement.tuple
	Tuple outerTuple;
	
	// Joined + possibly projected output tuple
	Tuple outputTuple;
	
	// The output element
	Element outputElement;
	
	evalContext -> bind (innerElement.tuple, INNER_ROLE);

	// Insert the tuple into the inner synopsis
	if ((rc = innerSynopsis -> insertTuple (innerElement.tuple)) != 0)
		return rc;
	
	// Scan of outer tuples that join with inner tuple
	if ((rc = outerSynopsis -> getScan (outerScanId, outerScan)) != 0)
		return rc;
	
	while (!outputQueue -> isFull() && outerScan -> getNext(outerTuple)) {
		
		// allocate space for the output tuple
		if ((rc = outStore -> newTuple (outputTuple)) != 0)
			return rc;
		
		// construct the output tuple
		evalContext -> bind (outerTuple, OUTER_ROLE);
		evalContext -> bind (outputTuple, OUTPUT_ROLE);		
		outputConstructor -> eval ();
		
		// We have to insert this tuple in joinSynopsis (if it exists) to
		// enable us to produce MINUS tuples (see processInnerMinus & 
		// processOuterMinus)
		
		if (joinSynopsis) {
			
			// Lineage for a tuple is the set of tuples that produced it. 
			lineage [0] = outerTuple;
			lineage [1] = innerElement.tuple;
			
			rc = joinSynopsis -> insertTuple (outputTuple, lineage);
			if (rc != 0)									  
				return rc;

			LOCK_OUT_TUPLE (outputTuple);
		}
		
		// construct output element
		outputElement.kind = E_PLUS;
		outputElement.tuple = outputTuple;
		outputElement.timestamp = innerElement.timestamp;

		outputQueue -> enqueue (outputElement);
		lastOutputTs = outputElement.timestamp;

#ifdef _MONITOR_
		logJoin ();
#endif
		
	}
	
	// We stalled: It is possible that outerScan is also over, in which
	// case we are strictly not stalled, but ignoring this case simplifies
	// code.  We will discover that we are not really stalled later ...
	if (outputQueue -> isFull()) {
		bStalled = true;
		stallType = INNER_PLUS;
		scanWhenStalled = outerScan;
		stallElement = innerElement;
	}
	
	else {
		if ((rc = outerSynopsis -> releaseScan (outerScanId, outerScan)) != 0)
			return rc;
	}
	
	return 0;
}

int BinaryJoin::processInnerMinus (Element innerElement)
{
	int rc;

	// Tuple iterator of the outer
	// Iterator that scans the outer
	TupleIterator *outerScan;
	
	// Tuple of outer that joins with innerElement.tuple
	Tuple outerTuple;
	
	// Joined + possibly projected output tuple
	Tuple outputTuple;
	
	// The output element
	Element outputElement;
	
	evalContext -> bind (innerElement.tuple, INNER_ROLE);
	
	// Delete tuple from the outer synopsis
	if ((rc = innerSynopsis -> deleteTuple (innerElement.tuple)) != 0)
		return rc;
	UNLOCK_INNER_TUPLE (innerElement.tuple);
	
	// Scan of outer tuples that join with inner tuple
	if ((rc = outerSynopsis -> getScan (outerScanId, outerScan)) != 0)
		return rc;
	
	while (!outputQueue -> isFull() && outerScan -> getNext(outerTuple)) {
		
		// If we are maintaining the joinSynopsis, we can use it to
		// directly retrieve the output tuple.  The output tuple would
		// have been created when the PLUS tuples were joined [[
		// Explanation ]]
		
		if (joinSynopsis) {
			
			lineage [0] = outerTuple;
			lineage [1] = innerElement.tuple;
			
			rc = joinSynopsis -> getTuple (lineage, outputTuple);
			if (rc != 0)										   
				return rc;

			rc = joinSynopsis -> deleteTuple (outputTuple);
			if (rc != 0) return rc;
		}

		// Otherwise  we  freshly create  a  new  tuple.   Note taht  this
		// violates  the   requirement  that   a  PLUS  element   and  its
		// corresponding MINUS element refer to the same data tuple - this
		// requirement is  needed only when  the tuples are  inserted into
		// synopses upstream.  So the fact that we are here means that
		// there are no synopses upstream for these tuples ...		
		
		else {
			
			// allocate space for the output tuple
			if ((rc = outStore -> newTuple (outputTuple)) != 0)
				return rc;
			
			// construct the output tuple
			evalContext -> bind (outerTuple, OUTER_ROLE);
			evalContext -> bind (outputTuple, OUTPUT_ROLE);		
			outputConstructor -> eval ();
		}
		
		// output element
		outputElement.kind = E_MINUS;
		outputElement.tuple = outputTuple;
		outputElement.timestamp = innerElement.timestamp;

		outputQueue -> enqueue (outputElement);
		lastOutputTs = outputElement.timestamp;		
	}
	
	// We stalled: It is possible that outerScan is also over, in which
	// case we are strictly not stalled, but ignoring this case simplifies
	// code.  We will discover that we are not really stalled later ...
	
	if (outputQueue -> isFull()) {
		bStalled = true;
		stallType = INNER_MINUS;
		scanWhenStalled = outerScan;
		stallElement = innerElement;
		LOCK_INNER_TUPLE (stallElement.tuple);
	}
	
	else {
		if ((rc = outerSynopsis -> releaseScan (outerScanId, outerScan)) != 0)
			return rc;
	}
	
	return 0;
}

/**
 * Clear a stall that occurred while processing an outer PLUS element. 
 */

int BinaryJoin::clearStallOuterPlus ()
{
	int rc;
	// Outer element when stalled
	Element outerElement;

	// Inner scan when stalled
	TupleIterator *innerScan;
	
	Tuple innerTuple;
	Tuple outputTuple;	
	Element outputElement;
	
	outerElement = stallElement;
	innerScan = scanWhenStalled;

	// Optimism
	bStalled = false;
	
	while (!outputQueue -> isFull() && innerScan -> getNext(innerTuple)) {
		
		// allocate space for the output tuple
		if ((rc = outStore -> newTuple (outputTuple)) != 0)
			return rc;
		
		// construct the output tuple
		evalContext -> bind (innerTuple, INNER_ROLE);
		evalContext -> bind (outputTuple, OUTPUT_ROLE);		
		outputConstructor -> eval ();
		
		// We have to insert this tuple in joinSynopsis (if it exists) to
		// enable us to produce MINUS tuples (see processOuterMinus & 
		// processInnerMinus)
		
		if (joinSynopsis) {
			
			// Lineage for a tuple is the set of tuples that produced it. 
			lineage [0] = outerElement.tuple;
			lineage [1] = innerTuple;
			
			rc = joinSynopsis -> insertTuple (outputTuple, lineage);
			if (rc != 0)									  
				return rc;

			LOCK_OUT_TUPLE (outputTuple);
		}
		
		// construct output element
		outputElement.kind = E_PLUS;
		outputElement.tuple = outputTuple;
		outputElement.timestamp = outerElement.timestamp;

		outputQueue -> enqueue (outputElement);
		lastOutputTs = outputElement.timestamp;

#ifdef _MONITOR_
		logJoin ();
#endif
		
	}
	
	// We stalled: It is possible that innerScan is also over, in which
	// case we are strictly not stalled, but ignoring this case simplifies
	// code.  We will discover that we are not really stalled later ...
	if (outputQueue -> isFull()) {
		bStalled = true;
		stallType = OUTER_PLUS;
		scanWhenStalled = innerScan;		
	}
	
	else {
		if ((rc = innerSynopsis -> releaseScan (innerScanId, innerScan)) != 0)
			return rc;
	}	
	
	return 0;
}

int BinaryJoin::clearStallOuterMinus ()
{
	int rc;
	Element outerElement;
	TupleIterator *innerScan;
	Tuple innerTuple;
	Tuple outputTuple;
	Element outputElement;
	
	outerElement = stallElement;
	innerScan = scanWhenStalled;

	bStalled = false;
	
	while (!outputQueue -> isFull() && innerScan -> getNext(innerTuple)) {
		
		// If we are maintaining the joinSynopsis, we can use it to
		// directly retrieve the output tuple.  The output tuple would
		// have been created when the PLUS tuples were joined [[
		// Explanation ]]
		
		if (joinSynopsis) {
			
			lineage [0] = outerElement.tuple;
			lineage [1] = innerTuple;
			
			rc = joinSynopsis -> getTuple (lineage, outputTuple);
			if (rc != 0)										   
				return rc;
			rc = joinSynopsis -> deleteTuple (outputTuple);
			if (rc != 0) return rc;
		}
		
		// Otherwise  we  freshly create  a  new  tuple.   Note taht  this
		// violates  the   requirement  that   a  PLUS  element   and  its
		// corresponding MINUS element refer to the same data tuple - this
		// requirement is  needed only when  the tuples are  inserted into
		// synopses upstream.  So the fact that we are here means that
		// there are no synopses upstream for these tuples ...		
		
		else {
			
			// allocate space for the output tuple
			if ((rc = outStore -> newTuple (outputTuple)) != 0)
				return rc;
			
			// construct the output tuple
			evalContext -> bind (innerTuple, INNER_ROLE);
			evalContext -> bind (outputTuple, OUTPUT_ROLE);		
			outputConstructor -> eval ();
		}
		
		// output element
		outputElement.kind = E_MINUS;
		outputElement.tuple = outputTuple;
		outputElement.timestamp = outerElement.timestamp;

		outputQueue -> enqueue (outputElement);
		lastOutputTs = outputElement.timestamp;		
	}
	
	// We stalled: It is possible that innerScan is also over, in which
	// case we are strictly not stalled, but ignoring this case simplifies
	// code.  We will discover that we are not really stalled later ...
	
	if (outputQueue -> isFull()) {
		bStalled = true;
		stallType = OUTER_MINUS;
		scanWhenStalled = innerScan;
	}
	
	else {
		UNLOCK_OUTER_TUPLE(stallElement.tuple);
		if ((rc = innerSynopsis -> releaseScan (innerScanId, innerScan)) != 0)
			return rc;
	}
	
	return 0;	
}	

int BinaryJoin::clearStallInnerPlus ()
{
	int rc;
	Element innerElement;
	TupleIterator *outerScan;
	Tuple outerTuple;
	Tuple outputTuple;
	Element outputElement;

	innerElement = stallElement;
	outerScan = scanWhenStalled;

	bStalled = false;
	
	while (!outputQueue -> isFull() && outerScan -> getNext(outerTuple)) {
		
		// allocate space for the output tuple
		if ((rc = outStore -> newTuple (outputTuple)) != 0)
			return rc;
		
		// construct the output tuple
		evalContext -> bind (outerTuple, OUTER_ROLE);
		evalContext -> bind (outputTuple, OUTPUT_ROLE);		
		outputConstructor -> eval ();
		
		// We have to insert this tuple in joinSynopsis (if it exists) to
		// enable us to produce MINUS tuples (see processInnerMinus & 
		// processOuterMinus)
		
		if (joinSynopsis) {
			
			// Lineage for a tuple is the set of tuples that produced it. 
			lineage [0] = outerTuple;
			lineage [1] = innerElement.tuple;
			
			rc = joinSynopsis -> insertTuple (outputTuple, lineage);
			if (rc != 0)									  
				return rc;

			LOCK_OUT_TUPLE (outputTuple);
		}
		
		// construct output element
		outputElement.kind = E_PLUS;
		outputElement.tuple = outputTuple;
		outputElement.timestamp = innerElement.timestamp;

		outputQueue -> enqueue (outputElement);
		lastOutputTs = outputElement.timestamp;

#ifdef _MONITOR_
		logJoin ();
#endif
		
	}
	
	// We stalled: It is possible that outerScan is also over, in which
	// case we are strictly not stalled, but ignoring this case simplifies
	// code.  We will discover that we are not really stalled later ...
	if (outputQueue -> isFull()) {
		bStalled = true;
		stallType = INNER_PLUS;
		scanWhenStalled = outerScan;		
	}
	
	else {
		if ((rc = outerSynopsis -> releaseScan (outerScanId, outerScan)) != 0)
			return rc;
	}
	
	return 0;
}

int BinaryJoin::clearStallInnerMinus ()
{
	int rc;
	Element innerElement;
	TupleIterator *outerScan;
	Tuple outerTuple;
	Tuple outputTuple;
	Element outputElement;
	
	innerElement = stallElement;
	outerScan = scanWhenStalled;

	bStalled = false;
	
	while (!outputQueue -> isFull() && outerScan -> getNext(outerTuple)) {
		
		// If we are maintaining the joinSynopsis, we can use it to
		// directly retrieve the output tuple.  The output tuple would
		// have been created when the PLUS tuples were joined [[
		// Explanation ]]
		
		if (joinSynopsis) {
			
			lineage [0] = outerTuple;
			lineage [1] = innerElement.tuple;
			
			rc = joinSynopsis -> getTuple (lineage, outputTuple);
			if (rc != 0)										   
				return rc;
			rc = joinSynopsis -> deleteTuple (outputTuple);
			if (rc != 0) return rc;
		}

		// Otherwise  we  freshly create  a  new  tuple.   Note taht  this
		// violates  the   requirement  that   a  PLUS  element   and  its
		// corresponding MINUS element refer to the same data tuple - this
		// requirement is  needed only when  the tuples are  inserted into
		// synopses upstream.  So the fact that we are here means that
		// there are no synopses upstream for these tuples ...		
		
		else {
			
			// allocate space for the output tuple
			if ((rc = outStore -> newTuple (outputTuple)) != 0)
				return rc;
			
			// construct the output tuple
			evalContext -> bind (outerTuple, OUTER_ROLE);
			evalContext -> bind (outputTuple, OUTPUT_ROLE);		
			outputConstructor -> eval ();
		}
		
		// output element
		outputElement.kind = E_MINUS;
		outputElement.tuple = outputTuple;
		outputElement.timestamp = innerElement.timestamp;

		outputQueue -> enqueue (outputElement);
		lastOutputTs = outputElement.timestamp;		
	}
	
	// We stalled: It is possible that outerScan is also over, in which
	// case we are strictly not stalled, but ignoring this case simplifies
	// code.  We will discover that we are not really stalled later ...
	
	if (outputQueue -> isFull()) {
		bStalled = true;
		stallType = INNER_MINUS;
		scanWhenStalled = outerScan;
	}
	
	else {
		UNLOCK_INNER_TUPLE(stallElement.tuple);
		if ((rc = outerSynopsis -> releaseScan (outerScanId, outerScan)) != 0)
			return rc;		
	}
	
	return 0;
}

int BinaryJoin::clearStall()
{
	if (stallType == OUTER_PLUS)
		return clearStallOuterPlus ();
	
	if (stallType == OUTER_MINUS)
		return clearStallOuterMinus ();

	if (stallType == INNER_PLUS)
		return clearStallInnerPlus ();

	if (stallType == INNER_MINUS)
		return clearStallInnerMinus ();

	// should never reach
	ASSERT (0);

	return -1;
}
