#ifndef _EXCEPT_
#include "execution/operators/except.h"
#endif

#ifndef _DEBUG_
#include "common/debug.h"
#endif

using namespace Execution;
using namespace std;

#define MIN(a,b) (((a) < (b))? (a) : (b))

#define UNLOCK_COUNT_TUPLE(t) (countStore -> decrRef((t)))
#define UNLOCK_LEFT_TUPLE(t) (leftInStore -> decrRef ((t)))
#define UNLOCK_RIGHT_TUPLE(t) (rightInStore -> decrRef ((t)))
#define LOCK_OUT_TUPLE(t) (outStore -> addRef ((t)))

Except::Except (unsigned int id, ostream& _LOG)
	: LOG (_LOG)
{
	this -> id             = id;
	this -> leftInQueue    = 0;
	this -> rightInQueue   = 0;
	this -> outputQueue    = 0;
	this -> outStore       = 0;
	this -> outSyn         = 0;
	this -> leftInStore    = 0;
	this -> rightInStore   = 0;
	this -> countSyn       = 0;
	this -> countStore     = 0;
	this -> evalContext    = 0;
	this -> outEval        = 0;
	this -> initEval       = 0;
	this -> cplsEval       = 0;
	this -> cprsEval       = 0;
	this -> lastLeftTs     = 0;
	this -> lastRightTs    = 0;
	this -> lastOutputTs   = 0;
}

Except::~Except () {}

int Except::setRightInputQueue (Queue *queue)
{
	ASSERT (queue);
	
	this -> rightInQueue = queue;
	return 0;
}

int Except::setLeftInputQueue (Queue *queue)
{
	ASSERT (queue);
	
	this -> leftInQueue = queue;
	return 0;
}

int Except::setOutputQueue (Queue *queue)
{
	ASSERT (queue);

	this -> outputQueue = queue;
	return 0;
}

int Except::setOutStore (StorageAlloc *store)
{
	ASSERT (store);

	this -> outStore = store;
	return 0;
}

int Except::setLeftInputStore (StorageAlloc *store)
{
	ASSERT (store);

	this -> leftInStore = store;
	return 0;
}

int Except::setRightInputStore (StorageAlloc *store)
{
	ASSERT (store);

	this -> rightInStore = store;
	return 0;
}

int Except::setEvalContext (EvalContext *evalContext)
{
	ASSERT (evalContext);

	this -> evalContext = evalContext;
	return 0;
}

int Except::setCountSyn (RelationSynopsis *syn)
{
	ASSERT (syn);
	
	this -> countSyn = syn;
	return 0;
}

int Except::setCountScanId (unsigned int scanId)
{
	this -> countScanId = scanId;
	return 0;
}

int Except::setCountStore (StorageAlloc *store)
{
	ASSERT (store);

	this -> countStore = store;
	return 0;
}

int Except::setOutSyn (RelationSynopsis *outSyn)
{
	ASSERT (outSyn);

	this -> outSyn = outSyn;
	return 0;
}

int Except::setOutScanId (unsigned int scanId)
{
	this -> outScanId = scanId;
	return 0;
}

int Except::setOutEval (AEval *eval)
{
	ASSERT (eval);

	this -> outEval = eval;
	return 0;
}

int Except::setInitEval (AEval *eval)
{
	ASSERT (eval);

	this -> initEval = eval;
	return 0;
}

int Except::setCopyLeftToScratchEval (AEval *eval)
{
	ASSERT (eval);

	this -> cplsEval = eval;
	return 0;
}

int Except::setCopyRightToScratchEval (AEval *eval)
{
	ASSERT (eval);
	
	this -> cprsEval = eval;
	return 0;
}

int Except::setCountCol (Column col)
{
	this -> countCol = col;
	return 0;
}

int Except::run (TimeSlice timeSlice)
{
	int          rc;
	unsigned int numElements;
	Timestamp    leftMinTs;
	Timestamp    rightMinTs;
	Element      inputElement;

#ifdef _MONITOR_
	startTimer ();
#endif
	
	// Minimum timestamp possible on the next outer element
	leftMinTs = lastLeftTs;
	
	// Minimum timestamp possible on the next inner element
	rightMinTs = lastRightTs;	
	
	numElements = timeSlice;
	
	for (unsigned int e = 0 ; e < numElements ; e++) {

		// Stalled @ output
		if (outputQueue -> isFull ())
			break;

		// Minimum possible timestamp for the next left element
		if (leftInQueue -> peek (inputElement))
			leftMinTs = inputElement.timestamp;
		
		// Minimum possible timestamp for the next outer element
		if (rightInQueue -> peek (inputElement))
			rightMinTs = inputElement.timestamp;
		
		// We have to process left input if leftMinTs < rightMinTs.  If
		// leftMinTs = rightMinTs, we have the option of doing so.
		if (leftMinTs <= rightMinTs &&
			leftInQueue -> dequeue (inputElement)) {
			
			lastLeftTs = inputElement.timestamp;
			
			// Process PLUS
			if (inputElement.kind == E_PLUS) {
				if ((rc = handleLeftPlus (inputElement)) != 0)
					return rc;
				
				UNLOCK_LEFT_TUPLE (inputElement.tuple);
			}
			
			// Process MINUS
			else if (inputElement.kind == E_MINUS) {
				if ((rc = handleLeftMinus (inputElement)) != 0)
					return rc;
				
				UNLOCK_LEFT_TUPLE (inputElement.tuple);			
			}
			
			// else: ignore heartbeats
		}
		
		// Symmetric argument for right input
		else if (rightMinTs <= leftMinTs &&
				 rightInQueue -> dequeue (inputElement)) {
			
			lastRightTs = inputElement.timestamp;
			
			if (inputElement.kind == E_PLUS) {
				if ((rc = handleRightPlus (inputElement)) != 0)
					return rc;
				
				UNLOCK_RIGHT_TUPLE (inputElement.tuple);
			}
			
			// Process MINUS
			else if (inputElement.kind == E_MINUS) {
				if ((rc = handleRightMinus (inputElement)) != 0)
					return rc;
				
				UNLOCK_RIGHT_TUPLE (inputElement.tuple);			
			}
			
			// else: ignore heartbeats
		}
		
		else {
			break;
		}
	}
	
	// Heartbeat generation
	if ((!outputQueue -> isFull())   &&
		(lastOutputTs < leftMinTs)  &&
		(lastOutputTs < rightMinTs)) {
		
		lastOutputTs = MIN(leftMinTs, rightMinTs);
		
		outputQueue -> enqueue (Element::Heartbeat(lastOutputTs));
	}

	
#ifdef _MONITOR_
	stopTimer ();
	logOutTs (lastOutputTs);
#endif	
	
	return 0;
}

int Except::handleLeftPlus (Element inputElement)
{
	int      rc;
	Tuple    inTuple;
	Tuple    countTuple;
	Tuple    outTuple;
	Element  outElement;
	int      count;
	
	// Input tuple
	inTuple = inputElement.tuple;

	// Get the count tuple for this input tuple
	if ((rc = getCountTuple_l (inTuple, countTuple)) != 0)
		return rc;
	
	// Increment the count
	count = ++ICOL(countTuple, countCol);
	
	// Count is positive: send out an output
	if (count > 0) {
		
		// alloc output tuple
		if ((rc = outStore -> newTuple (outTuple)) != 0)
			return rc;
		
		// copy attributes from count tuple -> outTuple
		evalContext -> bind (countTuple, COUNT_ROLE);
		evalContext -> bind (outTuple, OUTPUT_ROLE);
		outEval -> eval ();
		
		// Output element
		outElement.kind      = E_PLUS;
		outElement.tuple     = outTuple;
		outElement.timestamp = inputElement.timestamp;
		
		outputQueue -> enqueue (outElement);
		lastOutputTs = outElement.timestamp;
		
		// Insert the tuple into output synopsis
		if ((rc = outSyn -> insertTuple (outTuple)) != 0)
			return rc;
		
		LOCK_OUT_TUPLE (outTuple);
	}
	
	// Count is 0, remove the count tuple
	else if (count == 0) {
		
		if ((rc = countSyn -> deleteTuple (countTuple)) != 0)
			return rc;
		
		UNLOCK_COUNT_TUPLE (countTuple);
	}
	
	return 0;
}

int Except::handleLeftMinus (Element inputElement)
{
	int     rc;
	Tuple   inTuple;
	Tuple   countTuple;
	Tuple   outTuple;
	Element outElement;
	int     count;
	
	// Input tuple
	inTuple = inputElement.tuple;	

	// Get the count tuple for this tuple
	if ((rc = getCountTuple_l (inTuple, countTuple)) != 0)
		return rc;
	
	// Decrement the count
	count = --ICOL(countTuple, countCol);
	
	// If the count is nonnegative, we produce a minus element
	// corresponding to this tuple.   
	if (count >= 0) {
		
		// Assert: there exists at least one tuple in the output identical
		// to the input tuple
		if ((rc = getOutTuple (countTuple, outTuple)) != 0)
			return rc;
		
		outElement.kind      = E_MINUS;
		outElement.tuple     = outTuple;
		outElement.timestamp = inputElement.timestamp;
		
		outputQueue -> enqueue (outElement);
		lastOutputTs = outElement.timestamp;
		
		// Delete the tuple from the output synopsis
		if ((rc = outSyn -> deleteTuple (outTuple)) != 0)
			return rc;
	}
	
	if (count == 0) {
		
		// Delete the count tuple
		if ((rc = countSyn -> deleteTuple (countTuple)) != 0)
			return rc;
		
		UNLOCK_COUNT_TUPLE (countTuple);
	}
	
	return 0;
}

// Nearly identical to handleLeftMinus
int Except::handleRightMinus (Element inputElement)
{
	int      rc;
	Tuple    inTuple;
	Tuple    countTuple;
	Tuple    outTuple;
	Element  outElement;
	int      count;
	
	// Input tuple
	inTuple = inputElement.tuple;
	
	// Get the count tuple for this input tuple
	if ((rc = getCountTuple_r (inTuple, countTuple)) != 0)
		return rc;

	// Increment the count
	count = ++ICOL(countTuple, countCol);
	
	// Count is positive: send out an output
	if (count > 0) {
		
		// alloc output tuple
		if ((rc = outStore -> newTuple (outTuple)) != 0)
			return rc;
		
		// copy attributes from count tuple -> outTuple
		evalContext -> bind (countTuple, COUNT_ROLE);
		evalContext -> bind (outTuple, OUTPUT_ROLE);		
		outEval -> eval ();
		
		// Output element
		outElement.kind      = E_PLUS;
		outElement.tuple     = outTuple;
		outElement.timestamp = inputElement.timestamp;
		
		outputQueue -> enqueue (outElement);
		lastOutputTs = outElement.timestamp;
		
		// Insert the tuple into output synopsis
		if ((rc = outSyn -> insertTuple (outTuple)) != 0)
			return rc;
		
		LOCK_OUT_TUPLE (outTuple);
	}
	
	// Count is 0, remove the count tuple
	else if (count == 0) {
		
		if ((rc = countSyn -> deleteTuple (countTuple)) != 0)
			return rc;
		
		UNLOCK_COUNT_TUPLE (countTuple);
	}
	
	return 0;
}


int Except::handleRightPlus (Element inputElement)
{
	int     rc;
	Tuple   inTuple;
	Tuple   countTuple;
	Tuple   outTuple;
	Element outElement;
	int     count;
	
	// Input tuple
	inTuple = inputElement.tuple;	
	
	// Get the count tuple for this tuple
	if ((rc = getCountTuple_r (inTuple, countTuple)) != 0)
		return rc;

	// Decrement the count
	count = --ICOL(countTuple, countCol);
	
	// If the count is positive ...
	if (count >= 0) {

		// Assert: there exists at least one tuple in the output identical
		// to the input tuple
		if ((rc = getOutTuple (countTuple, outTuple)) != 0)
			return rc;
		
		outElement.kind      = E_MINUS;
		outElement.tuple     = outTuple;
		outElement.timestamp = inputElement.timestamp;
		
		outputQueue -> enqueue (outElement);
		lastOutputTs = outElement.timestamp;
		
		// Delete the tuple from the output synopsis
		if ((rc = outSyn -> deleteTuple (outTuple)) != 0)
			return rc;
	}
	
	if (count == 0) {
		
		// Delete the count tuple
		if ((rc = countSyn -> deleteTuple (countTuple)) != 0)
			return rc;
		
		UNLOCK_COUNT_TUPLE (countTuple);
	}
	
	return 0;
}

int Except::getCountTuple_l (Tuple inTuple, Tuple &countTuple)
{
	int rc;
	TupleIterator *scan;
	
	evalContext -> bind (inTuple, INPUT_ROLE);		
	evalContext -> bind (inTuple, LEFT_ROLE);
	cplsEval -> eval ();
	
	// Get the count tuple for this input tuple if it exists
	if ((rc = countSyn -> getScan (countScanId, scan)) != 0)
		return rc;
	
	// The count tuple does not exist
	if (!scan -> getNext (countTuple)) {
		
		// Create a new count tuple
		if ((rc = countStore -> newTuple (countTuple)) != 0)
			return rc;
		
		// Copy the inTuple's attributes to countTuple
		evalContext -> bind (countTuple, COUNT_ROLE);
		initEval -> eval ();
		
		// Initialize count to 0
		ICOL(countTuple, countCol) = 0;
		
		// Insert the count tuple into count synopsis
		if ((rc = countSyn -> insertTuple (countTuple)) != 0)
			return rc;		
	}
	
#ifdef _DM_	
	else {
		// There cannot be more than one tuple that match
		Tuple dummy;
		ASSERT (!scan -> getNext (dummy));		
	}
#endif
	
	if ((rc = countSyn -> releaseScan (countScanId, scan)) != 0)
		return rc;
	
	return 0;
}

int Except::getCountTuple_r (Tuple inTuple, Tuple &countTuple)
{
	int rc;
	TupleIterator *scan;
	
	evalContext -> bind (inTuple, INPUT_ROLE);
	evalContext -> bind (inTuple, RIGHT_ROLE);
	cprsEval -> eval ();
	
	// Get the count tuple for this input tuple if it exists
	if ((rc = countSyn -> getScan (countScanId, scan)) != 0)
		return rc;
	
	// The count tuple does not exist
	if (!scan -> getNext (countTuple)) {
		
		// Create a new count tuple
		if ((rc = countStore -> newTuple (countTuple)) != 0)
			return rc;

		// Copy the inTuple's attributes to countTuple
		evalContext -> bind (countTuple, COUNT_ROLE);
		initEval -> eval ();
		
		// Initialize count to 0
		ICOL(countTuple, countCol) = 0;
		
		// Insert the count tuple into count synopsis
		if ((rc = countSyn -> insertTuple (countTuple)) != 0)
			return rc;		
	}
	
#ifdef _DM_	
	else {
		// There cannot be more than one tuple that match
		Tuple dummy;
		ASSERT (!scan -> getNext (dummy));		
	}
#endif
	
	if ((rc = countSyn -> releaseScan (countScanId, scan)) != 0)
		return rc;
	
	return 0;
}


int Except::getOutTuple (Tuple countTuple, Tuple &outTuple)
{
	int rc;
	TupleIterator *scan;
	bool bFound;
	
	evalContext -> bind (countTuple, COUNT_ROLE);	
	
	// Get an output tuple identical to the input tuple
	if ((rc = outSyn -> getScan (outScanId, scan)) != 0)
		return rc;
	
	bFound = scan -> getNext (outTuple);
	ASSERT (bFound);
	
	if ((rc = outSyn -> releaseScan (outScanId, scan)) != 0)
		return rc;
	
	return 0;
}

