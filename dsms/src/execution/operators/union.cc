#ifndef _UNION_
#include "execution/operators/union.h"
#endif

#ifndef _DEBUG_
#include "common/debug.h"
#endif

#define MIN(a,b) (((a) < (b))? (a) : (b))
#define LOCK_OUT_TUPLE(t) (outStore -> addRef ((t)))
#define UNLOCK_LEFT_TUPLE(t) (leftInStore -> decrRef ((t)))
#define UNLOCK_RIGHT_TUPLE(t) (rightInStore -> decrRef ((t)))
using namespace Execution;
using namespace std;

Union::Union (unsigned int id, ostream &_LOG)
	: LOG (_LOG)
{
	this -> id            = id;
	this -> leftInQueue   = 0;
	this -> rightInQueue  = 0;
	this -> outputQueue   = 0;
	this -> outStore      = 0;
	this -> outSyn        = 0;
	this -> leftInStore   = 0;
	this -> rightInStore  = 0;
	this -> evalContext   = 0;
	this -> leftOutEval   = 0;
	this -> rightOutEval  = 0;
	this -> lastLeftTs    = 0;
	this -> lastRightTs   = 0;
	this -> lastOutputTs  = 0;
}

Union::~Union () {}

int Union::setRightInputQueue (Queue *queue)
{
	ASSERT (queue);
	
	this -> rightInQueue = queue;
	return 0;
}

int Union::setLeftInputQueue (Queue *queue)
{
	ASSERT (queue);
	
	this -> leftInQueue = queue;
	return 0;
}

int Union::setOutputQueue (Queue *queue)
{
	ASSERT (queue);

	this -> outputQueue = queue;
	return 0;
}

int Union::setOutStore (StorageAlloc *store)
{
	ASSERT (store);
	
	this -> outStore = store;
	return 0;
}

int Union::setLeftInputStore (StorageAlloc *store)
{
	ASSERT (store);

	this -> leftInStore = store;
	return 0;
}

int Union::setRightInputStore (StorageAlloc *store)
{
	ASSERT (store);

	this -> rightInStore = store;
	return 0;
}

int Union::setEvalContext (EvalContext *evalContext)
{
	ASSERT (evalContext);

	this -> evalContext = evalContext;
	return 0;
}

int Union::setLeftOutEval (AEval *eval)
{
	ASSERT (eval);

	this -> leftOutEval = eval;
	return 0;
}

int Union::setRightOutEval (AEval *eval)
{
	ASSERT (eval);
	
	this -> rightOutEval = eval;
	return 0;
}

int Union::setOutSyn (LineageSynopsis *syn)
{
	this -> outSyn = syn;
	return 0;
}

int Union::run (TimeSlice timeSlice)
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
		
		// We have to process the left input
		if (leftMinTs <= rightMinTs &&
			leftInQueue -> dequeue (inputElement)) {
			
			lastLeftTs = inputElement.timestamp;
			
			if (inputElement.kind == E_PLUS) {
				if ((rc = handleLeftPlus (inputElement)) != 0) {
					return rc;
				}
				UNLOCK_LEFT_TUPLE (inputElement.tuple);							
			}
			
			// Process minus
			else if (inputElement.kind == E_MINUS) {
				if ((rc = handleLeftMinus (inputElement)) != 0) {
					return rc;
				}
				UNLOCK_LEFT_TUPLE (inputElement.tuple);
			}
			// else: ignore heartbeats
			

		}
		
		// We have to process the right input
		else if (rightMinTs <= leftMinTs &&
				 rightInQueue -> dequeue (inputElement)) {
			
			lastRightTs = inputElement.timestamp;

			// Process plus
			if (inputElement.kind == E_PLUS) {
				if ((rc = handleRightPlus (inputElement)) != 0) {
					return rc;
				}
				UNLOCK_RIGHT_TUPLE (inputElement.tuple);				
			}
		
			// Process minus
			else if (inputElement.kind == E_MINUS) {
				if ((rc = handleRightMinus (inputElement)) != 0) {
					return rc;
				}
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

int Union::handleLeftPlus (Element inputElement)
{
	int      rc;
	Tuple    inTuple;
	Tuple    outTuple;
	Element  outElement;
	Tuple   *lineage;
	
	inTuple = inputElement.tuple;
	
	// Alloc output tuple
	if ((rc = outStore -> newTuple (outTuple)) != 0)
		return rc;
	
	// Copy input tuple data to output tuple
	evalContext -> bind (outTuple, OUTPUT_ROLE);
	evalContext -> bind (inTuple, LEFT_ROLE);
	leftOutEval -> eval();
	
	// Output element
	outElement.kind      = E_PLUS;
	outElement.tuple     = outTuple;
	outElement.timestamp = inputElement.timestamp;
	
	outputQueue -> enqueue (outElement);
	
	if (outSyn) {
		
		lineage = &inTuple;		
		if ((rc = outSyn -> insertTuple (outTuple, lineage)) != 0)
			return rc;
		
		LOCK_OUT_TUPLE (outTuple);
	}
	
	return 0;
}

int Union::handleRightPlus (Element inputElement)
{
	int      rc;
	Tuple    inTuple;
	Tuple    outTuple;
	Element  outElement;
	Tuple   *lineage;
	
	inTuple = inputElement.tuple;
	
	// Alloc output tuple
	if ((rc = outStore -> newTuple (outTuple)) != 0)
		return rc;
	
	// Copy input tuple data to output tuple
	evalContext -> bind (outTuple, OUTPUT_ROLE);
	evalContext -> bind (inTuple, RIGHT_ROLE);
	rightOutEval -> eval();
	
	// Output element
	outElement.kind      = E_PLUS;
	outElement.tuple     = outTuple;
	outElement.timestamp = inputElement.timestamp;

	outputQueue -> enqueue (outElement);
	
	if (outSyn) {
		
		lineage = &inTuple;		
		if ((rc = outSyn -> insertTuple (outTuple, lineage)) != 0)
			return rc;
		
		LOCK_OUT_TUPLE (outTuple);
	}
	
	return 0;
}

int Union::handleLeftMinus (Element inputElement)
{
	int       rc;
	Tuple     inTuple;
	Tuple     outTuple;
	Element   outElement;
	Tuple    *lineage;
	
	inTuple = inputElement.tuple;
	
	// Get the plus tuple that we produced corresponding to +(inTuple)
	lineage = &inTuple;
	
	if ((rc = outSyn -> getTuple (lineage, outTuple)) != 0)
		return rc;
	
	// Output element
	outElement.kind      = E_MINUS;
	outElement.tuple     = outTuple;
	outElement.timestamp = inputElement.timestamp;
	
	outputQueue -> enqueue (outElement);
	
	if ((rc = outSyn -> deleteTuple (outTuple)) != 0)
		return rc;
	
	return 0;
}
	
int Union::handleRightMinus (Element inputElement)
{
	int       rc;
	Tuple     inTuple;
	Tuple     outTuple;
	Element   outElement;
	Tuple    *lineage;
	
	inTuple = inputElement.tuple;
	
	// Get the plus tuple that we produced corresponding to +(inTuple)
	lineage = &inTuple;
	
	if ((rc = outSyn -> getTuple (lineage, outTuple)) != 0)
		return rc;
	
	// Output element
	outElement.kind      = E_MINUS;
	outElement.tuple     = outTuple;
	outElement.timestamp = inputElement.timestamp;
	
	outputQueue -> enqueue (outElement);
	
	if ((rc = outSyn -> deleteTuple (outTuple)) != 0)
		return rc;
	
	return 0;
}
	
