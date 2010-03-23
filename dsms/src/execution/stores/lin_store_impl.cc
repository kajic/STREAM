#ifndef _LIN_STORE_IMPL_
#include "execution/stores/lin_store_impl.h"
#endif

using namespace Execution;
using namespace std;

#define USAGE(t) ((int *)(t))[usageCol]

#define NEXT(t)  ((char **)(t))[nextCol]

#define PREV(t)  ((char **)(t))[prevCol]

#define REF_COUNT(t) (((int *)(t))[refCountCol])

#define MARK_INSERT(t,s) (USAGE(t) &= (~(0x00000001<< (s))))

#define MARK_DELETE(t,s) (USAGE(t) &= (~(0x00010000<< (s))))

#define UNUSED(t) (USAGE(t) == 0)

#define ID(t) ((memMgr -> getId (t)))

LinStoreImpl::LinStoreImpl(unsigned int _id,
						   ostream& _LOG)
	: LOG (_LOG)
{
	this -> id = _id;
	
	this -> memMgr = 0;
	this -> numStubs = 0;
	this -> tuples = 0;
	this -> freeTuples = 0;
	this -> numLins = 0;
	this -> linIndex = 0;
}

LinStoreImpl::~LinStoreImpl() {}

int LinStoreImpl::setMemoryManager(MemoryManager *memMgr)
{
	ASSERT (memMgr);
	
	this -> memMgr   = memMgr;
	this -> pageSize = memMgr -> getPageSize();
	
	return 0;
}

int LinStoreImpl::setTupleLen(unsigned int tupleLen)
{
	ASSERT (tupleLen > 0);
	
	this -> tupleLen = tupleLen;
	
	return 0;
}

int LinStoreImpl::setNextCol (Column nextCol)
{
	this -> nextCol = nextCol;
	return 0;
}

int LinStoreImpl::setPrevCol (Column prevCol)
{
	this -> prevCol = prevCol;
	return 0;
}

int LinStoreImpl::setUsageCol (Column usageCol)
{
	this -> usageCol = usageCol;
	return 0;
}

int LinStoreImpl::setRefCountCol (Column refCountCol)
{
	this -> refCountCol = refCountCol;
	return 0;
}

int LinStoreImpl::setNumStubs (unsigned int numStubs)
{
	this -> numStubs = numStubs;
	return 0;
}

int LinStoreImpl::setIndex (Index *index)
{
	ASSERT (index);
	
	this -> linIndex = index;
	return 0;
}

int LinStoreImpl::addLineage (Column col)
{
	ASSERT (numLins < MAX_LINEAGE);
	
	linCols [numLins++] = col;
	return 0;
}

int LinStoreImpl::setLineageTuple (Tuple tuple)
{
	ASSERT (tuple);
	
	this -> linTuple = tuple;
	return 0;
}

int LinStoreImpl::setEvalContext (EvalContext *evalContext)
{
	ASSERT (evalContext);
	
	this -> evalContext = evalContext;
	return 0;
}

int LinStoreImpl::initialize()
{
	unsigned int mask, stubPattern;
	ASSERT (memMgr);
	
	numTuplesPerPage = pageSize / tupleLen;
	ASSERT (numTuplesPerPage > 0);
	
	tuples     = 0;
	freeTuples = 0;

	for (unsigned int s = 0 ; s < numStubs ; s++) {			
		mask        = (0x00010001 << s);
		stubPattern = (0x00010000 << s);
		
		iters [s] = new LinStoreIterator (usageCol, nextCol, mask,
										  stubPattern);		
	}
	
	return 0;
}

int LinStoreImpl::allocateMoreSpace ()
{
	int   rc;
	char *newPage, *tuple;

	ASSERT (!freeTuples);
	
	// get a page from memory manager
	if ((rc = memMgr -> allocatePage (newPage)) != 0)
		return rc;
#ifdef _MONITOR_
	logPageAlloc ();
#endif	
	
	
	// add all the tuple locations in this page to the free list
	freeTuples = tuple = newPage;
	for (unsigned int t = 0 ; t < numTuplesPerPage - 1 ; t++) {
		NEXT (tuple) = tuple + tupleLen;
		tuple += tupleLen;
	}
	
	NEXT (tuple) = 0;
	return 0;
}

int LinStoreImpl::newTuple (Tuple& tuple)
{
	int rc;
	unsigned int newTupleUsage;
	
	// I don't have free tuples, and can't allocate more
	if (!freeTuples && ((rc = allocateMoreSpace()) != 0))
		return rc;	
	ASSERT (freeTuples);
	
	// Allocate
	tuple = freeTuples;
	freeTuples = NEXT(freeTuples);
	
	// Initialize usage
	newTupleUsage = 0xffffffff;
	newTupleUsage <<= numStubs;
	newTupleUsage = ~newTupleUsage;
	newTupleUsage |= (newTupleUsage << 16);
	
	USAGE(tuple) = newTupleUsage;

	REF_COUNT (tuple) = 1;
	
	// Add to the beginning of the linked list
	NEXT(tuple) = tuples;
	if (tuples) {
		ASSERT (!PREV(tuples));
		PREV(tuples) = tuple;
	}
	tuples = tuple;
	PREV(tuples) = 0;
	
	return 0;
}

int LinStoreImpl::addRef (Tuple tuple)
{	
	ASSERT (REF_COUNT (tuple) > 0);
	REF_COUNT (tuple) ++;
	
	return 0;
}

int LinStoreImpl::addRef (Tuple tuple, unsigned int ref)
{	
	ASSERT (REF_COUNT (tuple) > 0);
	REF_COUNT (tuple) += ref;
	
	return 0;
}

int LinStoreImpl::decrRef (Tuple tuple)
{
	ASSERT (REF_COUNT (tuple) > 0);
	
	if (--REF_COUNT (tuple) == 0) {
		ASSERT (UNUSED(tuple));
		ASSERT (!NEXT(tuple));
		ASSERT (!PREV(tuple));
		
		NEXT (tuple) = freeTuples;
		freeTuples = tuple;
	}
	
	return 0;
}

int LinStoreImpl::insertTuple_r (Tuple tuple, unsigned int stubId)
{
	MARK_INSERT (tuple, stubId);
	
	return 0;
}

int LinStoreImpl::insertTuple_l (Tuple tuple, Tuple *lineage,
								 unsigned int stubId)
{
	ASSERT (lineage);
	
	MARK_INSERT (tuple, stubId);
	
	// Store the lineage
	for (unsigned int l = 0 ; l < numLins ; l++) {
		ASSERT (lineage [l]);
		ICOL (tuple, linCols [l]) = ID(lineage[l]);
	}
	
	return linIndex -> insertTuple (tuple);
}

int LinStoreImpl::deleteTuple_r (Tuple tuple, unsigned int stubId)
{
	MARK_DELETE (tuple, stubId);
	
	if (UNUSED(tuple)) {
		if (NEXT(tuple)) {
			PREV(NEXT(tuple)) = PREV(tuple);
		}
		
		if (PREV(tuple)) {
			NEXT(PREV(tuple)) = NEXT(tuple);
		}
		else {
			// Head of the linked list
			ASSERT (tuples == tuple);
			tuples = NEXT(tuple);
		}
#ifdef _DM_
		NEXT(tuple) = 0;
		PREV(tuple) = 0;
#endif
	}
	
	return 0;
}

int LinStoreImpl::deleteTuple_l (Tuple tuple, unsigned int stubId)
{
	int rc;
	MARK_DELETE (tuple, stubId);

	if ((rc = linIndex -> deleteTuple (tuple)) != 0)
		return rc;
	
	if (UNUSED(tuple)) {
		if (NEXT(tuple)) {
			PREV(NEXT(tuple)) = PREV(tuple);
		}
		
		if (PREV(tuple)) {
			NEXT(PREV(tuple)) = NEXT(tuple);
		}
		else {
			// Head of the linked list
			ASSERT (tuples == tuple);
			tuples = NEXT(tuple);
		}
#ifdef _DM_
		NEXT(tuple) = 0;
		PREV(tuple) = 0;
#endif		
	}
	
	return 0;
}

int LinStoreImpl::getScan_r (TupleIterator *&iter, unsigned int stubId)
{
	int rc;
	
	if ((rc = iters [stubId] -> initialize (tuples)) != 0)
		return rc;
	
	iter = iters [stubId];
	return 0;
}

int LinStoreImpl::releaseScan_r (TupleIterator *iter, unsigned int stubId)
{
	return 0;
}

int LinStoreImpl::getTuple_l (Tuple *lineage, Tuple &tuple,
							  unsigned int stubId)
{
	int rc;
	TupleIterator *scan;
	
	ASSERT (lineage);
	
	for (unsigned int l = 0 ; l < numLins ; l++) {
		ASSERT (lineage [l]);
		
		ICOL (linTuple, linCols [l]) = ID(lineage[l]);
	}
	
	// Assert: linTuple already bound to evalContext
	
	// Scan that returns the tuple with the given lineage
	if ((rc = linIndex -> getScan (scan)) != 0)
		return rc;
	
	if (!scan -> getNext (tuple)) {
		linIndex -> releaseScan (scan);
		return -1;
	}
	
#ifdef _DM_
	Tuple dummy;
	/// debug
	ASSERT(!scan -> getNext (dummy));
#endif

	if ((rc = linIndex -> releaseScan (scan)) != 0)
		return rc;	
	
	return 0;
}

LinStoreIterator::LinStoreIterator(Column       _usageCol,
								   Column       _nextCol,
								   unsigned int _mask,
								   unsigned int _stubPattern)
{
	this -> usageCol    = _usageCol;
	this -> nextCol     = _nextCol;
	this -> mask        = _mask;
	this -> stubPattern = _stubPattern;	
}

LinStoreIterator::~LinStoreIterator() {}

int LinStoreIterator::initialize (char *_tuples)
{
	this -> nextTuple   = _tuples;
	
	// First valid tuple.
	while (nextTuple && ((USAGE(nextTuple) & mask) != stubPattern))
		nextTuple = NEXT(nextTuple);

	return 0;
}
	


bool LinStoreIterator::getNext (Tuple& tuple)
{
	if (!nextTuple)
		return false;
	
	tuple = nextTuple;
	
	nextTuple = NEXT(nextTuple);
	while (nextTuple && ((USAGE(nextTuple) & mask) != stubPattern))
		nextTuple = NEXT(nextTuple);
	
	return true;
}
