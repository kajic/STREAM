#ifndef _DEBUG_
#include "common/debug.h"
#endif

#ifndef _REL_STORE_IMPL_
#include "execution/stores/rel_store_impl.h"
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


RelStoreImpl::RelStoreImpl(unsigned int _id,
						   ostream& _LOG)
	: LOG (_LOG)
{
	this -> id              = _id;
	
	this -> memMgr          = 0;
	this -> numStubs        = 0;
	this -> tuples          = 0;
	this -> freeTuples      = 0;

	for (unsigned int s = 0 ; s < MAX_STUBS ; s++)
		iters [s] = 0;	
}

RelStoreImpl::~RelStoreImpl()
{
	for (unsigned int s = 0 ; s < MAX_STUBS ; s++)
		if (iters [s])
			delete iters [s];
}

int RelStoreImpl::setMemoryManager(MemoryManager *memMgr)
{
	ASSERT (memMgr);
	
	this -> memMgr   = memMgr;
	this -> pageSize = memMgr -> getPageSize();
	
	return 0;
}

int RelStoreImpl::setTupleLen(unsigned int tupleLen)
{
	ASSERT (tupleLen > 0);
	
	this -> tupleLen = tupleLen;

	return 0;
}

int RelStoreImpl::setNextCol (Column nextCol)
{
	this -> nextCol = nextCol;
	return 0;
}

int RelStoreImpl::setPrevCol (Column prevCol)
{
	this -> prevCol = prevCol;
	return 0;
}

int RelStoreImpl::setUsageCol (Column usageCol)
{
	this -> usageCol = usageCol;
	
	return 0;
}

int RelStoreImpl::setRefCountCol (Column refCountCol)
{
	this -> refCountCol = refCountCol;
	return 0;
}

int RelStoreImpl::setNumStubs (unsigned int numStubs)
{
	this -> numStubs = numStubs;
	return 0;
}

int RelStoreImpl::initialize()
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
		
		iters [s] = new RelnStoreIterator (usageCol, nextCol,
										   mask, stubPattern);
	}
	
	return 0;
}

int RelStoreImpl::allocateMoreSpace ()
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

int RelStoreImpl::newTuple (Tuple& tuple)
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

int RelStoreImpl::addRef (Tuple tuple)
{	
	ASSERT (REF_COUNT (tuple) > 0);
	REF_COUNT (tuple) ++;
	
	return 0;
}

int RelStoreImpl::addRef (Tuple tuple, unsigned int ref)
{	
	ASSERT (REF_COUNT (tuple) > 0);
	REF_COUNT (tuple) += ref;
	
	return 0;
}

int RelStoreImpl::decrRef (Tuple tuple)
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

int RelStoreImpl::insertTuple_r (Tuple tuple, unsigned int stubId)
{
	MARK_INSERT (tuple, stubId);
	
	return 0;
}

int RelStoreImpl::deleteTuple_r (Tuple tuple, unsigned int stubId)
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

int RelStoreImpl::getScan_r (TupleIterator *&iter, unsigned int stubId)
{
	int rc;
	ASSERT (stubId < numStubs);
	
	if ((rc = iters [stubId] -> initialize (tuples)) != 0)
		return rc;	
	iter = iters [stubId];
	
	return 0;
}

int RelStoreImpl::releaseScan_r (TupleIterator *iter, unsigned int stubId)
{
	return 0;
}

RelnStoreIterator::RelnStoreIterator(Column       _usageCol,
									 Column       _nextCol,
									 unsigned int _mask,
									 unsigned int _stubPattern)									 
{
	this -> usageCol    = _usageCol;
	this -> nextCol     = _nextCol;
	this -> mask        = _mask;
	this -> stubPattern = _stubPattern;	
}

int RelnStoreIterator::initialize (char *_tuples)
{
	this -> nextTuple   = _tuples;
	
	// First valid tuple.
	while (nextTuple && ((USAGE(nextTuple) & mask) != stubPattern))
		nextTuple = NEXT(nextTuple);
	
	return 0;
}

RelnStoreIterator::~RelnStoreIterator() {}

bool RelnStoreIterator::getNext (Tuple& tuple)
{
	if (!nextTuple)
		return false;
	
	tuple = nextTuple;
	
	nextTuple = NEXT(nextTuple);
	while (nextTuple && ((USAGE(nextTuple) & mask) != stubPattern))
		nextTuple = NEXT(nextTuple);
	
	return true;
}
