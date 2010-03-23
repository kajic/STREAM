#ifndef _DEBUG_
#include "common/debug.h"
#endif

#ifndef _WIN_STORE_IMPL_
#include "execution/stores/win_store_impl.h"
#endif

#define LOCK_TUPLE(t) (memMgr -> incrRefCount ((t)))
#define UNLOCK_TUPLE(t) (memMgr -> decrRefCount ((t)))

using namespace Execution;
using namespace std;

WinStoreImpl::WinStoreImpl (unsigned int _id,
						  ostream&     _LOG)
	: LOG (_LOG)
{
	this -> id                   = _id;
	this -> memMgr               = 0;
	this -> numStubs             = 0;
	this -> tupleLen             = 0;
	this -> pageSize             = 0;
	this -> numTuplesPerPage     = 0;
	this -> nextPagePtrOffset    = 0;
	this -> nextPagePtrOffset_lt = 0;

	for (unsigned int s = 0 ; s < MAX_NUM_STUBS ; s++)
		iters [s] = 0;
}

WinStoreImpl::~WinStoreImpl() {
	for (unsigned int s = 0 ; s < MAX_NUM_STUBS ; s++)
		if (iters [s])
			delete iters [s];
}

int WinStoreImpl::setMemoryManager(MemoryManager *memMgr)
{
	ASSERT (memMgr);
	
	this -> memMgr   = memMgr;
	return 0;
}

int WinStoreImpl::setNumStubs (unsigned int numStubs)
{
	this -> numStubs = numStubs;
	return 0;
}

int WinStoreImpl::setTimestampCol (Column tsCol)
{
	this -> tsCol = tsCol;
	return 0;
}

int WinStoreImpl::setTupleLen (unsigned int tupleLen)
{
	this -> tupleLen = tupleLen;
	return 0;
}

int WinStoreImpl::computeDataLayout ()
{
	unsigned int lastTupleOffset;
	
	pageSize = memMgr -> getPageSize();
	
	// We will store the next pointer in the page in the last possible
	// position given the alignment requirement.
	ASSERT (pageSize > sizeof(char *));	
	nextPagePtrOffset = pageSize - sizeof (char *);
	nextPagePtrOffset -= (nextPagePtrOffset %  __alignof__ (char *));
	
	// We want to layout the tuples only at multiples of tupleLen from the
	// beginning of the page (to play safe with alignment related issues)	
	firstTupleOffset = 0;
	for (unsigned int t = 0 ; t < pageSize &&
			 firstTupleOffset < INT_SIZE ; t++) {
		firstTupleOffset += tupleLen;
	}
	
	if (firstTupleOffset >= nextPagePtrOffset)
		return -1;
	
	numTuplesPerPage = (nextPagePtrOffset - firstTupleOffset) / tupleLen;
	if (numTuplesPerPage == 0)
		return -1;
	
	// Offset of the (beginning of) the last tuple
	lastTupleOffset = firstTupleOffset + tupleLen * (numTuplesPerPage - 1);
	
	// Offset of next page pointer from the last tuple
	nextPagePtrOffset_lt = nextPagePtrOffset - lastTupleOffset;
			
	return 0;
}

#define NEXT_PAGE(p) (*((char **)((p) + nextPagePtrOffset)))
#define NEXT_PAGE_LT(t) (*((char **)((t) + nextPagePtrOffset_lt)))
#define REF_COUNT(p) (*((int *)(p)))

int WinStoreImpl::initialize()
{
	int rc;
	TuplePtr dummyTuple;	

	ASSERT (memMgr);
	ASSERT (numStubs > 0);
	ASSERT (tupleLen > 0);
	
	// Determine how we layout data on a page
	if ((rc = computeDataLayout()) != 0)
		return rc;
	
	if ((rc = memMgr -> allocatePage (curPage)) != 0)
		return rc;
	
#ifdef _MONITOR_
	logPageAlloc ();
#endif
	
	NEXT_PAGE (curPage) = 0;
	REF_COUNT (curPage) = numStubs;
	
	// The first tuple in the page is used as a dummy tuple:
	dummyTuple.dataPtr = curPage + firstTupleOffset;
	dummyTuple.posInPage = 0;
	
	for (unsigned int s = 0 ; s < numStubs ; s++) {
		stubs [s].lastInsTuple = dummyTuple;
		stubs [s].lastDelTuple = dummyTuple;

		iters [s] = new WindowIterator (tupleLen,
										firstTupleOffset,
										numTuplesPerPage,
										nextPagePtrOffset_lt);
	}
	
	nextTuple = curPage + firstTupleOffset + tupleLen;
	numAllocInCurPage = 1;
	
	return 0;
}
 
int WinStoreImpl::newTuple(Tuple& tuple)
{
	int rc;
	char *newPage;

	ASSERT (nextTuple);
	tuple = nextTuple;
	
	REF_COUNT (curPage)++;
	
	if (++numAllocInCurPage == numTuplesPerPage) {
		
		if ((rc = memMgr -> allocatePage (newPage)) != 0)
			return rc;

#ifdef _MONITOR_
		logPageAlloc ();
#endif
		
		NEXT_PAGE (curPage) = newPage;
		curPage = newPage;

		REF_COUNT (curPage) = numStubs;
		NEXT_PAGE (curPage) = 0;
		
		numAllocInCurPage = 0;
		nextTuple = curPage + firstTupleOffset;
	}
	
	else {
		nextTuple += tupleLen;
	}
	
	return 0;
}

int WinStoreImpl::addRef (Tuple tuple)
{
	ASSERT (REF_COUNT (memMgr -> getPage (tuple)) > 0);
	REF_COUNT(memMgr -> getPage (tuple))++;
	
	return 0;
}

int WinStoreImpl::addRef (Tuple tuple, unsigned int ref)
{
	ASSERT (REF_COUNT (memMgr -> getPage (tuple)) > 0);
	REF_COUNT(memMgr -> getPage (tuple)) += ref;
	
	return 0;
}

int WinStoreImpl::decrRef (Tuple tuple)
{
	ASSERT (REF_COUNT (memMgr -> getPage (tuple)) > 0);
	
	if ( --(REF_COUNT (memMgr -> getPage (tuple))) == 0) {
		
#ifdef _MONITOR_
		logPageFree ();
#endif
		
		return memMgr -> deallocatePage (memMgr -> getPage (tuple));
	}
	
	return 0;
}

#define INCR(t)                                                     \
{                                                                   \
	if ((++((t).posInPage)) == numTuplesPerPage) {                  \
		(t).posInPage = 0;                                          \
        (t).dataPtr = NEXT_PAGE_LT((t).dataPtr) + firstTupleOffset; \
    }                                                               \
	else {                                                          \
		(t).dataPtr += tupleLen;                                    \
    }                                                               \
}                                                                   \

int WinStoreImpl::insertTuple_w (Tuple tuple, Timestamp timestamp,
								 unsigned int stubId)
{
	ASSERT (stubId < numStubs);
	
	// Move to the next position 
	INCR (stubs [stubId].lastInsTuple);
	
	ASSERT (stubs [stubId].lastInsTuple.dataPtr == tuple);
	
	// [[ Some of these copies will be redundant ]]
	((Timestamp *)tuple) [tsCol] = timestamp;
	
	return 0;
}

bool WinStoreImpl::isEmpty_w (unsigned int stubId) const {
	ASSERT (stubId < numStubs);
	
	return (stubs [stubId].lastDelTuple.dataPtr ==
			stubs [stubId].lastInsTuple.dataPtr);
}

int WinStoreImpl::getOldestTuple_w (Tuple&       tuple,
								   Timestamp&   timestamp,
								   unsigned int stubId) const
{
	TuplePtr tmpPtr;
	
	ASSERT (stubId < numStubs);
	ASSERT (stubs [stubId].lastDelTuple.dataPtr !=
			stubs [stubId].lastInsTuple.dataPtr);
	
	// Copy of the lastDelTuple
	tmpPtr = stubs [stubId].lastDelTuple;
	
	// Now we should be pointing to the last tuple
	INCR (tmpPtr);
	
	tuple = (tmpPtr.dataPtr);
	timestamp = ((Timestamp *)tmpPtr.dataPtr)[tsCol];
	
	return 0;
}

int WinStoreImpl::deleteOldestTuple_w (unsigned int stubId)
{
	int rc;
	ASSERT (stubId < numStubs);	
	char         *pagePtr;	
	
	if (++(stubs[stubId].lastDelTuple.posInPage) == numTuplesPerPage) {

		// Page containing stubs[].dataPtr
		pagePtr = memMgr -> getPage (stubs[stubId].lastDelTuple.dataPtr);
		
		stubs [stubId].lastDelTuple.posInPage = 0;
		stubs [stubId].lastDelTuple.dataPtr =
			NEXT_PAGE_LT (stubs [stubId].lastDelTuple.dataPtr) +
			firstTupleOffset;

		// [[ Explanation ]]
		ASSERT (REF_COUNT(pagePtr) > 0);				
		if (--REF_COUNT (pagePtr) == 0) {
			
			if ((rc = memMgr -> deallocatePage (pagePtr)) != 0)
				return rc;
			
#ifdef _MONITOR_
			logPageFree ();
#endif
			
		}
	}
	
	else {
		stubs [stubId].lastDelTuple.dataPtr += tupleLen;
	}
	
	return 0;
}

int WinStoreImpl::insertTuple_r (Tuple tuple, unsigned int stubId)
{
	ASSERT (stubId < numStubs);
	
	INCR (stubs [stubId].lastInsTuple);	
	ASSERT (stubs [stubId].lastInsTuple.dataPtr == tuple);
	
	return 0;
}



int WinStoreImpl::getScan_r (TupleIterator *&iter, unsigned int stubId)
{
	int rc;
	ASSERT (stubId < numStubs);

	rc = iters [stubId] -> initialize (stubs [stubId].lastDelTuple,
									   stubs [stubId].lastInsTuple);
	if (rc != 0) return rc;
	
	iter = iters [stubId];
	
	return 0;
}

int WinStoreImpl::releaseScan_r (TupleIterator *iter, unsigned int stubId)
{
	return 0;
}

int WinStoreImpl::deleteTuple_r (Tuple tuple, unsigned int stubId)
{
	int rc;
	ASSERT (stubId < numStubs);		
	char *pagePtr;
	
	if (++(stubs[stubId].lastDelTuple.posInPage) == numTuplesPerPage) {
		
		// Page containing stubs[].dataPtr
		pagePtr = memMgr -> getPage (stubs[stubId].lastDelTuple.dataPtr);
		
		stubs [stubId].lastDelTuple.posInPage = 0;
		stubs [stubId].lastDelTuple.dataPtr =
			NEXT_PAGE_LT (stubs [stubId].lastDelTuple.dataPtr) +
			firstTupleOffset;
		
		// [[ Explanation ]]
		ASSERT (REF_COUNT(pagePtr) >= 0);		
		if (--REF_COUNT (pagePtr) == 0) {
			
			if ((rc = memMgr -> deallocatePage (pagePtr)) != 0)
				return rc;

#ifdef _MONITOR_
			logPageFree ();
#endif
			
		}
	}
	
	else {
		stubs [stubId].lastDelTuple.dataPtr += tupleLen;
	}	
	
	ASSERT (stubs [stubId].lastDelTuple.dataPtr == tuple);
	
	return 0;
}

WindowIterator::WindowIterator (unsigned int tupleLen,
								unsigned int firstTupleOffset,
								unsigned int numTuplesPerPage,
								unsigned int nextPagePtrOffset_lt)
{
	this -> tupleLen = tupleLen;
	this -> numTuplesPerPage = numTuplesPerPage;
	this -> nextPagePtrOffset_lt = nextPagePtrOffset_lt;
	this -> firstTupleOffset = firstTupleOffset;
}

WindowIterator::~WindowIterator() {}

int WindowIterator::initialize (WinStoreImpl::TuplePtr lastDelTuple,
								WinStoreImpl::TuplePtr lastInsTuple)
{
	// I have actually not output any tuple, but this initialization
	// simplifies code 
	this -> lastOutputTuple = lastDelTuple;	
	this -> lastInsTuple = lastInsTuple;
	
	return 0;
}

bool WindowIterator::getNext (Tuple& tuple)
{
	if (lastInsTuple.dataPtr == lastOutputTuple.dataPtr)
		return false;
	
	INCR (lastOutputTuple);
	
	tuple = (lastOutputTuple.dataPtr);
	
	return true;
}
