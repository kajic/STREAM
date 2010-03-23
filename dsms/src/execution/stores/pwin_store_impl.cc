/**
 * @file      pwin_store_impl.cc
 * @date      Sept. 10, 2004
 * @brief     Implementation of partition window store
 */

#ifndef _DEBUG_
#include "common/debug.h"
#endif

#ifndef _PWIN_STORE_IMPL_
#include "execution/stores/pwin_store_impl.h"
#endif

using namespace Execution;
using namespace std;

#define NEXT_PAGE(p) (*((char **)((p) + h_nppOffset)))
#define NEXT_TUPLE(t) (((char **)(t))[d_nextCol])
#define NEXT_PAGE_LT(t) (*((char **)((t) + h_nppOffset_lt)))
#define USAGE(t) (((int *)(t))[d_usageCol])
#define REF_COUNT(t) (((int *)(t))[d_refCountCol])

#define MARK_INSERT(t,s) (USAGE(t) &= (~(0x00000001<< (s))))
#define MARK_DELETE(t,s) (USAGE(t) &= (~(0x00010000<< (s))))

#define OLDEST(t) (((char **)(t))[h_oldestCol])
#define NEWEST(t) (((char **)(t))[h_newestCol])
#define COUNT(t)  (((int *)(t))[h_countCol])
#define UNUSED(t) (USAGE(t) == 0)

PwinStoreImpl::PwinStoreImpl (unsigned int id, ostream &_LOG)
	: LOG (_LOG)
{
	this -> id = id;
	this -> memMgr = 0;
	this -> pageSize = 0;	
	this -> h_tupleLen = 0;
	this -> h_numTuplesPerPage = 0;
	this -> h_pages = 0;
	this -> hdrIndex = 0;
	this -> d_tupleLen = 0;
	this -> d_numTuplesPerPage = 0;
	this -> d_freeTuples = 0;
	this -> d_oldestExpTuple = 0;
	this -> d_newestExpTuple = 0;
	this -> evalContext = 0;
	this -> copyEval = 0;
	this -> numStubs = 0;

	for (unsigned int s = 0 ; s < MAX_STUBS ; s++)
		iters [s] = 0;
}

PwinStoreImpl::~PwinStoreImpl()
{
	if (evalContext)
		delete evalContext;
	if (copyEval)
		delete copyEval;
	for (unsigned int s = 0 ; s < MAX_STUBS ; s++)
		if (iters[s])
			delete iters[s];

}

int PwinStoreImpl::setMemoryManager (MemoryManager *memMgr)
{
	ASSERT (memMgr);
	
	this -> memMgr = memMgr;
	return 0;
}

int PwinStoreImpl::setHdrTupleLen (unsigned int len)
{
	this -> h_tupleLen = len;
	return 0;
}

int PwinStoreImpl::setOldestCol (Column col)
{
	this -> h_oldestCol = col;
	return 0;
}

int PwinStoreImpl::setNewestCol (Column col)
{
	this -> h_newestCol = col;
	return 0;
}

int PwinStoreImpl::setCountCol (Column col)
{
	this -> h_countCol = col;
	return 0;
}

int PwinStoreImpl::setDataTupleLen (unsigned int len)
{
	this -> d_tupleLen = len;
	return 0;
}

int PwinStoreImpl::setUsageCol (Column col)
{
	this -> d_usageCol = col;
	return 0;
}

int PwinStoreImpl::setNextCol (Column col)
{
	this -> d_nextCol = col;
	return 0;
}

int PwinStoreImpl::setRefCountCol (Column col)
{
	this -> d_refCountCol = col;
	return 0;
}

int PwinStoreImpl::setEvalContext (EvalContext *evalContext)
{
	ASSERT (evalContext);
	
	this -> evalContext = evalContext;
	return 0;
}

int PwinStoreImpl::setCopyEval (AEval *eval)
{
	ASSERT (eval);
	
	this -> copyEval = eval;
	return 0;
}

int PwinStoreImpl::setNumStubs (unsigned int numStubs)
{
	this -> numStubs = numStubs;
	return 0;
}

int PwinStoreImpl::setHdrIndex (Index *index)
{
	ASSERT (index);
	
	this -> hdrIndex = index;
	return 0;
}

int PwinStoreImpl::initialize ()
{
	int rc;
	
	ASSERT (memMgr);
	ASSERT (h_tupleLen > 0);
	ASSERT (d_tupleLen > 0);
	
	pageSize = memMgr -> getPageSize ();

	if ((rc = initHeaderState ()) != 0)
		return rc;
	
	if ((rc = initDataState ()) != 0)
		return rc;

	for (unsigned int s = 0 ; s < numStubs ; s++)
		iters [s] = new PwinStoreIterator (this, s);
	
	return 0;
}

int PwinStoreImpl::initHeaderState ()
{
	int rc;
	
	// Next page pointer occurs as late as possible in a page given
	// alignment requirements ...
	h_nppOffset = pageSize - sizeof(char *);
	h_nppOffset -= (h_nppOffset % __alignof__ (char *));
	
	// ... remaining space goes to header tuples
	h_numTuplesPerPage = h_nppOffset / h_tupleLen;
	
	// Page size too small to be of any use
	if (h_numTuplesPerPage == 0)
		return -1;
	
	// Offset of next pointer from the last tuple
	h_nppOffset_lt = (h_nppOffset - h_numTuplesPerPage * h_tupleLen);
	
	// Allocate the first header page
	if ((rc = memMgr -> allocatePage (h_pages)) != 0)
		return rc;
#ifdef _MONITOR_
	logPageAlloc ();
#endif	
	
	// Currently the only page in the linked list of pages
	NEXT_PAGE (h_pages) = 0;
	
	// Free slot points to the first slot in the page
	freeSlot.ptr = h_pages;
	freeSlot.pos = 0;
	
	return 0;
}

int PwinStoreImpl::initDataState ()
{	
	d_numTuplesPerPage = pageSize / d_tupleLen;	
	return allocateDataSpace();
}

int PwinStoreImpl::newTuple (Tuple &tuple)
{
	int rc;
	unsigned int newTupleUsage;
	
	// Allocate space if needed
	if (!d_freeTuples && ((rc = allocateDataSpace()) != 0))
		return rc;
	ASSERT (d_freeTuples);
	
	// New tuple = first tuple in the free list
	tuple = d_freeTuples;
	
	// Update free list 
	d_freeTuples = NEXT_TUPLE(d_freeTuples);
	NEXT_TUPLE (tuple) = 0;
	
	// Initialize tuple usage:
	newTupleUsage = 0xffffffff;
	newTupleUsage <<= numStubs;
	newTupleUsage = ~newTupleUsage;
	newTupleUsage |= (newTupleUsage << 16);
	
	USAGE(tuple) = newTupleUsage;	

	REF_COUNT(tuple) = 1;
	
	return 0;
}

int PwinStoreImpl::addRef (Tuple tuple)
{
	ASSERT (REF_COUNT(tuple) > 0);
	REF_COUNT(tuple)++;
	return 0;
}

int PwinStoreImpl::addRef (Tuple tuple, unsigned int ref)
{
	ASSERT (REF_COUNT(tuple) > 0);
	REF_COUNT(tuple) += ref;
	return 0;
}

int PwinStoreImpl::decrRef (Tuple tuple)
{
	ASSERT (REF_COUNT(tuple) > 0);
	
	if (--REF_COUNT(tuple) == 0) {
		
		ASSERT (NEXT_TUPLE (tuple) == 0);
		
		NEXT_TUPLE (tuple) = d_freeTuples;
		d_freeTuples = tuple;		
	}
	
	return 0;
}

int PwinStoreImpl::insertTuple_p (Tuple tuple, unsigned int stubId)
{
	int rc;
	TupleIterator *scan;
	char *hdrTuple;
	
	evalContext -> bind (tuple, DATA_ROLE);
	
	// Has this tuple been inserted before ... ?
	if ((rc = hdrIndex -> getScan (scan)) != 0)
		return rc;
	
	// ... Yes
	if (scan -> getNext (hdrTuple)) {
		
#ifdef _DM_
		char *dummy;
		ASSERT (!scan -> getNext(dummy));
		ASSERT (NEWEST (hdrTuple));
		ASSERT (NEXT_TUPLE (NEWEST (hdrTuple)) == 0);		
#endif
		if ((rc = hdrIndex -> releaseScan (scan)) != 0)
			return rc;
		
		NEXT_TUPLE (NEWEST (hdrTuple)) = tuple;
		NEXT_TUPLE (tuple) = 0;
		NEWEST (hdrTuple) = tuple;
		COUNT (hdrTuple) ++;
	}
	
	// ... No
	else {
		if ((rc = hdrIndex -> releaseScan (scan)) != 0)
			return rc;		
		
		// get space for new header tuple
		if ((rc = getNewHdrTuple (hdrTuple)) != 0)
			return rc;	   
		
		// copy the partitioning attributes to the header tuple
		evalContext -> bind (hdrTuple, HEADER_ROLE);
		copyEval -> eval();
		
		// insert it into the index
		if ((rc = hdrIndex -> insertTuple (hdrTuple)) != 0)
			return rc;
		
		OLDEST(hdrTuple) = tuple;
		NEWEST(hdrTuple) = tuple;
		COUNT (hdrTuple) = 1;
	}
	
	MARK_INSERT (tuple, stubId);
	
	return 0;
}

int PwinStoreImpl::deleteOldestTuple_p (Tuple partnSpec,
										Tuple &oldestTuple,
										unsigned int stubId)
{	
	int rc;
	TupleIterator *scan;
	Tuple hdrTuple;
	
	// Get the header tuple for this partition
	evalContext -> bind (partnSpec, DATA_ROLE);
	
	if ((rc = hdrIndex -> getScan (scan)) != 0)
		return rc;
	
	// The partition not present?
	if (!scan -> getNext (hdrTuple))
		return -1;
	
#ifdef _DM_
	Tuple dummy;
	ASSERT (!scan -> getNext (dummy));
#endif
	if ((rc = hdrIndex -> releaseScan (scan)) != 0)
		return rc;
	
	// Current oldest tuple
	oldestTuple = OLDEST(hdrTuple);
	ASSERT (oldestTuple);
	
	// The "new" oldest tuple:
	OLDEST(hdrTuple) = NEXT_TUPLE (oldestTuple);
	
	// The count goes down by 1
	COUNT(hdrTuple) --;
	
	// Dirty code: we  are assuming here that the store  will be used only
	// by partition window (even if size == 1) , in which case a partition
	// once introduced always has some tuples.	
	ASSERT (OLDEST(hdrTuple));
	
	// Delete the "old" oldest tuple
	MARK_DELETE (oldestTuple, stubId);
	
	if (!UNUSED (oldestTuple)) {
		
		if (!d_newestExpTuple) {
			ASSERT (!d_oldestExpTuple);
			d_oldestExpTuple = oldestTuple;
		}
		
		else {
			ASSERT (!NEXT_TUPLE (d_newestExpTuple));
			ASSERT (d_oldestExpTuple);	   	
			NEXT_TUPLE (d_newestExpTuple) = oldestTuple;
		}
		
		d_newestExpTuple = oldestTuple;
		NEXT_TUPLE (d_newestExpTuple) = 0;
	}
	
#ifdef _DM_
	else {		   		
		ASSERT (numStubs == 1);
		ASSERT (REF_COUNT (oldestTuple) > 0);
		NEXT_TUPLE (oldestTuple) = 0;
	}
#endif
	
	return 0;
}

int PwinStoreImpl::getPartnSize_p (Tuple partnSpec,
								   unsigned int& partnSize,
								   unsigned int stubId)
{
	int rc;
	TupleIterator *scan;
	Tuple hdrTuple;
	
	if ((rc = hdrIndex -> getScan (scan)) != 0)
		return rc;
	
	// The partition not present?
	if (!scan -> getNext (hdrTuple))
		return -1;
	
#ifdef _DM_
	Tuple dummy;
	ASSERT (!scan -> getNext (dummy));
#endif
	if ((rc = hdrIndex -> releaseScan (scan)) != 0)
		return rc;
	
	partnSize = COUNT(hdrTuple);
	return 0;	
}

int PwinStoreImpl::insertTuple_r (Tuple tuple, unsigned int stubId)
{	
	MARK_INSERT (tuple, stubId);
	return 0;
}

int PwinStoreImpl::deleteTuple_r (Tuple tuple, unsigned int stubId)
{	
	MARK_DELETE (tuple, stubId);
	
	// If the tuple is unused then it should be moved from expired tuples
	// list to free tuples list.  Since the expiration of tuples is the
	// same in all the synopsis stubs, when a tuple becomes unused it is
	// the currently oldest expired tuple.	
	if (UNUSED (tuple)) {
		
		ASSERT (tuple == d_oldestExpTuple);
		
		d_oldestExpTuple = NEXT_TUPLE (d_oldestExpTuple);
		if (!d_oldestExpTuple)
			d_newestExpTuple = 0;

#ifdef _DM_
		ASSERT (REF_COUNT(tuple) > 0);
		NEXT_TUPLE (tuple) = 0;
#endif
	}
	
	return 0;
}

int PwinStoreImpl::getScan_r (TupleIterator *&iter, unsigned int stubId)
{
	int rc;
	
	if ((rc = iters [stubId] -> initialize()) != 0)
		return rc;

	iter = iters [stubId];
	return 0;
}

int PwinStoreImpl::releaseScan_r (TupleIterator *iter,
								  unsigned int stubId)
{
	return 0;
}

int PwinStoreImpl::allocateDataSpace ()
{
	int rc;
	char *newPage;
	char *tuple;
	
	// We come here only when there are no free tuples
	ASSERT (!d_freeTuples);
	
	if ((rc = memMgr -> allocatePage (newPage)) != 0)
		return rc;
	
#ifdef _MONITOR_
	logPageAlloc ();
#endif	
	
	// Form a linked list of the tuples in the page
	// Note: we loop (d_numTuplesPerPage - 1) times
	tuple = d_freeTuples = newPage;
	for (unsigned int t = 1 ; t < d_numTuplesPerPage ;
		 t++, tuple += d_tupleLen) {
		NEXT_TUPLE (tuple) = (tuple + d_tupleLen);
	}
	NEXT_TUPLE (tuple) = 0;
	
	return 0;
}

int PwinStoreImpl::getNewHdrTuple (char *&hdrTuple)
{
	int rc;
	char *newPage;
	
	hdrTuple = freeSlot.ptr;
	ASSERT (hdrTuple);
	
	// We have reached the end of this page.  Allocate a nwe page.  The
	// freeSlot is the first slot in the new page
	if (++freeSlot.pos == h_numTuplesPerPage) {
		
		if ((rc = memMgr -> allocatePage (newPage)) != 0)
			return rc;
		
#ifdef _MONITOR_
		logPageAlloc ();
#endif	
		
		freeSlot.pos = 0;						
		NEXT_PAGE_LT (freeSlot.ptr) = newPage;
		freeSlot.ptr = newPage;
	}

	// We haven't reached the end of this page.  freeSlot is the next slot
	// in this page
	else {
		freeSlot.ptr += h_tupleLen;
	}
	
	return 0;
}

PwinStoreIterator::PwinStoreIterator(PwinStoreImpl *store,
									 unsigned int stubId)
{
	this -> store = store;
	this -> h_oldestCol = store -> h_oldestCol;
	this -> h_nppOffset_lt = store -> h_nppOffset_lt;
	this -> d_usageCol = store -> d_usageCol;
	this -> d_nextCol = store -> d_nextCol;	
	this -> mask = (0x00010001 << stubId);	
	this -> stubPattern = (0x00010000 << stubId);
}

int PwinStoreIterator::initialize ()
{	
	this -> nextTuple = 0;
	this -> hdrTuple.ptr = 0;
	
	if (getNextExpTuple ()) {
		state = EXPIRED;
	}
	else if (getNextMainTuple ()) {
		state = MAIN;
	}
	else {	
		state = END;
	}

	return 0;
}

PwinStoreIterator::~PwinStoreIterator () {}

bool PwinStoreIterator::getNext (Tuple& tuple)
{
	if (state == END)
		return false;
	
	ASSERT (nextTuple);
	
	tuple = nextTuple;
	
	if ((state == EXPIRED) && !getNextExpTuple())
		state = (getNextMainTuple()) ? MAIN : END;
	
	if ((state == MAIN) && !getNextMainTuple())
		state = END;
	
	return true;
}

bool PwinStoreIterator::getNextExpTuple ()
{
	if (nextTuple) {
		nextTuple = NEXT_TUPLE (nextTuple);
	}
	
	else {
		nextTuple = store -> d_oldestExpTuple;
	}
	
	while (nextTuple && ((USAGE(nextTuple) & mask) != stubPattern))
		nextTuple = NEXT_TUPLE (nextTuple);
	
	return (nextTuple != 0);
}

bool PwinStoreIterator::getNextMainTuple ()
{
	// Assert: this method has been called previously
	if (nextTuple) {
		nextTuple = NEXT_TUPLE (nextTuple);
		
		while (nextTuple && ((USAGE(nextTuple) & mask) != stubPattern))
			nextTuple = NEXT_TUPLE (nextTuple);
		
		if (nextTuple) return true;
	}
	
	while (getNextHdrTuple ()) {
		
		nextTuple = OLDEST (hdrTuple.ptr);
		ASSERT (nextTuple);
		
		while (nextTuple && ((USAGE(nextTuple) & mask) != stubPattern))
			nextTuple = NEXT_TUPLE (nextTuple);
		
		if (nextTuple) return true;			
	}
	
	return false;
}

bool PwinStoreIterator::getNextHdrTuple ()
{
	// Called for the first time
	if (!hdrTuple.ptr) {
		
		ASSERT (store -> h_pages);
		hdrTuple.ptr = store -> h_pages;
		hdrTuple.pos = 0;
		
	}

	// This method has been called before
	else {
		
		hdrTuple.pos ++;
		
		if (hdrTuple.pos == store -> h_numTuplesPerPage) {
			hdrTuple.pos = 0;			
			hdrTuple.ptr = NEXT_PAGE_LT (hdrTuple.ptr);
		}
		
		else {
			hdrTuple.ptr += store -> h_tupleLen;
		}
	}
	
	return (hdrTuple.ptr != store -> freeSlot.ptr);
}
