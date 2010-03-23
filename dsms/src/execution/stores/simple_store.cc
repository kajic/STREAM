
#ifndef _DEBUG_
#include "common/debug.h"
#endif

#ifndef _SIMPLE_STORE_
#include "execution/stores/simple_store.h"
#endif

#ifndef _TYPES_
#include "common/types.h"
#endif

using namespace std;
using namespace Execution;

#define COUNT(p) (*((int *)(p)))

SimpleStore::SimpleStore(unsigned int _id, ostream& _LOG)
	: LOG (_LOG)
{
	this -> id                = _id;
	this -> memMgr            = 0;
	this -> tupleLen          = 0;
	this -> numTuplesPerPage  = 0;
	this -> firstTupleOffset  = 0;
	this -> nextTuple         = 0;
	this -> numAllocInCurPage = 0;
}

SimpleStore::~SimpleStore() {}

int SimpleStore::setMemoryManager(MemoryManager *memMgr)
{
	ASSERT (memMgr);
	
	this -> memMgr   = memMgr;
	return 0;
}

int SimpleStore::setTupleLen (unsigned int tupleLen)
{
	ASSERT (tupleLen > 0);
	
	this -> tupleLen = tupleLen;	
	return 0;
}

int SimpleStore::initialize()
{
	int rc;
	
	ASSERT (memMgr);
	ASSERT (tupleLen > 0);
	
	// Compute the layout of tuples in a page
	if ((rc = computePageLayout ()) != 0)
		return rc;
	
	// Get a new page from memory manager & initialize
	if ((rc = allocateNewPage ()) != 0)
		return rc;
	
	return 0;
}

int SimpleStore::newTuple(Tuple& tuple)
{
	int rc;
	
	ASSERT (numAllocInCurPage < numTuplesPerPage);
	ASSERT (nextTuple);
	ASSERT (COUNT (curPage) > 0);
	
	tuple = nextTuple;
	COUNT (curPage)++;	
	
	if (++numAllocInCurPage == numTuplesPerPage) {				
		// Corresponding to COUNT(curPage) = 1 in allocateNewPage
		COUNT (curPage)--;
		
		if ((rc = allocateNewPage()) != 0) {
			return rc;
		}
	}
	
	else {
		nextTuple += tupleLen;
	}
	
	return 0;
}

int SimpleStore::addRef (Tuple tuple)
{	
	ASSERT (COUNT (memMgr -> getPage (tuple)) > 0);
	COUNT (memMgr -> getPage (tuple))++;
	
	return 0;
}

int SimpleStore::addRef (Tuple tuple, unsigned int ref)
{
	ASSERT (COUNT (memMgr -> getPage (tuple)) > 0);
	COUNT (memMgr -> getPage (tuple)) += ref;
	return 0;
}

int SimpleStore::decrRef (Tuple tuple)
{
	ASSERT (COUNT (memMgr -> getPage (tuple)) > 0);
	if (--COUNT (memMgr -> getPage (tuple)) == 0) {
		
#ifdef _MONITOR_
		logPageFree ();
#endif
		
		return memMgr -> deallocatePage (memMgr -> getPage (tuple));
	}
	
	return 0;
}

/** 
 * Each page contains a 4 byte integer counter that keeps track of the
 * usage of tuples in the page.  The counter occurs in the beginning of
 * the page followed by the tuples of the page.
 */ 
int SimpleStore::computePageLayout ()
{
	pageSize = memMgr -> getPageSize();
	
	// We want to layout the tuples only at multiples of tupleLen from the
	// beginning of the page (to play safe with alignment related issues)	
	firstTupleOffset = 0;
	for (unsigned int t = 0 ; t < pageSize && firstTupleOffset < INT_SIZE ; t++) {
		firstTupleOffset += tupleLen;
	}
	
	// Number of tuples we can layout per page
	numTuplesPerPage = (pageSize - firstTupleOffset) / tupleLen;
	if (numTuplesPerPage == 0)
		return -1;
	
	return 0;
}

int SimpleStore::allocateNewPage ()
{
	int rc;
	
	if ((rc = memMgr -> allocatePage(curPage)) != 0)
		return rc;
	
	COUNT (curPage) = 1;
	
	nextTuple = curPage + firstTupleOffset;
	numAllocInCurPage = 0;

#ifdef _MONITOR_
	logPageAlloc ();
#endif
	
	return 0;
}
