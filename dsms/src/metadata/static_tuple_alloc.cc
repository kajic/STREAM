#ifndef _STATIC_TUPLE_ALLOC_
#include "metadata/static_tuple_alloc.h"
#endif

using namespace Metadata;

StaticTupleAlloc::StaticTupleAlloc (MemoryManager *_memMgr)
{
	int rc;
	
	ASSERT (_memMgr);
	
	memMgr = _memMgr;
	pageSize = memMgr -> getPageSize ();
	
	if ((rc = memMgr -> allocatePage (curPage)) != 0) {
		curPage = 0;
		freePtr = 0;
		return;
	}
	
	freePtr = curPage;
}

StaticTupleAlloc::~StaticTupleAlloc () {}

int StaticTupleAlloc::getStaticTuple (unsigned tupleLen, char *& tuple)
{
	int rc;
	
	if (!curPage)
		return -1;
	
	ASSERT (freePtr >= curPage);
	ASSERT (freePtr - curPage < (int)pageSize);
	
	// We can't satisfy this request
	if (tupleLen > pageSize)
		return -1;
	
	// We dont have space in the current page: allocate a new one
	if (pageSize - (freePtr - curPage) < tupleLen) {		
		if ((rc = memMgr -> allocatePage (curPage)) != 0) {
			curPage = 0;
			freePtr = 0;
			return rc;
		}
		freePtr = curPage;
	}
	
	tuple = freePtr;	
	freePtr += tupleLen;
	
	return 0;
}

	

