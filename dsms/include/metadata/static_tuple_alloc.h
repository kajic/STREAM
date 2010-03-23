#ifndef _STATIC_TUPLE_ALLOC_
#define _STATIC_TUPLE_ALLOC_

#ifndef _MEMORY_MGR_
#include "execution/memory/memory_mgr.h"
#endif

using Execution::MemoryManager;

namespace Metadata {
	class StaticTupleAlloc {
	private:
		
		/// System-wide memory manager
		MemoryManager *memMgr;

		/// Size of pages allocated by memory manager
		unsigned int pageSize;
		
		/// Current page from which we are allocating tuples
		char *curPage;
		
		/// Pointer to next free slot
		char *freePtr;
		
	public:
		StaticTupleAlloc (MemoryManager *memMgr);		
		~StaticTupleAlloc ();
		
		int getStaticTuple (unsigned int tupleSize, char *&tuple);
	};
}

#endif
