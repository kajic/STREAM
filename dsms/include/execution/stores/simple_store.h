#ifndef _SIMPLE_STORE_
#define _SIMPLE_STORE_

#include <ostream>

#ifndef _STORE_ALLOC_
#include "execution/stores/store_alloc.h"
#endif

#ifndef _MEMORY_MGR_
#include "execution/memory/memory_mgr.h"
#endif

/**
 * @file       simple_store.h
 * @date       Jun 3, 2004
 * @brief      Simple storage allocator that does not support
 *             "sharing".
 */


namespace Execution {
	class SimpleStore : public StorageAlloc {
	private:
		/// System id
		unsigned int id;
		
		/// Memory manager to serve us the pages
		MemoryManager *memMgr;
		
		/// Size of pages served by memory manager
		unsigned int pageSize;
		
		/// Length of each tuple we allocate.
		unsigned int tupleLen;
		
		/// Number of tuples that we store per page
		unsigned int numTuplesPerPage;

		/// Offset of the first tuple in a page
		unsigned int firstTupleOffset;

		/// Page from which we are currently allocating tuples
		char *curPage;
		
		/// pointer to next tuple that we will allocate
		char *nextTuple;
		
		/// Number of tuples allocated so far in this page
		unsigned int numAllocInCurPage;
		
		/// System Log
		std::ostream& LOG;
		
	public:
		
		SimpleStore (unsigned int id, std::ostream& LOG);
		virtual ~SimpleStore ();
		
		int setMemoryManager(MemoryManager *memMgr);
		int setTupleLen(unsigned int tupleLen);
		int initialize();
		
		int newTuple (Tuple& tuple);
		int addRef (Tuple tuple);
		int addRef (Tuple tuple, unsigned int ref);
		int decrRef (Tuple tuple);

	private:
		int computePageLayout ();
		int allocateNewPage ();
	};
}
		
#endif
