#ifndef _REL_STORE_IMPL_
#define _REL_STORE_IMPL_

#include <ostream>

#ifndef _STORE_ALLOC_
#include "execution/stores/store_alloc.h"
#endif

#ifndef _REL_STORE_
#include "execution/stores/rel_store.h"
#endif

#ifndef _MEMORY_MGR_
#include "execution/memory/memory_mgr.h"
#endif

namespace Execution {
	// forward decl.
	class RelnStoreIterator;
	
	class RelStoreImpl : public StorageAlloc, public RelStore {
	private:
		// System id
		unsigned int id;
		
		// Memory manager to serve us the pages
		MemoryManager *memMgr;
		
		// Size of pages dished out by memory manager
		unsigned int pageSize;
		
		// Length of each tuple (this includes the meta-data that we
		// maintain per tuple)
		unsigned int tupleLen;
		
		// Number of tuples that we store per page: pageSize / tupleLen
		unsigned int numTuplesPerPage;
		
		// The implementation exploits this constant - changing it
		// will break the implementation.
		static const unsigned int MAX_STUBS = 16;
		
		// Number of registered stubs
		unsigned int numStubs;
		
		// Doubly linked list of tuples
		char *tuples;		
		
		// Linked list of free tuples
		char *freeTuples;
		
		// column of the tuple where we store the next pointer
		Column        nextCol;

		// column of the tuple where we store the previous pointer
		Column        prevCol;
		
		// column of the tuple where we store the usage info for the
		// tuple: who has inserted/ who has deleted the tuple.
		Column        usageCol;
		
		// Column of the tuple where we store the refcounts [[ Explanation ]]
		Column refCountCol;
		
		std::ostream& LOG;
		
		// Iterators for various stubs
		RelnStoreIterator *iters [MAX_STUBS];
		
	public:
		RelStoreImpl (unsigned int id, std::ostream& LOG);
		virtual ~RelStoreImpl();
		
		int setMemoryManager (MemoryManager *memMgr);
		int setTupleLen (unsigned int tupleLen);
		int setNextCol (Column next);
		int setPrevCol (Column prev);
		int setUsageCol(Column usage);
		int setRefCountCol (Column refCountCol);
		int setNumStubs (unsigned int numStubs);
		int initialize ();
		
		int newTuple (Tuple& tuple);
		int addRef (Tuple tuple);
		int addRef (Tuple tuple, unsigned int ref);
		int decrRef (Tuple tuple);
		
		int insertTuple_r (Tuple tuple, unsigned int stubId);
		int deleteTuple_r (Tuple tuple, unsigned int stubId);
		int getScan_r (TupleIterator *& iter, unsigned int stubId);
		int releaseScan_r (TupleIterator *iter, unsigned int stubId);
		
	private:
		int allocateMoreSpace ();
	};
	
	// [[ Explain concurrent access semantics ]]
	class RelnStoreIterator : public TupleIterator {
	private:
		char         *nextTuple;
		Column        usageCol;
		Column        nextCol;
		
		unsigned int  mask;
		unsigned int  stubPattern;
		
	public:
		
		RelnStoreIterator (Column       usage,
						   Column       next,
						   unsigned int mask,
						   unsigned int stubPattern);		
		
		int initialize (char *tuples);
		
		~RelnStoreIterator ();
		
		bool getNext (Tuple& tuple);		
	};
}
		

#endif
