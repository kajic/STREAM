#ifndef _LIN_STORE_IMPL_
#define _LIN_STORE_IMPL_

#ifndef _CPP_OSTREAM_
#include <ostream>
#endif

#ifndef _STORE_ALLOC_
#include "execution/stores/store_alloc.h"
#endif

#ifndef _REL_STORE_
#include "execution/stores/rel_store.h"
#endif

#ifndef _LIN_STORE_
#include "execution/stores/lin_store.h"
#endif

#ifndef _MEMORY_MGR_
#include "execution/memory/memory_mgr.h"
#endif

#ifndef _EVAL_CONTEXT_
#include "execution/internals/eval_context.h"
#endif

#ifndef _INDEX_
#include "execution/indexes/index.h"
#endif

namespace Execution {
	class LinStoreIterator;
	
	class LinStoreImpl : public StorageAlloc, public RelStore, public LinStore {
	private:
		
		/// System-wide unique id
		unsigned int id;
		
		/// System log
		std::ostream &LOG;
		
		/// System-wide memory manager
		MemoryManager *memMgr;
		
		/// Size of pages dished out by memory manager
		unsigned int pageSize;
		
		/// Length of each tuple including the metadata (lineages, next
		/// pointer, usage info) part
		unsigned int tupleLen;
		
		/// Number of tuples that we store per page: pageSize / tupleLen
		unsigned int numTuplesPerPage;
		
		/// Maximum number of synopsis stubs we allow.  Note: The
		/// implementation exploits this constant - changing it will break
		/// the implementation.
		static const unsigned int MAX_STUBS = 16;

		/// number of registered stubs
		unsigned int numStubs;
		
		/**
		 * Linked list of tuples that have been inserted by at least one
		 * stub.  Ideally we do not want to keep around tuples that have
		 * been deleted by all stubs, but we nevertheless do so to avoid
		 * maintaining  a "prev" pointer in the linked list.   So we do
		 * keep tuples that are not relevant to any stub in the linked
		 * list.  Instead, when the number of useless tuples in the linked
		 * list exceeds some threshold, we scan the list and reclaim the
		 * unused tuples.
		 */
		char         *tuples;
		
		/// Linked list of free locations in the pages we have allocated so
		/// far.
		char         *freeTuples;
		
		/// column of the tuple where we store the next pointer
		Column        nextCol;

		// column of the tuple where we store the prev pointer
		Column        prevCol;
		
		/// column of the tuple where we store the usage info for the
		/// tuple: who has inserted/ who has deleted the tuple.
		Column        usageCol;
		
		// Column of the tuple where we store the refcounts [[ Explanation ]]
		Column refCountCol;
		
		//----------------------------------------------------------------------
		// Lineage information
		//----------------------------------------------------------------------
		
		/// Maximum number of lineage columns
		static const unsigned int MAX_LINEAGE = 2;
		
		/// The columns in the stored tuples that correspond to lineages.
		Column linCols [MAX_LINEAGE];
		
		/// number of lineages in this store
		unsigned int numLins;
		
		/// Index on the lineage columns
		Index *linIndex;
		
		/// Evaluation context (needed for index lookup)
		EvalContext *evalContext;
		
		/// A "buffer" tuple needed for index lookup
		Tuple linTuple;

		//----------------------------------------------------------------------
		// Iterators for stubs
		//----------------------------------------------------------------------
		LinStoreIterator *iters [MAX_STUBS];
		
	public:
		LinStoreImpl (unsigned int id, std::ostream& LOG);
		virtual ~LinStoreImpl();
		
		int setMemoryManager (MemoryManager *memMgr);
		int setTupleLen (unsigned int tupleLen);
		int setNextCol (Column next);
		int setPrevCol (Column prev);			
		int setUsageCol (Column usage);		
		int setRefCountCol (Column refcount);
		int setNumStubs (unsigned int numStubs);
		int setThreshold (float threshold);
		int addLineage (Column col);
		int setLineageTuple (Tuple tuple);
		int setIndex (Index *index);
		int setEvalContext (EvalContext *evalContext);
		int initialize ();
		
		int newTuple (Tuple& tuple);
		int addRef (Tuple tuple);
		int addRef (Tuple tuple, unsigned int ref);
		int decrRef (Tuple tuple);
		
		int insertTuple_r (Tuple tuple, unsigned int stubId);
		int deleteTuple_r (Tuple tuple, unsigned int stubId);
		int getScan_r (TupleIterator *& iter, unsigned int stubId);
		int releaseScan_r (TupleIterator *iter, unsigned int stubId);
		
		int insertTuple_l (Tuple tuple, Tuple *lineage,
						   unsigned int stubId);
		int deleteTuple_l (Tuple tuple, unsigned int stubId);
		int getTuple_l (Tuple *lineage, Tuple &tuple,
						unsigned int stubId);
		
	private:
		int allocateMoreSpace ();
	};
	
	// [[ Explain concurrent access semantics ]]
	class LinStoreIterator : public TupleIterator {
	private:
		char         *nextTuple;
		Column        usageCol;
		Column        nextCol;
		
		unsigned int  mask;
		unsigned int  stubPattern;
		
	public:
		
		LinStoreIterator (Column       usage,
						  Column       next,
						  unsigned int mask,
						  unsigned int stubPattern);
		
		int initialize (char *tuples);
		
		~LinStoreIterator ();
		
		bool getNext (Tuple& tuple);		
	};
}

#endif
