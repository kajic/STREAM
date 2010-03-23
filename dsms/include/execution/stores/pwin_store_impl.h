#ifndef _PWIN_STORE_IMPL_
#define _PWIN_STORE_IMPL_

#ifndef _PWIN_STORE_
#include "execution/stores/pwin_store.h"
#endif

#ifndef _REL_STORE_
#include "execution/stores/rel_store.h"
#endif

#ifndef _STORE_ALLOC_
#include "execution/stores/store_alloc.h"
#endif

#ifndef _MEMORY_MGR_
#include "execution/memory/memory_mgr.h"
#endif

#ifndef _CPP_OSTREAM
#include <ostream>
#endif

#ifndef _EVAL_CONTEXT_
#include "execution/internals/eval_context.h"
#endif

#ifndef _AEVAL_
#include "execution/internals/aeval.h"
#endif

#ifndef _INDEX_
#include "execution/indexes/index.h"
#endif

namespace Execution {

	// forward decl.
	class PwinStoreIterator;
	
	/**
	 * An implementation of partition window store that provides the
	 * storage space and access methods for *one* partition window
	 * synopsis and any number of relation synopses.  
	 */ 
	
	class PwinStoreImpl : public StorageAlloc, public PwinStore, public RelStore {
	private:
		friend class PwinStoreIterator;
		
		/// System-wide id
		unsigned int id;
		
		/// System-wide memory manager
		MemoryManager *memMgr;
		
		/// Size of pages allocated by memory manager
		unsigned int pageSize;
		
		/// System log
		std::ostream &LOG;
		
		//----------------------------------------------------------------------
		// State related to header tuples.
		//----------------------------------------------------------------------
		
		/// Length of a header (tuple)
		unsigned int h_tupleLen;
		
		/// number of header tuples that fit in one page
		unsigned int h_numTuplesPerPage;
		
		/// Oldest-data-tuple column for the partition corr. to the header
		/// tuple
		Column h_oldestCol;
		
		/// Newest-data-tuple column ....
		Column h_newestCol;		

		/// Count for a partition
		Column h_countCol;
		
		/// Linked list of header pages
		char *h_pages;
		
		/// Pointer to the next available slot for a new header tuple.  We
		/// also store the  position of the pointer within  a page so that
		/// we can identify page boundaries.
		
		struct {
			char *ptr;
			unsigned int pos;
		} freeSlot;
		
		/// Offset within a page where we store the next-page pointer (npp)
		unsigned int h_nppOffset;

		/// Next page pointer offset from the last tuple
		unsigned int h_nppOffset_lt;
		
		/// Index over header tuples that let us do equality searches over
		/// partitioning attributes
		Index *hdrIndex;
		
		//----------------------------------------------------------------------
		// State related to data tuples
		//----------------------------------------------------------------------
		
		/// Length of a data tuple
		unsigned int d_tupleLen;
		
		/// Number of data tuples per page
		unsigned int d_numTuplesPerPage;
		
		/// Linked list of free data tuple slots
		char *d_freeTuples;
		
		/// Oldest expired tuple (start of expired tuples linked list)
		char *d_oldestExpTuple;
		
		/// Newest expired tuple (end of expired tuples linked list)
		char *d_newestExpTuple;
		
		/// Column recording usage info of a data tuple for each stub.  We
		/// can infer using this column if this data tuple is visible to a
		/// stub or not.
		Column d_usageCol;
		
		/// Column for next pointer.  Data tuples belonging to one
		/// partition are linked in a list in order of insertion time
		Column d_nextCol;

		/// Column for the ref counts
		Column d_refCountCol;
		
		//----------------------------------------------------------------------
		// Evaluators
		//----------------------------------------------------------------------
		
		/// Evaluation context for all computations
		EvalContext *evalContext;
		
		/// Copy the partitioning attributes from an inserted data tuple
		/// to a header tuple
		AEval *copyEval;

		//----------------------------------------------------------------------
		// Stub information
		//----------------------------------------------------------------------

		/// Maximum  number   of  stubs  permitted.    The  implementation
		/// exploits this  constant in nontrivial ways -  changing it will
		/// break things		
		static const unsigned int MAX_STUBS = 16;
		
		/// number of registered stubs
		unsigned int numStubs;

		/// Iterators for stub scans
		PwinStoreIterator *iters [MAX_STUBS];
		
	public:
		
		PwinStoreImpl (unsigned int id, std::ostream& LOG);
		virtual ~PwinStoreImpl ();
		
		//----------------------------------------------------------------------
		// Initiatialization routines
		//----------------------------------------------------------------------
		
		int setMemoryManager (MemoryManager *memMgr);
		int setHdrTupleLen (unsigned int len);
		int setOldestCol (Column col);
		int setNewestCol (Column col);
		int setCountCol (Column col);
		int setHdrIndex (Index *index);
		int setDataTupleLen (unsigned int len);
		int setUsageCol (Column col);
		int setNextCol (Column col);
		int setRefCountCol (Column col);
		int setEvalContext (EvalContext *evalContext);
		int setCopyEval (AEval *eval);
		int setNumStubs (unsigned int numStubs);
		int initialize();
		
		//----------------------------------------------------------------------
		// Inherited from StoreAlloc
		//----------------------------------------------------------------------
		int newTuple (Tuple &tuple);
		int addRef (Tuple tuple);
		int addRef (Tuple tuple, unsigned int ref);
		int decrRef (Tuple tuple);
		
		//----------------------------------------------------------------------
		// Inherited from PwinStore
		//----------------------------------------------------------------------
		int insertTuple_p (Tuple tuple, unsigned int stubId);
		int deleteOldestTuple_p (Tuple partnSpec, Tuple &oldestTuple,
								 unsigned int stubId);
		int getPartnSize_p (Tuple partnSpec, unsigned int &partnSize,
							unsigned int stubId);
		
		//----------------------------------------------------------------------
		// Inherited from RelStore
		//----------------------------------------------------------------------
		int insertTuple_r (Tuple tuple, unsigned int stubId);
		int deleteTuple_r (Tuple tuple, unsigned int stubId);
		int getScan_r (TupleIterator *&iter, unsigned int stubId);
		int releaseScan_r (TupleIterator *iter, unsigned int stubId);

		static const unsigned int DATA_ROLE = 2;
		static const unsigned int HEADER_ROLE = 3;		
		
	private:
		int initHeaderState ();
		int initDataState ();
		int allocateDataSpace ();
		int getNewHdrTuple(char *&hdrTuple);
	};
	
	class PwinStoreIterator : public TupleIterator {
	private:
		
		/// Three states that an iterator could be depending on which
		/// tuples it is reading: 
		enum State {
			
			/// State when reading expired tuples
			EXPIRED,
			
			/// State when reading tuples currently in partition window
			MAIN,
			
			/// State when iterator has reached the end
			END
		};
		
		/// State of the iterator
		State state;
		
		/// Next tuple that will be returned by the iterator if state != END
		Tuple nextTuple;
		
		/// The partition (header tuple) whose data tuples are being
		/// read.  Valid only if state == MAIN
		struct {
			char *ptr;
			unsigned int pos;
		} hdrTuple;
		
		/// Store whose tuples we are scanning
		PwinStoreImpl *store;
		
		/// Copied from namesakes within stores to make macros work
		Column h_nppOffset_lt;
		Column h_oldestCol;
		Column d_usageCol;				
		Column d_nextCol;
		
		/// bit mask used to determine if a tuple belongs to the stub
		unsigned int mask;
		
		/// bit pattern used to determine if a tuple belongs to the stub
		unsigned int stubPattern;
		
	public:
		
		/**
		 * @param stubId    stub for which we are doing the scan
		 * @param store     The store which created this iterator		 
		 */ 		
		PwinStoreIterator (PwinStoreImpl *store, unsigned int stubId);		
		
		~PwinStoreIterator();

		int initialize ();
		
		bool getNext (Tuple& tuple);
		
	private:
		bool getNextExpTuple();
		bool getNextMainTuple ();
		bool getNextHdrTuple ();
	};
}

#endif
