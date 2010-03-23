#ifndef _WIN_STORE_IMPL_
#define _WIN_STORE_IMPL_

#include <ostream>

#ifndef _MEMORY_MGR_
#include "execution/memory/memory_mgr.h"
#endif

#ifndef _STORE_ALLOC_
#include "execution/stores/store_alloc.h"
#endif

#ifndef _WIN_STORE_
#include "execution/stores/win_store.h"
#endif

#ifndef _REL_STORE_
#include "execution/stores/rel_store.h"
#endif


namespace Execution {

	// forward decl.
	class WindowIterator;
	
	class WinStoreImpl : public StorageAlloc, public WinStore, public RelStore {
		
	private:
		friend class WindowIterator;
		
		/// Identifier for the store
		unsigned int id;
		
		/// Memory manager who serves us pages
		MemoryManager *memMgr;
		
		//---------------------------------------------------------------
		// Data page layout: The first 4 bytes of the data page is used
		// for a counter.  The counter is used to track usage for the
		// tuples within the page.  When a counter becomes zero, none of
		// the tuple in the page are in use, and the page could be
		// deallocated back to the memory manager.
		//
		// The last 4 bytes are used for a "next page pointer".  Next page
		// pointers are used to form a linked list of the pages.  Older
		// tuples occur earlier in the linked list & newer tuples later.
		//--------------------------------------------------------------
		
		/// length of tuples encoded within the store.
		unsigned int tupleLen;
		
		/// Size of pages dished out by the memory manager
		unsigned int pageSize;

		/// Offset of the first tuple
		unsigned int firstTupleOffset;
		
		/// Number of tuples that we will layout per page
		unsigned int numTuplesPerPage;
		
		/// Offset of the next page pointer within a page
		unsigned int nextPagePtrOffset;
		
		/// Offset of next page pointer from the last tuple.
		unsigned int nextPagePtrOffset_lt;
		
		/// Current page from which we are allocating tuples
		char *curPage;

		/// pointer to next tuple that we will allocate
		char *nextTuple;
		
		/// Number of tuples allocated so far in this page
		unsigned int numAllocInCurPage;
		
		//------------------------------------------------------------
		
		// A tuple pointer is a pointer to some tuple in the window.  But
		// it maintains an additional info: the position of the tuple
		// within the page which helps us increment the tuple over page
		// boundaries.
		struct TuplePtr {
			char *dataPtr;
			unsigned int  posInPage;
		};
		
		// State that we maintain for each stub
		struct Stub {
			TuplePtr lastInsTuple;
			TuplePtr lastDelTuple;
		};
		
		// Stub states
		static const unsigned int MAX_NUM_STUBS = 10;
		unsigned int numStubs;	   
		Stub stubs [MAX_NUM_STUBS];		

		// Iterators corresponding to stubs
		WindowIterator *iters [MAX_NUM_STUBS];
		
        // System-wide log
		std::ostream&  LOG;
		
		// The column for timestamps.  Timestamp is part of tuple ,\ie,
		// tupleLen should factor in the length required to encode
		// timestamps - but none of the outside entries will be aware of
		// this extra baggage associated with this tuple.  It is the job
		// of [[ some meta-query generation unit? ]] to ensure that this
		// is taken into account.
		Column tsCol;
		
	public:
		
		WinStoreImpl (unsigned int id, std::ostream& LOG);
		virtual ~WinStoreImpl ();
		
		int setTupleLen (unsigned int tupleLen);
		int setMemoryManager(MemoryManager *memMgr);
		int setNumStubs (unsigned int numStubs);
		int setTimestampCol(Column tsCol);
		int initialize ();
		
		// StorageAlloc interface
		int newTuple (Tuple& tuple);
		int addRef (Tuple tuple);
		int addRef (Tuple tuple, unsigned int ref);
		int decrRef (Tuple tuple);
		
		// WinStore interface
		int insertTuple_w (Tuple tuple, Timestamp timestamp, 
						   unsigned int stubId);
		
		bool isEmpty_w (unsigned int stubId) const;
		
		int getOldestTuple_w (Tuple& tuple, Timestamp& timestamp,
							  unsigned int stubId) const;
		
		int deleteOldestTuple_w (unsigned int stubId);
		
		// RelStore interface
		int insertTuple_r (Tuple tuple, unsigned int stubId);
		
		int deleteTuple_r (Tuple tuple, unsigned int stubId);
		
		int getScan_r (TupleIterator *&iter, unsigned int stubId);
		int releaseScan_r (TupleIterator *iter, unsigned int stubId);
		
	private:
		int computeDataLayout ();			   
		inline static void incrPtr (TuplePtr&);
	};


	class WindowIterator : public TupleIterator {
		
	private:
		WinStoreImpl::TuplePtr    lastOutputTuple;
		WinStoreImpl::TuplePtr    lastInsTuple;

		// Data layout information from WindowStorage
		unsigned int             numTuplesPerPage;
		unsigned int             nextPagePtrOffset_lt;
		unsigned int             tupleLen;
		unsigned int             firstTupleOffset;
		
	public:
		
		WindowIterator (unsigned int tupleLen,
						unsigned int firstTupleOffset,
						unsigned int numTuplesPerPage,
						unsigned int nextPagePtrOffset_lt);
		
		~WindowIterator ();
		
		int initialize (WinStoreImpl::TuplePtr lastDelTuple,
						WinStoreImpl::TuplePtr lastInsTuple);						
		
		bool getNext (Tuple& tuple);		
	};
}

#endif
