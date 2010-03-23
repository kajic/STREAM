#ifndef _HASH_INDEX_
#define _HASH_INDEX_

#ifndef _INDEX_
#include "execution/indexes/index.h"
#endif

#ifndef _BEVAL_
#include "execution/internals/beval.h"
#endif

#ifndef _HEVAL_
#include "execution/internals/heval.h"
#endif

#ifndef _MEMORY_MGR_
#include "execution/memory/memory_mgr.h"
#endif

#ifndef _EVAL_CONTEXT_
#include "execution/internals/eval_context.h"
#endif

#ifndef _CPP_OSTREAM_
#include <ostream>
#endif

namespace Execution {

	// forward decl.
	class HashIndexIterator;
	
	// Implementation is essentially the standard one: fixed number of
	// buckets, and an index structure that linearizes memory for layout
	// of buckets. [[ Explanation ]]
	
	class HashIndex : public Index {
	private:
		
		// An entry in the hash index: 
		struct Entry {
			char   *dataPtr;
			Entry  *next;
		};
		
		/// System-wide unique identifier for this object
		unsigned int id;
		
		// Memory manager who gives us pages.
		MemoryManager         *memMgr;
		
		// size of pages served out by memory manager
		unsigned int           pageSize;

		//----------------------------------------------------------------------
		// Parameters defining the bucket-index structure
		//----------------------------------------------------------------------
		
		// Number of bits of a hash value used to identify the bucket.
		unsigned int           numBits;		
		
		// Number of buckets in the hashtable: 2^{numBits}.
		unsigned int           numBuckets;
		
		// [[ needs explanation]]: floor (log_2 (pageSize / sizeof(char*))).
		unsigned int           numBitsPerLayer;
		
		// [[ needs explanation]]: ceil (numBits / numBitsPerLayer).
		unsigned int           numLayers;
		
		//----------------------------------------------------------------------
		// Parameters defining the data (index entries) layout within a
		// page
		//----------------------------------------------------------------------
		
		// number of data entries in a data page
		unsigned               numEntriesPerPage;
		
		// The offset (in bytes) of the data part in a page.
		unsigned int           dataPartOffset;
		
		//----------------------------------------------------------------------
		// Structure-holding state:
		//----------------------------------------------------------------------
		
		// Root node from which we start our search for a bucket.
		char                  *bucketIndexRoot;
		
		// Linked list of empty hash index entries
		Entry                 *freeEntriesList;
		
		// Linked list of data pages
		char                  *dataPageList;
		
		//----------------------------------------------------------------------
		// Statistics used to determine when to grow (and maybe in future,
		// shrink)
		//----------------------------------------------------------------------
		
		// When the fraction of nonempty buckets increases beyond the
		// threshold, we double the size.  
		float                  threshold;
		
		// The number of nonempty buckets
		unsigned int           numNonEmptyBuckets;
		
		//----------------------------------------------------------------------
		// Evaluators used to process data within tuples 
		//----------------------------------------------------------------------
		
		// Evaluation context ....
		EvalContext          *evalContext;
		
		// Hashes the keys of a (bound) tuple.
		HEval                *updateHashEval;
		
		// Used to eliminate false positives during scan. [[ explain ]]
		BEval                *keyEqual;
		
		// Computes the hash for a scan [[ explain ]]
		HEval                *scanHashEval;
		
		//----------------------------------------------------------------------
		// Tuple Iterator for scanning
		//----------------------------------------------------------------------
		HashIndexIterator    *iter;		
		
		//----------------------------------------------------------------------
		// Misc.
		//----------------------------------------------------------------------
		
		// Mask to extract bits from a hash value (used in getBucket)
		Hash                  ROOT_MASK;
		Hash                  NON_ROOT_MASK;
		
		// System log for writing messages
		std::ostream&         LOG;
		
		static const unsigned int UPDATE_ROLE = 6;

#ifdef _MONITOR_
		/// Number of hash entries
		int numEntries;
#endif
		
	public:
		friend class HashIndexIterator;
		
		HashIndex (unsigned int id, std::ostream& LOG);
		virtual ~HashIndex();
		
		int setMemoryManager (MemoryManager *memMgr);
		int setEvalContext (EvalContext *evalContext);
		int setUpdateHashEval (HEval *hashEval);
		int setScanHashEval (HEval *hashEval);
		int setKeyEqual (BEval *keyEqual);
		int setThreshold (float threshold);
		int initialize();
		
		int insertTuple (Tuple tuple);
		int deleteTuple (Tuple tuple);
		int getScan (TupleIterator *&iter);
		int releaseScan (TupleIterator *iter);
		
		void printDist() const;

		virtual int getIntProperty (int property, int& val);
		
	private:
		int computeBucketIndexParams();
		int computeDataPageLayoutParams();
		int constructBucketIndex();
		
		Entry **getBucket (Hash hashValue) const;
		Entry *getNewEntry ();
		void freeEntry (Entry *entryPtr);
		
		int doubleNumBuckets ();
		int rehashBucketIndexNode (char *, char *, unsigned int, Hash);		
		int rehashBucket (Entry **, Entry **, Hash);		
	};
	
	class HashIndexIterator : public TupleIterator {
	private:
		EvalContext          *evalContext;
		BEval                *keyEqual;
		HashIndex::Entry     *entryList;

		static const unsigned int SCAN_ROLE = 7;
	public:
		
		HashIndexIterator (EvalContext      *_evalContext, 
						   BEval            *_keyEqual) { 			
			evalContext = _evalContext;
			keyEqual = _keyEqual;
		}
		
		~HashIndexIterator () {}
		
		int initialize (HashIndex::Entry *_entryList) {
			entryList = _entryList;
			
			// get the first tuple
			for ( ; entryList ; entryList =  entryList -> next) {
				
				evalContext -> bind (entryList -> dataPtr, SCAN_ROLE);
				
				// found the tuple
				if (keyEqual -> eval())
					break;
			}
			
			return 0;
		}
		
		bool getNext (Tuple& tuple) {
			if (!entryList)
				return false;
			
			tuple = entryList -> dataPtr;
			
			// get the next tuple
			for ( entryList = entryList -> next ; entryList ; 
				  entryList =  entryList -> next) {
				
				evalContext -> bind (entryList -> dataPtr, SCAN_ROLE);
				
				// found the tuple
				if (keyEqual -> eval())
					break;
			}
			
			return true;
		}
	};
}

#endif
