#ifndef _REL_SYN_IMPL_
#define _REL_SYN_IMPL_

#ifndef _REL_SYN_
#include "execution/synopses/rel_syn.h"
#endif

#ifndef _REL_STORE_
#include "execution/stores/rel_store.h"
#endif

#ifndef _INDEX_
#include "execution/indexes/index.h"
#endif

#ifndef _BEVAL_
#include "execution/internals/beval.h"
#endif

#ifndef  _CPP_OSTREAM_
#include <ostream>
#endif

#ifndef _FILTER_ITER_
#include "execution/internals/filter_iter.h"
#endif

namespace Execution {
	
	class RelationSynopsisImpl : public RelationSynopsis {
	private:
		/// System wide unique id
		unsigned int id;
		
		/// System log
		std::ostream &LOG;

		EvalContext *evalContext;
		
		RelStore *store;
		unsigned int stubId;

		/// Maximum number of indexes
		static const unsigned MAX_INDEXES = 10;
		
		/// Indexes on the synopsis data
		Index *indexes [MAX_INDEXES];

		/// Number of indexes
		unsigned int numIndexes;

		/// Maximum number of scans per synopsis
		static const unsigned MAX_SCANS = 10;
		
		/// Scan specifications
		struct {
			BEval *predicate;
			Index *index;
		} scans [MAX_SCANS];
		
		/// Number of scans
		unsigned int numScans;
		
		/// Forward decl.
		FilterIterator *filterIters [MAX_SCANS];
		TupleIterator *sourceIters [MAX_SCANS];
		
	public:		
		
		
		RelationSynopsisImpl(unsigned int id, std::ostream &LOG);
		~RelationSynopsisImpl();
		
		int setStore (RelStore *store, unsigned int stubId);					  
		
		// scanId is the id used to identify the scan in method "scan",
		// also predicate is the predicate not included within the index
		// scan. 
		int setIndexScan (BEval *predicate, Index *index, 
						  unsigned int& scanId);
		
		int setScan (BEval *predicate, unsigned int& scanId);		
		int setEvalContext (EvalContext *evalContext);		
		int initialize ();
		
		int insertTuple (Tuple tuple);
		int deleteTuple (Tuple tuple);
		int getScan (unsigned int scanId, TupleIterator *&iter);
		int releaseScan (unsigned int scanId, TupleIterator *iter);
	};
}

#endif
