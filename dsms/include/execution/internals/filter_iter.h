#ifndef _FILTER_ITER_
#define _FILTER_ITER_

#ifndef _BEVAL_
#include "execution/internals/beval.h"
#endif

#ifndef _TUPLE_ITER_
#include "execution/internals/tuple_iter.h"
#endif

namespace Execution {
	class FilterIterator : public TupleIterator {
	private:

		TupleIterator    *source;
		BEval            *pred;
		EvalContext      *evalContext;
		
		static const unsigned int FI_SCAN_ROLE = 8;
		
	public:
		FilterIterator (EvalContext *evalContext,
						BEval       *predicate);
		
		~FilterIterator ();
		
		int initialize (TupleIterator *source);
		
		bool getNext (Tuple& tuple);
	};
}

#endif

