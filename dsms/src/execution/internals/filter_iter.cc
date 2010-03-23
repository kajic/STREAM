#ifndef _FILTER_ITER_
#include "execution/internals/filter_iter.h"
#endif

#ifndef _DEBUG_
#include "common/debug.h"
#endif

using namespace Execution;

FilterIterator::FilterIterator (EvalContext *evalContext,
								BEval *pred)
{
	ASSERT (pred);
	ASSERT (evalContext);
	
	this -> evalContext = evalContext;
	this -> pred = pred;
}

FilterIterator::~FilterIterator () {}

int FilterIterator::initialize (TupleIterator *source)
{
	ASSERT (source);
	this -> source = source;
	return 0;
}

bool FilterIterator::getNext (Tuple &tuple)
{
	while (true) {		
		if (!source -> getNext (tuple))
			return false;
		
		evalContext -> bind (tuple, FI_SCAN_ROLE);
		
		if (pred -> eval())
			return true;
	}
	
	// never comes
	return false;
}
	
