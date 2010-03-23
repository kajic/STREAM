#ifndef _PLAN_MGR_IMPL_
#include "metadata/plan_mgr_impl.h"
#endif

#ifndef _TUPLE_
#include "execution/internals/tuple.h"
#endif

#ifndef _SELECT_
#include "execution/operators/select.h"
#endif

using namespace Metadata;

using Execution::Select;
using Execution::BEval;
using Execution::EvalContext;
using Execution::Tuple;

static const unsigned int INPUT_CONTEXT = 2;

int PlanManagerImpl::inst_select (Physical::Operator *op)
{
	int rc;
	unsigned int     roleMap;
	EvalContextInfo  evalCxt;
	unsigned int     scratchTupleSize;
	unsigned int     constTupleSize;
	char            *scratchTuple;
	char            *constTuple;
	
	Select          *select;
	BEval           *pred;
	EvalContext     *evalContext;
	
	ASSERT (op -> kind == PO_SELECT);
	ASSERT (op -> instOp == 0);
	ASSERT (op -> u.SELECT.pred);

	// Evaluation context
	evalContext = new EvalContext ();
	
	// Selection predicate
	roleMap = INPUT_CONTEXT;
	evalCxt.st_layout = new TupleLayout();
	evalCxt.ct_layout = new ConstTupleLayout ();

	pred = 0;
	if ((rc = inst_bexpr (op -> u.SELECT.pred, &roleMap, op,
						  pred, evalCxt)) != 0)
		return rc;
	if ((rc = pred -> setEvalContext (evalContext)) != 0)
		return rc;
	
	// Scratch tuple
	scratchTupleSize = evalCxt.st_layout -> getTupleLen();
	if (scratchTupleSize > 0) {
		if ((rc = getStaticTuple (scratchTuple, scratchTupleSize)) != 0)
			return rc;
		evalContext -> bind (scratchTuple, SCRATCH_ROLE);
	}
	
	// Constant tuple
	constTupleSize = evalCxt.ct_layout -> getTupleLen ();
	if (constTupleSize > 0) {
		if ((rc = getStaticTuple (constTuple, constTupleSize)) != 0)
			return rc;
		evalContext -> bind (constTuple, CONST_ROLE);
		
		if ((rc = evalCxt.ct_layout -> genTuple (constTuple)) != 0)
			return rc;
	}
	
	// Create a new selection operator.
	select = new Select (op -> id, LOG);
	
	if ((rc = select -> setPredicate (pred)) != 0)
		return rc;
	
	if ((rc = select -> setEvalContext (evalContext)) != 0)
		return rc;
	
	op -> instOp = select;
	
	delete evalCxt.st_layout;
	delete evalCxt.ct_layout;
	
	return 0;
}

	
