#ifndef _PLAN_MGR_IMPL_
#include "metadata/plan_mgr_impl.h"
#endif

#ifndef _TUPLE_
#include "execution/internals/tuple.h"
#endif

#ifndef _PROJECT_
#include "execution/operators/project.h"
#endif

#ifndef _LIN_SYN_IMPL_
#include "execution/synopses/lin_syn_impl.h"
#endif

// Note: has to be consistent with Project.h
static const unsigned int INPUT_ROLE = 2;

// Note: has to be consistent with Project.h
static const unsigned int OUTPUT_ROLE = 3;

using namespace Metadata;

using Execution::Project;
using Execution::AEval;
using Execution::EvalContext;
using Execution::Tuple;
using Execution::LineageSynopsisImpl;
using Execution::StorageAlloc;

int PlanManagerImpl::inst_project (Physical::Operator *op)
{
	int rc;
	unsigned int         roleMap;
	EvalContextInfo      evalCxt;
	unsigned int         st_size;
	unsigned int         ct_size;			
	unsigned int         outCol;
	char                *constTuple;
	char                *scratchTuple;
	TupleLayout         *tupleLayout;
	
	Project             *project;
	AEval               *outEval;
	EvalContext         *evalContext;
	LineageSynopsisImpl *outSyn;
	StorageAlloc        *store;
	
	ASSERT (op -> kind == PO_PROJECT);
	ASSERT (op -> instOp == 0);
	
	evalCxt.st_layout = new TupleLayout();
	evalCxt.ct_layout = new ConstTupleLayout ();
	
	// Tuple layout of the output tuples
	tupleLayout = new TupleLayout (op);	
	
	// Create the evaluation context
	evalContext = new EvalContext ();
	
	// Create the evaluator that produces the output tuples from input
	roleMap = INPUT_ROLE;
	outEval = 0;
	for (unsigned int a = 0 ; a < op -> numAttrs ; a++) {
		
		outCol = tupleLayout -> getColumn (a);
		
		// Update the evaluator to produce the value for the column
		if ((rc = inst_expr_dest (op -> u.PROJECT.projs [a], &roleMap, 
								  op, OUTPUT_ROLE, outCol,
								  outEval, evalCxt)) != 0)
			return rc;
	}	
	if ((rc = outEval -> setEvalContext (evalContext)) != 0)
		return rc;
	
	// Scratch tuple
	st_size = evalCxt.st_layout -> getTupleLen ();
	if (st_size > 0) {		
		if ((rc = getStaticTuple (scratchTuple, st_size)) != 0)
			return rc;	
		evalContext -> bind (scratchTuple, SCRATCH_ROLE);
	}
	
	// Constant tuple
	ct_size = evalCxt.ct_layout -> getTupleLen ();
	if (ct_size > 0) {
		if ((rc = getStaticTuple (constTuple, ct_size)) != 0)
			return rc;		
		evalContext -> bind (constTuple, CONST_ROLE);
		
		if ((rc = evalCxt.ct_layout -> genTuple (constTuple)) != 0)
			return rc;
	}
	

	// Create output synopsis if necessary
	outSyn = 0;
	if (!op -> bStream) {		
		
		ASSERT (op -> u.PROJECT.outSyn);
		ASSERT (op -> u.PROJECT.outSyn -> store == op -> store);		
		ASSERT (op -> u.PROJECT.outSyn -> kind == LIN_SYN);
		
		outSyn = new LineageSynopsisImpl (op -> u.PROJECT.outSyn -> id,
										  LOG);
		op -> u.PROJECT.outSyn -> u.linSyn = outSyn;
	}
	
	// Storeage allocator
	ASSERT (op -> store);
	ASSERT (op -> bStream && (op -> store -> kind == SIMPLE_STORE ||
							  op -> store -> kind == WIN_STORE)
			||
			!op -> bStream && op -> store -> kind == LIN_STORE);	
	if (op -> store -> kind == LIN_STORE) {		
		if ((rc = inst_lin_store (op -> store, tupleLayout)) != 0)
			return rc;
		store = op -> store -> instStore;		
	}	
	else if (op -> store -> kind == SIMPLE_STORE) {
		if ((rc = inst_simple_store (op -> store, tupleLayout)) != 0)
			return rc;
		store = op -> store -> instStore;
	}
	else {
		if ((rc = inst_win_store (op -> store, tupleLayout)) != 0)
			return rc;
		store = op -> store -> instStore;
	}
	
	// Create a new project operator
	project = new Project (op -> id, LOG);
	
	if ((rc = project -> setEvalContext (evalContext)) != 0)
		return rc;

	if ((rc = project -> setOutSynopsis (outSyn)) != 0)
		return rc;

	if ((rc = project -> setProjEvaluator (outEval)) != 0)
		return rc;

	if ((rc = project -> setOutStore (store)) != 0)
		return rc;	
	
	op -> instOp = project;
	
	delete evalCxt.st_layout;
	delete evalCxt.ct_layout;
	delete tupleLayout;
	
	return 0;
}
