#ifndef _PLAN_MGR_IMPL_
#include "metadata/plan_mgr_impl.h"
#endif

#ifndef _PARTN_WIN_
#include "execution/operators/partn_win.h"
#endif

#ifndef _AEVAL_
#include "execution/internals/aeval.h"
#endif

#ifndef _PARTN_WIN_SYN_IMPL_
#include "execution/synopses/partn_win_syn_impl.h"
#endif

using namespace Metadata;

using Execution::PartnWindow;
using Execution::PartnWindowSynopsisImpl;
using Execution::EvalContext;
using Execution::AEval;
using Execution::AInstr;
using Execution::StorageAlloc;

static const unsigned int INPUT_ROLE = 2;
static const unsigned int COPY_ROLE = 3;

static int getCopyEval (Physical::Operator *op, AEval *&eval);

int PlanManagerImpl::inst_pwin (Physical::Operator *op)
{
	int rc;

	PartnWindow *pwin;
	PartnWindowSynopsisImpl *winSyn;
	EvalContext *evalContext;
	AEval *copyEval;
	StorageAlloc *store;
	
	ASSERT (op);
	ASSERT (op -> kind == PO_PARTN_WIN);
	
	// Eval context
	evalContext = new EvalContext ();
	
	// Copy evaluator
	if ((rc = getCopyEval (op, copyEval)) != 0)
		return rc;
	
	if ((rc = copyEval -> setEvalContext (evalContext)) != 0)
		return rc;	
	
	// Create the window synopsis
	ASSERT (op -> u.PARTN_WIN.winSyn);
	ASSERT (op -> u.PARTN_WIN.winSyn -> kind == PARTN_WIN_SYN);
	
	winSyn = new PartnWindowSynopsisImpl (op -> u.PARTN_WIN.winSyn -> id,
										  LOG);
	op -> u.PARTN_WIN.winSyn -> u.pwinSyn = winSyn;

	ASSERT (op -> store);
	ASSERT (op -> store -> kind == PARTN_WIN_STORE);

	if ((rc = inst_pwin_store (op -> store)) != 0)
		return rc;
	store = op -> store -> instStore;
	ASSERT (store);

	// Create the partn windoe operator
	pwin = new PartnWindow (op -> id, LOG);
	
	// Initialize		
	if ((rc = pwin -> setWindowSize (op -> u.PARTN_WIN.numRows)) != 0)
		return rc;
	
	if ((rc = pwin -> setEvalContext (evalContext)) != 0)
		return rc;
	
	if ((rc = pwin -> setCopyEval (copyEval)) != 0)
		return rc;
	
	if ((rc = pwin -> setWindowSynopsis (winSyn)) != 0)
		return rc;

	if ((rc = pwin -> setOutStore (store)) != 0)
		return rc;
	
	op -> instOp = pwin;
	
	return 0;
}
	
static int getCopyEval (Physical::Operator *op, AEval *&eval)
{
	int rc;
	AInstr instr;
	TupleLayout *tupleLayout;

	tupleLayout = new TupleLayout (op);

	eval = new AEval ();
	for (unsigned int a = 0 ; a < op -> numAttrs ; a++) {

		// Operation
		switch (op -> attrTypes [a]) {
		case INT:     instr.op = Execution::INT_CPY; break;
		case FLOAT:   instr.op = Execution::FLT_CPY; break;
		case CHAR:    instr.op = Execution::CHR_CPY; break;	 
		case BYTE:    instr.op = Execution::BYT_CPY; break;			
		default:
			ASSERT (0);
			break;
		}

		instr.r1 = INPUT_ROLE;
		instr.c1 = tupleLayout -> getColumn (a);

		instr.dr = COPY_ROLE;
		instr.dc = tupleLayout -> getColumn (a);

		if ((rc = eval -> addInstr (instr)) != 0)
			return rc;		
	}

	delete tupleLayout;
	return 0;
}
