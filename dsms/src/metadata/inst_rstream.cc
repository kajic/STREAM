#ifndef _PLAN_MGR_IMPL_
#include "metadata/plan_mgr_impl.h"
#endif

#ifndef _AEVAL_
#include "execution/internals/aeval.h"
#endif

#ifndef _REL_SYN_IMPL_
#include "execution/synopses/rel_syn_impl.h"
#endif

#ifndef _RSTREAM_
#include "execution/operators/rstream.h"
#endif

using namespace Metadata;

using Execution::EvalContext;
using Execution::AEval;
using Execution::AInstr;
using Execution::RelationSynopsisImpl;
using Execution::Rstream;
using Execution::StorageAlloc;

static const unsigned int INPUT_ROLE = 2;
static const unsigned int OUTPUT_ROLE = 3;

static int getCopyEval (Physical::Operator *op, AEval *&eval);

int PlanManagerImpl::inst_rstream (Physical::Operator *op)
{
	int rc;

	TupleLayout *tupleLayout;
	EvalContext *evalContext;
	AEval *copyEval;	
	RelationSynopsisImpl *inSyn;
	unsigned int scanId;
	Rstream *rstream;
	StorageAlloc *outStore;
	
	evalContext = new EvalContext ();

	// evaluator that copies the input tuple to an output tuple
	if ((rc = getCopyEval (op, copyEval)) != 0)
		return rc;
	if ((rc = copyEval -> setEvalContext (evalContext)) != 0)
		return rc;	
	
	// Create the input synopsis
	ASSERT (op -> u.RSTREAM.inSyn);
	ASSERT (op -> u.RSTREAM.inSyn -> kind == REL_SYN);
	
	inSyn = new RelationSynopsisImpl (op -> u.RSTREAM.inSyn -> id,
									  LOG);
	op -> u.RSTREAM.inSyn -> u.relSyn = inSyn;

	// Full scan of the input synopsis
	if ((rc = inSyn -> setScan (0, scanId)) != 0)
		return rc;
	if ((rc = inSyn -> setEvalContext (evalContext)) != 0)
		return rc;
	if ((rc = inSyn -> initialize ()) != 0)
		return rc;
	
	// Output store
	tupleLayout = new TupleLayout (op);
	
	ASSERT (op -> store -> kind == SIMPLE_STORE ||
			op -> store -> kind == WIN_STORE);

	if (op -> store -> kind == SIMPLE_STORE) {
		if ((rc = inst_simple_store (op -> store, tupleLayout)) != 0)
			return rc;
	}
	else {
		if ((rc = inst_win_store (op -> store, tupleLayout)) != 0)
			return rc;
	}	
	outStore = op -> store -> instStore;
	ASSERT (outStore);
	
	// Create & initialize the rstream operator
	rstream = new Rstream (op -> id, LOG);
	
	if ((rc = rstream -> setSynopsis (inSyn)) != 0)
		return rc;
	
	if ((rc = rstream -> setScan (scanId)) != 0)
		return rc;

	if ((rc = rstream -> setEvalContext (evalContext)) != 0)
		return rc;

	if ((rc = rstream -> setCopyEval (copyEval)) != 0)
		return rc;

	if ((rc = rstream -> setOutStore (outStore)) != 0)
		return rc;
	
	op -> instOp = rstream;

	delete tupleLayout;
	
	return 0;
}

static int getCopyEval (Physical::Operator *op, AEval *&eval)
{
	int rc;
	AInstr instr;
	TupleLayout *tupleLayout;
	
	eval = new AEval ();
	tupleLayout = new TupleLayout (op);

	for (unsigned int a = 0 ; a < op -> numAttrs ; a++) {

		switch (op -> attrTypes [a]) {
		case INT:   instr.op = Execution::INT_CPY; break;
		case FLOAT: instr.op = Execution::FLT_CPY; break;
		case CHAR:  instr.op = Execution::CHR_CPY; break;
		case BYTE:  instr.op = Execution::BYT_CPY; break;
			
#ifdef _DM_
		default:
			ASSERT (0);
			break;
#endif
		}
		
		// source
		instr.r1 = INPUT_ROLE;
		instr.c1 = tupleLayout -> getColumn (a);
		
		// dest
		instr.dr = OUTPUT_ROLE;
		instr.dc = tupleLayout -> getColumn (a);

		if ((rc = eval -> addInstr (instr)) != 0)
			return rc;
	}

	delete tupleLayout;
	
	return 0;
}
	
