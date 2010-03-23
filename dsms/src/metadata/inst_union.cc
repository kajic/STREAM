#ifndef _PLAN_MGR_IMPL_
#include "metadata/plan_mgr_impl.h"
#endif

#ifndef _LIN_SYN_IMPL_
#include "execution/synopses/lin_syn_impl.h"
#endif

#ifndef _UNION_
#include "execution/operators/union.h"
#endif

static const unsigned int LEFT_ROLE = 2;
static const unsigned int RIGHT_ROLE = 3;
static const unsigned int OUTPUT_ROLE = 4;

using namespace Metadata;

using Physical::Operator;
using Execution::Union;
using Execution::AEval;
using Execution::AInstr;
using Execution::EvalContext;
using Execution::LineageSynopsisImpl;
using Execution::StorageAlloc;

static int getLeftOutEval (Operator *op, AEval *&eval);
static int getRightOutEval (Operator *op, AEval *&eval);

int PlanManagerImpl::inst_union (Physical::Operator *op)
{
	int rc;
	
	Union                 *unionOp;
	StorageAlloc          *outStore;
	LineageSynopsisImpl   *outSyn;
	EvalContext           *evalContext;
	AEval                 *leftOutEval;
	AEval                 *rightOutEval;

	TupleLayout       *tupleLayout;
	
	// Determine input & output tuple layout
	tupleLayout = new TupleLayout (op);
	
	// Evaluation context
	evalContext = new EvalContext ();
	
	// Output evaluators
	if ((rc = getLeftOutEval (op, leftOutEval)) != 0)
		return rc;
	if ((rc = leftOutEval -> setEvalContext (evalContext)) != 0)
		return rc;

	if ((rc = getRightOutEval (op, rightOutEval)) != 0)
		return rc;
	if ((rc = rightOutEval -> setEvalContext (evalContext)) != 0)
		return rc;
	
	// Output storage allocator	
	ASSERT (op -> store);
	ASSERT (op -> bStream && (op -> store -> kind == SIMPLE_STORE ||
							  op -> store -> kind == WIN_STORE)
			||
			!op -> bStream && op -> store -> kind == LIN_STORE);
	
	if (op -> store -> kind == LIN_STORE) {
		if ((rc = inst_lin_store (op -> store, tupleLayout)) != 0)
			return rc;
	}
	else if (op -> store -> kind == SIMPLE_STORE) {
		if ((rc = inst_simple_store (op -> store, tupleLayout)) != 0)
			return rc;
	}
	else {
		if ((rc = inst_win_store (op -> store, tupleLayout)) != 0)
			return rc;
	}
	outStore = op -> store -> instStore;	
	
	// Output synopsis
	outSyn = 0;
	if (!op -> bStream) {
		
		ASSERT (op -> u.UNION.outSyn);
		ASSERT (op -> u.UNION.outSyn -> kind == LIN_SYN);
		ASSERT (op -> u.UNION.outSyn -> store == op -> store);
		
		outSyn = new LineageSynopsisImpl (op -> u.UNION.outSyn -> id,
										  LOG);	 
		op -> u.UNION.outSyn -> u.linSyn = outSyn;
	}

	// Union operator
	unionOp = new Union (op -> id, LOG);

	if ((rc = unionOp -> setOutStore (outStore)) != 0)
		return rc;
	if ((rc = unionOp -> setOutSyn (outSyn)) != 0)
		return rc;
	if ((rc = unionOp -> setEvalContext (evalContext)) != 0)
		return rc;
	if ((rc = unionOp -> setLeftOutEval (leftOutEval)) != 0)		
		return rc;
	if ((rc = unionOp -> setRightOutEval (rightOutEval)) != 0)
		return rc;
	
	op -> instOp = unionOp;
	
	delete tupleLayout;	
	return 0;
}
	
static int getLeftOutEval (Operator *op, AEval *&eval)
{
	int rc;
	AInstr instr;
	TupleLayout *tupleLayout;
	TupleLayout *leftTupleLayout;

	tupleLayout = new TupleLayout (op);
	leftTupleLayout = new TupleLayout (op -> inputs [0]);
	
	eval = new AEval ();	
	for (unsigned int a = 0 ; a < op -> numAttrs ; a++) {
		
		// Operation
		switch (op -> attrTypes [a]) {
		case INT:   instr.op = Execution::INT_CPY; break;
		case FLOAT: instr.op = Execution::FLT_CPY; break;
		case BYTE:  instr.op = Execution::BYT_CPY; break;
		case CHAR:  instr.op = Execution::CHR_CPY; break;				
		default:
			ASSERT (0); break;
		}
		
		// Source
		instr.r1 = LEFT_ROLE;
		instr.c1 = leftTupleLayout -> getColumn (a);
		
		// Destn.
		instr.dr = OUTPUT_ROLE;
		instr.dc = tupleLayout -> getColumn (a);
		
		if ((rc = eval -> addInstr (instr)) != 0)
			return rc;		
	}

	delete tupleLayout;
	delete leftTupleLayout;
	
	return 0;	
}

static int getRightOutEval (Operator *op, AEval *&eval)
{
	int rc;
	AInstr instr;
	TupleLayout *tupleLayout;
	TupleLayout *rightTupleLayout;
	
	tupleLayout = new TupleLayout (op);
	rightTupleLayout = new TupleLayout (op -> inputs [1]);
	
	eval = new AEval ();	
	for (unsigned int a = 0 ; a < op -> numAttrs ; a++) {
		
		// Operation
		switch (op -> attrTypes [a]) {
		case INT:   instr.op = Execution::INT_CPY; break;
		case FLOAT: instr.op = Execution::FLT_CPY; break;
		case BYTE:  instr.op = Execution::BYT_CPY; break;
		case CHAR:  instr.op = Execution::CHR_CPY; break;				
		default:
			ASSERT (0); break;
		}
		
		// Source
		instr.r1 = RIGHT_ROLE;
		instr.c1 = rightTupleLayout -> getColumn (a);
		
		// Destn.
		instr.dr = OUTPUT_ROLE;
		instr.dc = tupleLayout -> getColumn (a);
		
		if ((rc = eval -> addInstr (instr)) != 0)
			return rc;		
	}
	
	delete tupleLayout;
	delete rightTupleLayout;
	
	return 0;	
}
