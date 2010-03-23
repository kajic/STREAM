#ifndef _PLAN_MGR_IMPL_
#include "metadata/plan_mgr_impl.h"
#endif

#ifndef _REL_SYN_IMPL_
#include "execution/synopses/rel_syn_impl.h"
#endif

#ifndef _HASH_INDEX_
#include "execution/indexes/hash_index.h"
#endif

#ifndef _EVAL_CONTEXT_
#include "execution/internals/eval_context.h"
#endif

#ifndef _AEVAL_
#include "execution/internals/aeval.h"
#endif

#ifndef _BEVAL_
#include "execution/internals/beval.h"
#endif

#ifndef _HEVAL_
#include "execution/internals/heval.h"
#endif

#ifndef _DISTINCT_
#include "execution/operators/distinct.h"
#endif

static const unsigned int SCRATCH_ROLE = 0;
static const unsigned int CONST_ROLE = 1;
static const unsigned int SYN_ROLE = 3;
static const unsigned int INPUT_ROLE = 2;
static const unsigned int UPDATE_ROLE = 6;
static const unsigned int SCAN_ROLE = 7;
extern double INDEX_THRESHOLD;
using namespace Metadata;

using Execution::AEval;
using Execution::BEval;
using Execution::HEval;
using Execution::AInstr;
using Execution::BInstr;
using Execution::HInstr;
using Execution::EvalContext;
using Execution::RelationSynopsisImpl;
using Execution::HashIndex;
using Execution::Distinct;
using Execution::StorageAlloc;
using Execution::MemoryManager;

/// output columns
static unsigned int outCols [MAX_ATTRS];

/// Is the input a stream?
static bool bInpStr;

static int getOutputTupleLayout (Physical::Operator *op);

static int initIndex (Physical::Operator *op,
					  MemoryManager *memMgr,
					  EvalContext *evalCxt,
					  HashIndex *idx);

static int getPlusEval (Physical::Operator *op, ConstTupleLayout *ct_layout,
						AEval *&eval);

static int getMinusEval (Physical::Operator *op, ConstTupleLayout *ct_layout,
						 AEval *&eval);

static int getInitEval (Physical::Operator *op, ConstTupleLayout *ct_layout,
						AEval *&eval);

static int getEmptyEval (Physical::Operator *op, ConstTupleLayout *ct_layout,
						 BEval *&eval);

int PlanManagerImpl::inst_distinct (Physical::Operator *op)
{
	int rc;
	ConstTupleLayout *ct_layout;
	TupleLayout *tupleLayout;
	
	Distinct              *distinct;
	EvalContext           *evalContext;
	RelationSynopsisImpl  *outSyn;
	unsigned int           scanId;
	HashIndex             *idx;
	AEval                 *plusEval;
	AEval                 *minusEval;
	AEval                 *initEval;
	BEval                 *emptyEval;
	StorageAlloc          *store;
	char                  *constTuple;
	
	// Is the input a stream
	ASSERT (op -> inputs [0]);	
	bInpStr = op -> inputs [0] -> bStream;

	// Determine the output tuple layout
	if ((rc = getOutputTupleLayout (op)) != 0)
		return rc;
	
	// Create the shared evaluation context
	evalContext = new EvalContext ();

	// Const tuple layout
	ct_layout = new ConstTupleLayout();
	
	// Create the output synopsis
	ASSERT (op -> u.DISTINCT.outSyn);
	ASSERT (op -> u.DISTINCT.outSyn -> kind == REL_SYN);
	outSyn = new RelationSynopsisImpl (op -> u.DISTINCT.outSyn -> id,
									   LOG);	
	op -> u.DISTINCT.outSyn -> u.relSyn = outSyn;
	
	// Create the index for the output synopsis
	if (numIndexes >= MAX_INDEXES)
		return -1;
	idx = new HashIndex (numIndexes, LOG);
	indexes [numIndexes ++] = idx;
	
	if ((rc = initIndex (op, memMgr, evalContext, idx)) != 0)
		return rc;
	
	// Create the scan
	if ((rc = outSyn -> setIndexScan (0, idx, scanId)) != 0)
		return rc;
	if ((rc = outSyn -> setEvalContext (evalContext)) != 0)
		return rc;
	if ((rc = outSyn -> initialize ()) != 0)
		return rc;
	
	// initEval
	if ((rc = getInitEval (op, ct_layout, initEval)) != 0)
		return rc;
	if ((rc = initEval -> setEvalContext (evalContext)) != 0)
		return rc;
	
	if (!bInpStr) {
		
		// plus Eval
		if ((rc = getPlusEval (op, ct_layout, plusEval)) != 0)
			return rc;

		if ((rc = plusEval -> setEvalContext (evalContext)) != 0)
			return rc;
		
		// minusEval		
		if ((rc = getMinusEval (op, ct_layout, minusEval)) != 0)
			return rc;
		if ((rc = minusEval -> setEvalContext (evalContext)) != 0)
			return rc;
		
		// emptyEval
		if ((rc = getEmptyEval (op, ct_layout, emptyEval)) != 0)
			return rc;
		if ((rc = emptyEval -> setEvalContext (evalContext)) != 0)
			return rc;
		
	}
	
	else {
		plusEval = 0;
		minusEval = 0;
		emptyEval = 0;
	}

	// Constant tuple
	if ((rc = getStaticTuple (constTuple,
							  ct_layout -> getTupleLen ())) != 0)
		return rc;
	if ((rc = ct_layout -> genTuple (constTuple)) != 0)
		return rc;
	evalContext -> bind (constTuple, CONST_ROLE);		
	
	// Storage allocator
	ASSERT (op -> store);	
	ASSERT (op -> bStream && op -> store -> kind == WIN_STORE ||			
			!op -> bStream && op -> store -> kind == REL_STORE);

	tupleLayout = new TupleLayout (op);
	if (op -> store -> kind == WIN_STORE) {
		if ((rc = inst_win_store (op -> store, tupleLayout)) != 0)
			return rc;
	}
	else {
		if ((rc = inst_rel_store (op -> store, tupleLayout)) != 0)
			return rc;
	}

	store = op -> store -> instStore;
	ASSERT (store);
	
	delete tupleLayout;
	
	distinct = new Distinct (op -> id, LOG);
	
	if ((rc = distinct -> setOutputSynopsis (outSyn, scanId)) != 0)
		return rc;

	if ((rc = distinct -> setEvalContext (evalContext)) != 0)
		return rc;
	
	if ((rc = distinct -> setPlusEvaluator (plusEval)) != 0)
		return rc;
	
	if ((rc = distinct -> setMinusEvaluator (minusEval)) != 0)
		return rc;
	
	if ((rc = distinct -> setInitEvaluator (initEval)) != 0)
		return rc;
	
	if ((rc = distinct -> setEmptyEvaluator (emptyEval)) != 0)
		return rc;
	
	if ((rc = distinct -> setOutStore (store)) != 0)
		return rc;
	
	op -> instOp = distinct;

	delete ct_layout;
	
	return 0;
}
	
static int getOutputTupleLayout (Physical::Operator *op)
{
	TupleLayout *tupleLayout;

	tupleLayout = new TupleLayout (op);

	for (unsigned int a = 0 ; a < op -> numAttrs ; a++)
		outCols [a] = tupleLayout -> getColumn (a);

	delete tupleLayout;
	return 0;
}

static int initIndex (Physical::Operator *op,
					  MemoryManager *memMgr,
					  EvalContext *evalContext,
					  HashIndex *idx)
{
	int rc;
	HEval *updateHash, *scanHash;
	HInstr hinstr;
	BEval *keyEqual;
	BInstr binstr;
	unsigned int numDataAttrs;

	numDataAttrs = (bInpStr)? op -> numAttrs : op -> numAttrs - 1;
	
	updateHash = new HEval ();
	for (unsigned int a = 0 ; a < numDataAttrs ; a++) {

		hinstr.type = op -> attrTypes [a];
		hinstr.r = UPDATE_ROLE;
		hinstr.c = outCols [a];

		if ((rc = updateHash -> addInstr (hinstr)) != 0)
			return rc;		
	}

	scanHash = new HEval ();
	for (unsigned int a = 0 ; a < numDataAttrs ; a++) {

		hinstr.type = op -> attrTypes [a];
		hinstr.r = INPUT_ROLE;
		hinstr.c = outCols [a];

		if ((rc = scanHash -> addInstr (hinstr)) != 0)
			return rc;
	}

	keyEqual = new BEval ();
	for (unsigned int a = 0 ; a < numDataAttrs ; a++) {

		switch (op -> attrTypes [a]) {
		case INT:    binstr.op = Execution::INT_EQ; break;
		case FLOAT:  binstr.op = Execution::FLT_EQ; break;
		case CHAR:   binstr.op = Execution::CHR_EQ; break;
		case BYTE:   binstr.op = Execution::BYT_EQ; break;

#ifdef _DM_
		default:
			ASSERT (0);
#endif
		}

		binstr.r1 = INPUT_ROLE;
		binstr.c1 = outCols [a];
		binstr.e1 = 0;
		
		binstr.r2 = SCAN_ROLE;
		binstr.c2 = outCols [a];
		binstr.e2 = 0;
		
		if ((rc = keyEqual -> addInstr (binstr)) != 0)
			return rc;
	}

	if ((rc = updateHash -> setEvalContext (evalContext)) != 0)
		return rc;

	if ((rc = scanHash -> setEvalContext (evalContext)) != 0)
		return rc;

	if ((rc = keyEqual -> setEvalContext (evalContext)) != 0)
		return rc;
	
	if ((rc = idx -> setMemoryManager (memMgr)) != 0)
		return rc;
	if ((rc = idx -> setEvalContext (evalContext)) != 0)
		return rc;
	if ((rc = idx -> setUpdateHashEval (updateHash)) != 0)
		return rc;
	if ((rc = idx -> setScanHashEval (scanHash)) != 0)
		return rc;
	if ((rc = idx -> setKeyEqual (keyEqual)) != 0)
		return rc;
	if ((rc = idx -> setThreshold (INDEX_THRESHOLD)) != 0)
		return rc;
	if ((rc = idx -> initialize ()) != 0)
		return rc;
	
	return 0;
}

static int getPlusEval (Physical::Operator *op, ConstTupleLayout *ct_layout,
						AEval *&eval)
{
	int rc;
	AInstr instr;
	
	eval = new AEval ();
	
	instr.op = Execution::INT_ADD;
	instr.r1 = SYN_ROLE;
	instr.c1 = outCols [op -> numAttrs - 1];
	
	instr.r2 = CONST_ROLE;
	if ((rc = ct_layout -> addInt (1, instr.c2)) != 0)
		return rc;
	
	instr.dr = SYN_ROLE;
	instr.dc = outCols [op -> numAttrs - 1];
	
	if ((rc = eval -> addInstr (instr)) != 0)
		return rc;

	return 0;
}

static int getMinusEval (Physical::Operator *op, ConstTupleLayout *ct_layout,
						 AEval *&eval)
{	
	int rc;
	AInstr instr;
	
	eval = new AEval ();
	
	instr.op = Execution::INT_SUB;
	instr.r1 = SYN_ROLE;
	instr.c1 = outCols [op -> numAttrs - 1];
	
	instr.r2 = CONST_ROLE;
	if ((rc = ct_layout -> addInt (1, instr.c2)) != 0)
		return rc;
	
	instr.dr = SYN_ROLE;
	instr.dc = outCols [op -> numAttrs - 1];
	
	if ((rc = eval -> addInstr (instr)) != 0)
		return rc;
	
	return 0;
}

static int getInitEval (Physical::Operator *op, ConstTupleLayout *ct_layout,
						AEval *&eval)
{
	int rc;
	AInstr instr;
	unsigned int numExtAttrs;
	
	eval = new AEval ();

	numExtAttrs = (bInpStr)? op -> numAttrs : op -> numAttrs - 1;

	// Copy the "external attributes" (attributes other than the optional
	// count we maintain in the synopsis)
	for (unsigned int a = 0 ; a < numExtAttrs ; a++) {

		// Operation: copy
		switch (op -> attrTypes [a]) {
		case INT:    instr.op = Execution::INT_CPY; break;
		case FLOAT:  instr.op = Execution::FLT_CPY; break;
		case CHAR:   instr.op = Execution::CHR_CPY; break;
		case BYTE:   instr.op = Execution::BYT_CPY; break;			
			
#ifdef _DM_
		default:
			ASSERT (0);
#endif
			
		}
		
		// Source: input tuple
		instr.r1 = INPUT_ROLE;
		instr.c1 = outCols [a];

		// Destn: synopsis tuple
		instr.dr = SYN_ROLE;
		instr.dc = outCols [a];
		
		if ((rc = eval -> addInstr (instr)) != 0)
			return rc;
	}

	// If we maintain a count (bInpStr != true), initialize the count to 1
	if (!bInpStr) {
	
		instr.op = Execution::INT_CPY;	
	   
		instr.r1 = CONST_ROLE;
		if ((rc = ct_layout -> addInt (1, instr.c1)) != 0)
			return rc;	
		
		instr.dr = SYN_ROLE;
		instr.dc = outCols [op -> numAttrs - 1];
		
		if ((rc = eval -> addInstr (instr)) != 0)
			return rc;
	}
	
	return 0;
}

static int getEmptyEval (Physical::Operator *op, ConstTupleLayout *ct_layout,
						 BEval *&eval)
{
	int rc;
	BInstr instr;

	eval = new BEval ();

	instr.op = Execution::INT_EQ;
	
	instr.r1 = SYN_ROLE;
	instr.c1 = outCols [op -> numAttrs - 1];
	instr.e1 = 0;
	
	instr.r2 = CONST_ROLE;
	if ((rc = ct_layout -> addInt (1, instr.c2)) != 0)
		return rc;
	instr.e2 = 0;
	
	if ((rc = eval -> addInstr (instr)) != 0)
		return rc;
	
	return 0;
}
