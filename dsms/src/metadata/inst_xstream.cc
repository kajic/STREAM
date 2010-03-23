#ifndef _PLAN_MGR_IMPL_
#include "metadata/plan_mgr_impl.h"
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

#ifndef _REL_SYN_IMPL_
#include "execution/synopses/rel_syn_impl.h"
#endif

#ifndef _HASH_INDEX_
#include "execution/indexes/hash_index.h"
#endif

#ifndef _ISTREAM_
#include "execution/operators/istream.h"
#endif

#ifndef _DSTREAM_
#include "execution/operators/dstream.h"
#endif

static const unsigned int CONST_ROLE = 1;
static const unsigned int INPUT_ROLE = 2;
static const unsigned int SYN_ROLE = 3;
static const unsigned int OUTPUT_ROLE = 4;

static const unsigned int UPDATE_ROLE = 6;
static const unsigned int SCAN_ROLE = 7;
extern double INDEX_THRESHOLD;

using namespace Metadata;

using Execution::EvalContext;
using Execution::AEval;
using Execution::BEval;
using Execution::HEval;
using Execution::AInstr;
using Execution::BInstr;
using Execution::HInstr;
using Execution::RelationSynopsisImpl;
using Execution::HashIndex;
using Execution::Istream;
using Execution::Dstream;
using Execution::StorageAlloc;
using Execution::MemoryManager;

/// Column of the count attribute in the count synopsis
static unsigned int countCol;

/// Column of the external attributes
static unsigned int dataCols [MAX_ATTRS];

static int getTupleLayout (Physical::Operator *op);

static int getIncrEval (Physical::Operator *op, ConstTupleLayout *ct_layout,
						AEval *&eval);

static int getDecrEval (Physical::Operator *op, ConstTupleLayout *ct_layout,
						AEval *&eval);

static int getInitEval (Physical::Operator *op, ConstTupleLayout *ct_layout,
						AEval *&eval);

static int getPosEval (Physical::Operator *op, ConstTupleLayout *ct_layout,
					   BEval *&eval);

static int getNegEval (Physical::Operator *op, ConstTupleLayout *ct_layout,
					   BEval *&eval);

static int getZeroEval (Physical::Operator *op, ConstTupleLayout *ct_layout,
						BEval *&eval);

static int getOutEval (Physical::Operator *op, ConstTupleLayout *ct_layout,
					   AEval *&eval);

static int initCountIndex (Physical::Operator *op,
						   MemoryManager *memMgr,
						   EvalContext *evalContext,
						   HashIndex *idx);

int PlanManagerImpl::inst_istream (Physical::Operator *op)
{
	int rc;

	TupleLayout *tupleLayout;
	EvalContext *evalContext;
	ConstTupleLayout *ct_layout;
	AEval *incrEval, *decrEval, *initEval, *outEval;
	BEval *zeroEval, *posEval;

	RelationSynopsisImpl *countSyn;
	HashIndex *countIdx;
	unsigned int countScanId, fullScanId;
	
	Istream *istream;	
	StorageAlloc *outStore;
	StorageAlloc *synStore;
	char *constTuple;
	
	// Get the layout of the input, output, & count synopsis tuples
	if ((rc = getTupleLayout (op)) != 0)
		return rc;
	
	// Eval context
	evalContext = new EvalContext ();

	// Layout of constants used by the operator
	ct_layout = new ConstTupleLayout ();

	// Count incrementor
	if ((rc = getIncrEval (op, ct_layout, incrEval)) != 0)
		return rc;
	if ((rc = incrEval -> setEvalContext (evalContext)) != 0)
		return rc;
	
	// Count decrementor
	if ((rc = getDecrEval (op, ct_layout, decrEval)) != 0)
		return rc;
	if ((rc = decrEval -> setEvalContext (evalContext)) != 0)
		return rc;
	
	// Count initializer (to 1)
	if ((rc = getInitEval (op, ct_layout, initEval)) != 0)
		return rc;
	if ((rc = initEval -> setEvalContext (evalContext)) != 0)
		return rc;

	// Check if count is positive
	if ((rc = getPosEval (op, ct_layout, posEval)) != 0)
		return rc;
	if ((rc = posEval -> setEvalContext (evalContext)) != 0)
		return rc;

	// Check if count == 0
	if ((rc = getZeroEval (op, ct_layout, zeroEval)) != 0)
		return rc;
	if ((rc = zeroEval -> setEvalContext (evalContext)) != 0)
		return rc;	
	
	// Count synopsis
	ASSERT (op -> u.ISTREAM.nowSyn);
	ASSERT (op -> u.ISTREAM.nowSyn -> kind == REL_SYN);
	
	countSyn = new RelationSynopsisImpl (op -> u.ISTREAM.nowSyn -> id,
										 LOG);
	op -> u.ISTREAM.nowSyn -> u.relSyn = countSyn;
	
	// Index for looking up count synopsis
	if (numIndexes >= MAX_INDEXES)
		return -1;
	countIdx = new HashIndex (numIndexes, LOG);
	indexes [numIndexes++] = countIdx;

	// Initialize the count index
	if ((rc = initCountIndex (op, memMgr, evalContext, countIdx)) != 0)
		return rc;

	// Scan for count of a particular tuple
	if ((rc = countSyn -> setIndexScan (0, countIdx, countScanId)) != 0)
		return rc;

	// Full scan
	if ((rc = countSyn -> setScan (0, fullScanId)) != 0)
		return rc;
	
	if ((rc = countSyn -> setEvalContext (evalContext)) != 0)
		return rc;
	
	if ((rc = countSyn -> initialize ()) != 0)
		return rc;
	
	// Construct output tuple
	if ((rc = getOutEval (op, ct_layout, outEval)) != 0)
		return rc;
	if ((rc = outEval -> setEvalContext (evalContext)) != 0)
		return rc;
	
	// Output store
	ASSERT (op -> store);
	ASSERT (op -> store -> kind == SIMPLE_STORE ||
			op -> store -> kind == WIN_STORE);

	tupleLayout = new TupleLayout(op);
	if (op -> store -> kind == SIMPLE_STORE) {
		if ((rc = inst_simple_store (op -> store, tupleLayout)) != 0)
			return rc;
	}
	else {
		if ((rc = inst_win_store (op -> store, tupleLayout)) != 0)
			return rc;
	}
	outStore = op -> store -> instStore;
	
	// Synopsis store: contains an additional column - count
	ASSERT (op -> u.ISTREAM.nowStore);
	ASSERT (op -> u.ISTREAM.nowStore -> kind == REL_STORE);
	
	unsigned int cc;
	if ((rc = tupleLayout -> addFixedLenAttr (INT, cc)) != 0)
		return rc;
	if ((rc = inst_rel_store (op -> u.ISTREAM.nowStore, tupleLayout)) != 0)
		return rc;
	synStore = op -> u.ISTREAM.nowStore -> instStore;

	// Const tuple
	if ((rc = getStaticTuple (constTuple,
							  ct_layout -> getTupleLen ())) != 0)
		return rc;
	if ((rc = ct_layout -> genTuple (constTuple)) != 0)
		return rc;
	evalContext -> bind (constTuple, CONST_ROLE);
	
	// Construct the Istream object
	istream = new Istream (op -> id, LOG);

	if ((rc = istream -> setSynopsis (countSyn)) != 0)
		return rc;

	if ((rc = istream -> setCountScan (countScanId)) != 0)
		return rc;

	if ((rc = istream -> setFullScan (fullScanId)) != 0)
		return rc;

	if ((rc = istream -> setEvalContext (evalContext)) != 0)
		return rc;

	if ((rc = istream -> setIncrEval (incrEval)) != 0)
		return rc;

	if ((rc = istream -> setDecrEval (decrEval)) != 0)
		return rc;

	if ((rc = istream -> setInitEval (initEval)) != 0)
		return rc;

	if ((rc = istream -> setZeroEval (zeroEval)) != 0)
		return rc;

	if ((rc = istream -> setPosEval (posEval)) != 0)
		return rc;

	if ((rc = istream -> setOutEval (outEval)) != 0)
		return rc;
	
	if ((rc = istream -> setOutStore (outStore)) != 0)
		return rc;

	if ((rc = istream -> setSynStore (synStore)) != 0)
		return rc;
	
	op -> instOp = istream;	
	
	delete ct_layout;
	delete tupleLayout;
	
	return 0;	
}

int PlanManagerImpl::inst_dstream (Physical::Operator *op)
{
	int rc;

	TupleLayout *tupleLayout;
	EvalContext *evalContext;
	ConstTupleLayout *ct_layout;
	AEval *incrEval, *decrEval, *initEval, *outEval;
	BEval *zeroEval, *negEval;
	
	RelationSynopsisImpl *countSyn;
	HashIndex *countIdx;
	unsigned int countScanId, fullScanId;
	
	Dstream *dstream;	
	StorageAlloc *outStore;
	StorageAlloc *synStore;
	char *constTuple;
	
	// Get the layout of the input, output, & count synopsis tuples
	if ((rc = getTupleLayout (op)) != 0)
		return rc;
	
	// Eval context
	evalContext = new EvalContext ();
	
	// Layout of constants used by the operator
	ct_layout = new ConstTupleLayout ();
	
	// Count incrementor
	if ((rc = getIncrEval (op, ct_layout, incrEval)) != 0)
		return rc;
	if ((rc = incrEval -> setEvalContext (evalContext)) != 0)
		return rc;
		
	// Count decrementor
	if ((rc = getDecrEval (op, ct_layout, decrEval)) != 0)
		return rc;
	if ((rc = decrEval -> setEvalContext (evalContext)) != 0)
		return rc;
	
	// Count initializer (to 1)
	if ((rc = getInitEval (op, ct_layout, initEval)) != 0)
		return rc;
	if ((rc = initEval -> setEvalContext (evalContext)) != 0)
		return rc;
	
	// Check if count is negative
	if ((rc = getNegEval (op, ct_layout, negEval)) != 0)
		return rc;
	if ((rc = negEval -> setEvalContext (evalContext)) != 0)
		return rc;
	
	// Check if count == 0
	if ((rc = getZeroEval (op, ct_layout, zeroEval)) != 0)
		return rc;
	if ((rc = zeroEval -> setEvalContext (evalContext)) != 0)
		return rc;	

	// Count synopsis
	ASSERT (op -> u.DSTREAM.nowSyn);
	ASSERT (op -> u.DSTREAM.nowSyn -> kind == REL_SYN);
	
	countSyn = new RelationSynopsisImpl (op -> u.DSTREAM.nowSyn -> id,
										 LOG);
	op -> u.DSTREAM.nowSyn -> u.relSyn = countSyn;
	
	// Index for looking up count synopsis
	if (numIndexes >= MAX_INDEXES)
		return -1;	
	countIdx = new HashIndex (numIndexes, LOG);
	indexes [numIndexes++] = countIdx;

	// Initialize the count index
	if ((rc = initCountIndex (op, memMgr, evalContext, countIdx)) != 0)
		return rc;
	
	// Scan for count of a particular tuple
	if ((rc = countSyn -> setIndexScan (0, countIdx, countScanId)) != 0)
		return rc;

	// Full scan
	if ((rc = countSyn -> setScan (0, fullScanId)) != 0)
		return rc;

	if ((rc = countSyn -> setEvalContext (evalContext)) != 0)
		return rc;
	
	if ((rc = countSyn -> initialize ()) != 0)
		return rc;
	
	// Construct output tuple
	if ((rc = getOutEval (op, ct_layout, outEval)) != 0)
		return rc;
	if ((rc = outEval -> setEvalContext (evalContext)) != 0)
		return rc;
	
	// Output store
	ASSERT (op -> store);
	ASSERT (op -> store -> kind == SIMPLE_STORE ||
			op -> store -> kind == WIN_STORE);

	tupleLayout = new TupleLayout(op);
	if (op -> store -> kind == SIMPLE_STORE) {
		if ((rc = inst_simple_store (op -> store, tupleLayout)) != 0)
			return rc;
	}
	else {
		if ((rc = inst_win_store (op -> store, tupleLayout)) != 0)
			return rc;
	}
	outStore = op -> store -> instStore;
	
	// Synopsis store: contains an additional column - count
	ASSERT (op -> u.DSTREAM.nowStore);
	ASSERT (op -> u.DSTREAM.nowStore -> kind == REL_STORE);
	
	unsigned int cc;
	if ((rc = tupleLayout -> addFixedLenAttr (INT, cc)) != 0)
		return rc;
	if ((rc = inst_rel_store (op -> u.DSTREAM.nowStore, tupleLayout)) != 0)
		return rc;
	synStore = op -> u.DSTREAM.nowStore -> instStore;	
	
	// Const tuple
	if ((rc = getStaticTuple (constTuple,
							  ct_layout -> getTupleLen ())) != 0)
		return rc;
	if ((rc = ct_layout -> genTuple (constTuple)) != 0)
		return rc;
	evalContext -> bind (constTuple, CONST_ROLE);
	
	// Construct the Istream object
	dstream = new Dstream (op -> id, LOG);
	
	if ((rc = dstream -> setSynopsis (countSyn)) != 0)
		return rc;

	if ((rc = dstream -> setCountScan (countScanId)) != 0)
		return rc;

	if ((rc = dstream -> setFullScan (fullScanId)) != 0)
		return rc;

	if ((rc = dstream -> setEvalContext (evalContext)) != 0)
		return rc;

	if ((rc = dstream -> setIncrEval (incrEval)) != 0)
		return rc;

	if ((rc = dstream -> setDecrEval (decrEval)) != 0)
		return rc;

	if ((rc = dstream -> setInitEval (initEval)) != 0)
		return rc;

	if ((rc = dstream -> setZeroEval (zeroEval)) != 0)
		return rc;

	if ((rc = dstream -> setNegEval (negEval)) != 0)
		return rc;

	if ((rc = dstream -> setOutEval (outEval)) != 0)
		return rc;
	
	if ((rc = dstream -> setOutStore (outStore)) != 0)
		return rc;	

	if ((rc = dstream -> setSynStore (synStore)) != 0)
		return rc;
	
	op -> instOp = dstream;	
	
	delete ct_layout;
	delete tupleLayout;
	
	return 0;	
}

static int getIncrEval (Physical::Operator *op, ConstTupleLayout *ct_layout,
						AEval *&eval)
{
	int rc;
	AInstr instr;
	
	eval = new AEval();
	
	instr.op = Execution::INT_ADD;
	
	// Input1: old count
	instr.r1 = SYN_ROLE;
	instr.c1 = countCol;
	
	// Input2: 1 (const val)
	instr.r2 = CONST_ROLE;
	if ((rc = ct_layout -> addInt (1, instr.c2)) != 0)
		return rc;

	// Dest: (new count)
	instr.dr = SYN_ROLE;
	instr.dc = countCol;

	if ((rc = eval -> addInstr (instr)) != 0)
		return rc;
	
	return 0;
}

static int getDecrEval (Physical::Operator *op, ConstTupleLayout *ct_layout,
						AEval *&eval)
{
	int rc;
	AInstr instr;
	
	eval = new AEval();
	
	instr.op = Execution::INT_SUB;
	
	// Input1: old count
	instr.r1 = SYN_ROLE;
	instr.c1 = countCol;
	
	// Input2: 1 (const val)
	instr.r2 = CONST_ROLE;
	if ((rc = ct_layout -> addInt (1, instr.c2)) != 0)
		return rc;
	
	// Dest: (new count)
	instr.dr = SYN_ROLE;
	instr.dc = countCol;
	
	if ((rc = eval -> addInstr (instr)) != 0)
		return rc;
	
	return 0;
}

static int getInitEval (Physical::Operator *op, ConstTupleLayout *ct_layout,
						AEval *&eval)
{
	int rc;
	AInstr instr;
	
	eval = new AEval();

	// copy the data columns
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

		instr.r1 = INPUT_ROLE;
		instr.c1 = dataCols [a];

		instr.dr = SYN_ROLE;
		instr.dc = dataCols [a];
		
		if ((eval -> addInstr (instr)) != 0)
			return rc;
	}

	// Initialize counter to 0
	instr.op = Execution::INT_CPY;
	
	// Input1: 1 (const val)
	instr.r1 = CONST_ROLE;
	if ((rc = ct_layout -> addInt (0, instr.c1)) != 0)
		return rc;
	
	// Dest: (new count)
	instr.dr = SYN_ROLE;
	instr.dc = countCol;
	
	if ((rc = eval -> addInstr (instr)) != 0)
		return rc;
	
	return 0;
}

static int getPosEval (Physical::Operator *op, ConstTupleLayout *ct_layout,
					   BEval *&eval)
{
	int rc;
	BInstr instr;

	eval = new BEval ();

	// lhs > rhs
	instr.op = Execution::INT_GT;

	// lhs: count column
	instr.r1 = SYN_ROLE;
	instr.c1 = countCol;
	instr.e1 = 0;
	
	// rhs: 0
	instr.r2 = CONST_ROLE;
	if ((rc = ct_layout -> addInt (0, instr.c2)) != 0)
		return rc;
	instr.e2 = 0;
	
	if ((rc = eval -> addInstr (instr)) != 0)
		return rc;

	return 0;
}

static int getNegEval (Physical::Operator *op, ConstTupleLayout *ct_layout,
					   BEval *&eval)
{
	int rc;
	BInstr instr;

	eval = new BEval ();

	// lhs > rhs
	instr.op = Execution::INT_LT;

	// lhs: count column
	instr.r1 = SYN_ROLE;
	instr.c1 = countCol;
	instr.e1 = 0;
	
	// rhs: 0
	instr.r2 = CONST_ROLE;
	if ((rc = ct_layout -> addInt (0, instr.c2)) != 0)
		return rc;
	instr.e2 = 0;

	if ((rc = eval -> addInstr (instr)) != 0)
		return rc;

	return 0;
}

static int getZeroEval (Physical::Operator *op, ConstTupleLayout *ct_layout,
						BEval *&eval)
{
	int rc;
	BInstr instr;

	eval = new BEval ();

	// lhs > rhs
	instr.op = Execution::INT_EQ;

	// lhs: count column
	instr.r1 = SYN_ROLE;
	instr.c1 = countCol;
	instr.e1 = 0;
	
	// rhs: 0
	instr.r2 = CONST_ROLE;
	if ((rc = ct_layout -> addInt (0, instr.c2)) != 0)
		return rc;
	instr.e2 = 0;
	
	if ((rc = eval -> addInstr (instr)) != 0)
		return rc;
	
	return 0;
}

static int getOutEval (Physical::Operator *op, ConstTupleLayout *ct_layout,
					   AEval *&eval)
{
	int rc;
	AInstr instr;
	Type attrType;

	eval = new AEval ();
	
	for (unsigned int a = 0 ; a < op -> numAttrs ; a++) {

		attrType = op -> attrTypes [a];
		switch (attrType) {
		case INT:   instr.op = Execution::INT_CPY; break;
		case FLOAT: instr.op = Execution::FLT_CPY; break;
		case CHAR:  instr.op = Execution::CHR_CPY; break;
		case BYTE:  instr.op = Execution::BYT_CPY; break;
			
#ifdef _DM_
		default:
			ASSERT(0);
			break;
#endif
		}
		
		// source
		instr.r1 = SYN_ROLE;
		instr.c1 = dataCols [a];
		
		// dest
		instr.dr = OUTPUT_ROLE;
		instr.dc = dataCols [a];
		
		if ((rc = eval -> addInstr (instr)) != 0)
			return rc;
	}

	return 0;
}

static int initCountIndex (Physical::Operator *op,
						   MemoryManager *memMgr,
						   EvalContext *evalContext,
						   HashIndex *idx)
{
	int rc;
	HEval *updateHash, *scanHash;
	HInstr hinstr;
	BEval *keyEqual;
	BInstr binstr;

	updateHash = new HEval ();
	for (unsigned int a = 0 ; a < op -> numAttrs ; a++) {

		hinstr.type = op -> attrTypes [a];
		hinstr.r = UPDATE_ROLE;
		hinstr.c = dataCols [a];

		if ((rc = updateHash -> addInstr (hinstr)) != 0)
			return rc;
	}

	if ((rc = updateHash -> setEvalContext (evalContext)) != 0)
		return rc;
	
	scanHash = new HEval ();
	for (unsigned int a = 0 ; a < op -> numAttrs ; a++) {

		hinstr.type = op -> attrTypes [a];
		hinstr.r = INPUT_ROLE;
		hinstr.c = dataCols [a];
		
		if ((rc = scanHash -> addInstr (hinstr)) != 0)
			return rc;
	}
	if ((rc = scanHash -> setEvalContext (evalContext)) != 0)
		return rc;
	
	keyEqual = new BEval ();
	for (unsigned int a = 0 ; a < op -> numAttrs ; a++) {

		switch (op -> attrTypes [a]) {
		case INT:   binstr.op = Execution::INT_EQ; break;
		case FLOAT: binstr.op = Execution::FLT_EQ; break;
		case CHAR:  binstr.op = Execution::CHR_EQ; break;
		case BYTE:  binstr.op = Execution::BYT_EQ; break;
			
#ifdef _DM_
		default:
			ASSERT (0);
			break;
#endif
		}

		binstr.r1 = INPUT_ROLE;
		binstr.c1 = dataCols [a];
		binstr.e1 = 0;
		
		binstr.r2 = SCAN_ROLE;
		binstr.c2 = dataCols [a];
		binstr.e2 = 0;
		
		if ((rc = keyEqual -> addInstr (binstr)) != 0)
			return rc;
	}

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

static int getTupleLayout (Physical::Operator *op)
{
	int rc;
	TupleLayout *tupleLayout;
	unsigned int dataCol;
	
	tupleLayout = new TupleLayout (op);
	
	for (unsigned int a = 0 ; a < op -> numAttrs ; a++)
		dataCols [a] = tupleLayout -> getColumn (a);

	delete tupleLayout;
	
	tupleLayout = new TupleLayout();

	for (unsigned int a = 0 ; a < op -> numAttrs ; a++) {
		if ((rc = tupleLayout -> addAttr (op -> attrTypes [a],
										  op -> attrLen [a],
										  dataCol)) != 0) {
			return rc;
		}
		
		ASSERT (dataCol == dataCols [a]);
	}
	
	if ((rc = tupleLayout -> addFixedLenAttr (INT, countCol)) != 0)
		return rc;
	
	delete tupleLayout;	

	return 0;
}
