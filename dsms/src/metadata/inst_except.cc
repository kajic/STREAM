#ifndef _PLAN_MGR_IMPL_
#include "metadata/plan_mgr_impl.h"
#endif

#ifndef _EXCEPT_
#include "execution/operators/except.h"
#endif

#ifndef _REL_SYN_IMPL_
#include "execution/synopses/rel_syn_impl.h"
#endif

#ifndef _HASH_INDEX_
#include "execution/indexes/hash_index.h"
#endif

static const unsigned int SCRATCH_ROLE = 0;
static const unsigned int INPUT_ROLE   = 2;
static const unsigned int LEFT_ROLE    = 2;
static const unsigned int RIGHT_ROLE   = 3;
static const unsigned int COUNT_ROLE   = 4;
static const unsigned int OUTPUT_ROLE  = 5;
static const unsigned int UPDATE_ROLE  = 6;
static const unsigned int SCAN_ROLE    = 7;
extern double INDEX_THRESHOLD;

using namespace Metadata;

using Physical::Operator;
using Execution::HashIndex;
using Execution::RelationSynopsisImpl;
using Execution::AEval;
using Execution::BEval;
using Execution::HEval;
using Execution::AInstr;
using Execution::BInstr;
using Execution::HInstr;
using Execution::EvalContext;
using Execution::Except;
using Execution::StorageAlloc;
using Execution::MemoryManager;

/// Number of data columns
unsigned int numCols;

/// Layout of output tuples (& data cols in count synopsis tuples)
unsigned int outCols [MAX_ATTRS];

/// Layout of left tuples
unsigned int leftCols [MAX_ATTRS];

/// Layout of right tuples
unsigned int rightCols [MAX_ATTRS];

/// The count column in count synopsis tuples
unsigned int countCol;

/// Number of columns that are identical between left & right inputs
unsigned int numIdenticalCols;

/// [[ Explanation ]]
unsigned int scratchCols [MAX_ATTRS];

static int computeDataLayout (Operator *op, TupleLayout *st_layout);
static int getcprsEval (Operator *op, AEval *&eval);
static int getcplsEval (Operator *op, AEval *&eval);
static int getInitEval (Operator *op, AEval *&eval);
static int getOutEval (Operator *op, AEval *&eval);

static int initCountIdx (Operator      *op,
						 MemoryManager *memMgr,
						 EvalContext   *evalContext,
						 HashIndex     *idx);

static int initOutIdx (Operator      *op,
					   MemoryManager *memMgr,
					   EvalContext   *evalContext,
					   HashIndex     *idx);

int PlanManagerImpl::inst_except (Physical::Operator *op)
{
	int rc;
	
	Except               *except;
	StorageAlloc         *outStore;
	StorageAlloc         *countStore;
	RelationSynopsisImpl *outSyn;
	RelationSynopsisImpl *countSyn;
	HashIndex            *outIdx;
	HashIndex            *countIdx;
	unsigned int          outScanId;
	unsigned int          countScanId;
	EvalContext          *evalContext;
	AEval                *initEval;
	AEval                *outEval;
	AEval                *cplsEval;
	AEval                *cprsEval;
	char                 *scratchTuple;
	unsigned int          countCol;
	
	TupleLayout          *st_layout;
	TupleLayout          *tupleLayout;
	TupleLayout          *countTupleLayout;
	
	// Compute the tuple layout of input/output
	st_layout = new TupleLayout ();
	if ((rc = computeDataLayout (op, st_layout)) != 0)
		return rc;
	
	// Evaluation context
	evalContext = new EvalContext ();

	// Scratch tuple
	if ((rc = getStaticTuple (scratchTuple, st_layout -> getTupleLen()))
		!= 0)
		return rc;
	evalContext -> bind (scratchTuple, SCRATCH_ROLE);	
	
	// Init evaluator:
	if ((rc = getInitEval (op, initEval)) != 0)
		return rc;
	if ((rc = initEval -> setEvalContext (evalContext)) != 0)
		return rc;
	
	// Output evaluator
	if ((rc = getOutEval (op, outEval)) != 0)
		return rc;
	if ((rc = outEval -> setEvalContext (evalContext)) != 0)
		return rc;
	
	// Evaluator that copies from left -> scratch
	if ((rc = getcplsEval (op, cplsEval)) != 0)
		return rc;
	if ((rc = cplsEval -> setEvalContext (evalContext)) != 0)
		return rc;
	
	// Evaluator that copies from right -> scratch
	if ((rc = getcprsEval (op, cprsEval)) != 0)
		return rc;
	if ((rc = cprsEval -> setEvalContext (evalContext)) != 0)
		return rc;
	
	// Count index
	if (numIndexes >= MAX_INDEXES)
		return -1;
	countIdx = new HashIndex (numIndexes, LOG);
	indexes [numIndexes++] = countIdx;	
	if ((rc = initCountIdx (op, memMgr, evalContext, countIdx)) != 0)
		return rc;
	
	// Count synopsis
	ASSERT (op -> u.EXCEPT.countSyn);
	ASSERT (op -> u.EXCEPT.countSyn -> kind == REL_SYN);
	countSyn = new RelationSynopsisImpl (op -> u.EXCEPT.countSyn -> id,
										 LOG);
	op -> u.EXCEPT.countSyn -> u.relSyn = countSyn;
	if ((rc = countSyn -> setEvalContext (evalContext)) != 0)
		return rc;
	
	// Count scan
	if ((rc = countSyn -> setIndexScan (0, countIdx, countScanId)) != 0)
		return rc;
	
	if ((rc = countSyn -> initialize ()) != 0)
		return rc;
	
	// Out index
	if (numIndexes >= MAX_INDEXES)
		return -1;
	outIdx = new HashIndex (numIndexes, LOG);
	indexes [numIndexes++] = outIdx;
	if ((rc = initOutIdx (op, memMgr, evalContext, outIdx)) != 0)
		return rc;
	
	// Output synopsis
	ASSERT (op -> u.EXCEPT.outSyn);
	ASSERT (op -> u.EXCEPT.outSyn -> kind == REL_SYN);
	outSyn = new RelationSynopsisImpl (op -> u.EXCEPT.outSyn -> id, LOG);
	op -> u.EXCEPT.outSyn -> u.relSyn = outSyn;
	if ((rc = outSyn -> setEvalContext (evalContext)) != 0)
		return rc;
	
	// Output scan
	if ((rc = outSyn -> setIndexScan (0, outIdx, outScanId)) != 0)
		return rc;

	if ((rc = outSyn -> initialize ()) != 0)
		return rc;


	// Compute the count tuple layout
	countTupleLayout = new TupleLayout (op);
	if ((rc = countTupleLayout -> addFixedLenAttr (INT, countCol)) != 0)
		return rc;
		
	// Count store
	ASSERT (op -> u.EXCEPT.countStore);
	ASSERT (op -> u.EXCEPT.countStore -> kind == REL_STORE);
	if ((rc = inst_rel_store (op -> u.EXCEPT.countStore,
							  countTupleLayout)) != 0)		
		return rc;
	countStore = op -> u.EXCEPT.countStore -> instStore;

	// Out store
	tupleLayout = new TupleLayout (op);
	ASSERT (op -> store);
	ASSERT (op -> store -> kind == REL_STORE);
	if ((rc = inst_rel_store (op -> store, tupleLayout)) != 0)
		return rc;
	outStore = op -> store -> instStore;
	
	except = new Except (op -> id, LOG);
	
	if ((rc = except -> setOutStore (outStore)) != 0)
		return rc;
	if ((rc = except -> setOutSyn (outSyn)) != 0)
		return rc;
	if ((rc = except -> setOutScanId (outScanId)) != 0)
		return rc;
	if ((rc = except -> setEvalContext (evalContext)) != 0)
		return rc;
	if ((rc = except -> setCountSyn (countSyn)) != 0)
		return rc;
	if ((rc = except -> setCountScanId (countScanId)) != 0)
		return rc;
	if ((rc = except -> setCountStore (countStore)) != 0)
		return rc;
	if ((rc = except -> setInitEval (initEval)) != 0)
		return rc;
	if ((rc = except -> setOutEval (outEval)) != 0)
		return rc;
	if ((rc = except -> setCountCol (countCol)) != 0)
		return rc;
	if ((rc = except -> setCopyLeftToScratchEval (cplsEval)) != 0)
		return rc;
	if ((rc = except -> setCopyRightToScratchEval (cprsEval)) != 0)
		return rc;
	
	op -> instOp = except;
	
	delete tupleLayout;
	delete st_layout;
	delete countTupleLayout;
	
	return 0;
}

static int computeDataLayout (Operator *op, TupleLayout *st_layout)
{
	int rc;
	Operator *left, *right;
	TupleLayout *tupleLayout, *leftTupleLayout, *rightTupleLayout;
	
	ASSERT (op -> numInputs == 2);
	left = op -> inputs [0];
	right = op -> inputs [1]; 
	ASSERT (left && right);
	
	tupleLayout = new TupleLayout (op);
	leftTupleLayout = new TupleLayout (left);
	rightTupleLayout = new TupleLayout (right);

	// The child operator could contain "internal" attributes (e.g., count
	// in an group aggr attr) not visible to op.
	ASSERT (left -> numAttrs >= op -> numAttrs);
	ASSERT (right -> numAttrs >= op -> numAttrs);	
	
	// Data layout
	numCols = op -> numAttrs;	
	for (unsigned int a = 0 ; a < numCols ; a++) {		
		outCols [a] = tupleLayout -> getColumn (a);
		leftCols [a] = leftTupleLayout -> getColumn (a);
		rightCols [a] = rightTupleLayout -> getColumn (a);		
	}

	// Number of identical columns
	for (unsigned int a = 0 ; a < numCols ; a++) {

		// The columns could differ only due to a CHAR attr with different
		// lengths in left & right
		if (outCols [a] != leftCols [a] ||
			outCols [a] != rightCols [a]) {

			ASSERT (a > 0 && op -> attrTypes [a - 1] == CHAR);
			break;
		}
		
		else {
			numIdenticalCols ++;
		}
	}
	
	// Scratch columns
	for (unsigned int a = numIdenticalCols ; a < numCols ; a++) {
		rc = st_layout -> addAttr (op -> attrTypes [a],
								   op -> attrLen [a],
								   scratchCols [a]);
		if (rc != 0) return rc;
	}

	delete tupleLayout;
	delete leftTupleLayout;
	delete rightTupleLayout;
	
	return 0;
}

static int getcplsEval (Operator *op, AEval *&eval)
{
	int rc;
	AInstr instr;

	eval = new AEval ();

	for (unsigned int a = numIdenticalCols ; a < numCols ; a++) {

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
		instr.c1 = leftCols [a];

		// Destn
		instr.dr = SCRATCH_ROLE;
		instr.dc = scratchCols [a];

		if ((rc = eval -> addInstr (instr)) != 0)
			return rc;
	}

	return 0;
}

static int getcprsEval (Operator *op, AEval *&eval)
{
	int rc;
	AInstr instr;

	eval = new AEval ();

	for (unsigned int a = numIdenticalCols ; a < numCols ; a++) {
		
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
		instr.c1 = rightCols [a];
		
		// Destn
		instr.dr = SCRATCH_ROLE;
		instr.dc = scratchCols [a];
		
		if ((rc = eval -> addInstr (instr)) != 0)
			return rc;
	}
	
	return 0;
}

static int getInitEval (Operator *op, AEval *&eval)						
{
	int rc;
	AInstr instr;
	
	eval = new AEval ();
	
	// Copy the data cols
	for (unsigned int a = 0 ; a < numIdenticalCols ; a++) {
		
		// Operation
		switch (op -> attrTypes [a]) {
		case INT:   instr.op = Execution::INT_CPY; break;
		case FLOAT: instr.op = Execution::FLT_CPY; break;
		case BYTE:  instr.op = Execution::BYT_CPY; break;
		case CHAR:  instr.op = Execution::CHR_CPY; break;				
		default:
			ASSERT (0); break;
		}
		
		// Source: Note the outCols[a] == leftCols[a] == rightCols[a] by
		// the definition of numIdenticalCols
		instr.r1 = INPUT_ROLE;
		instr.c1 = outCols [a]; 
		
		// Destn
		instr.dr = COUNT_ROLE;
		instr.dc = outCols [a];
		
		if ((rc = eval -> addInstr (instr)) != 0)
			return rc;
	}
	
	for (unsigned int a = numIdenticalCols ; a < numCols ; a++) {
		
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
		instr.r1 = SCRATCH_ROLE;
		instr.c1 = scratchCols [a];
		
		// Destn
		instr.dr = COUNT_ROLE;
		instr.dc = outCols [a];
		
		if ((rc = eval -> addInstr (instr)) != 0)
			return rc;
	}
	
	return 0;
}

static int getOutEval (Operator *op, AEval *&eval)					   
{
	int rc;
	AInstr instr;

	eval = new AEval ();
	
	// Copy the data cols
	for (unsigned int a = 0 ; a < numCols ; a++) {
		
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
		instr.r1 = COUNT_ROLE;
		instr.c1 = outCols [a];
		
		// Destn
		instr.dr = OUTPUT_ROLE;
		instr.dc = outCols [a];
		
		if ((rc = eval -> addInstr (instr)) != 0)
			return rc;
	}
	
	return 0;
}

static int initCountIdx (Operator      *op,
						 MemoryManager *memMgr,
						 EvalContext   *evalContext,
						 HashIndex     *idx)
{	
	int rc;
	
	HEval *updateHash, *scanHash;
	HInstr hinstr;
	BEval *keyEqual;
	BInstr binstr;
	
	updateHash = new HEval();
	for (unsigned int a = 0 ; a < op -> numAttrs ; a++) {
		
		hinstr.type = op -> attrTypes [a];
		hinstr.r    = UPDATE_ROLE;
		hinstr.c    = outCols [a];
		
		if ((rc = updateHash -> addInstr (hinstr)) != 0)
			return rc;		
	}
	
	if ((rc = updateHash -> setEvalContext (evalContext)) != 0)
		return rc;
	
	scanHash = new HEval ();
	for (unsigned int a = 0 ; a < numIdenticalCols ; a++) {
		
		hinstr.type = op -> attrTypes [a];
		hinstr.r    = INPUT_ROLE;
		hinstr.c    = outCols [a];

		if ((rc = scanHash -> addInstr (hinstr)) != 0)
			return rc;
	}
	for (unsigned int a = numIdenticalCols ; a < numCols ; a++) {
		
		hinstr.type = op -> attrTypes [a];
		hinstr.r    = SCRATCH_ROLE;
		hinstr.c    = scratchCols [a];
		
		if ((rc = scanHash -> addInstr (hinstr)) != 0)
			return rc;
	}	
	if ((rc = scanHash -> setEvalContext (evalContext)) != 0)
		return rc;
	
	keyEqual = new BEval ();
	for (unsigned int a = 0 ; a < numIdenticalCols ; a++) {

		switch (op -> attrTypes [a]) {
		case INT:    binstr.op = Execution::INT_EQ; break;
		case FLOAT:  binstr.op = Execution::FLT_EQ; break;
		case CHAR:   binstr.op = Execution::CHR_EQ; break;
		case BYTE:   binstr.op = Execution::BYT_EQ; break;			
		default:
			ASSERT (0);
			break;
		}
		
		binstr.r1    = INPUT_ROLE;
		binstr.c1    = outCols [a];
		binstr.e1    = 0;
		
		binstr.r2    = SCAN_ROLE;
		binstr.c2    = outCols [a];
		binstr.e2    = 0;
		
		if ((rc = keyEqual -> addInstr (binstr)) != 0)
			return rc;
	}
	for (unsigned int a = numIdenticalCols ; a < numCols ; a++) {

		switch (op -> attrTypes [a]) {
		case INT:    binstr.op = Execution::INT_EQ; break;
		case FLOAT:  binstr.op = Execution::FLT_EQ; break;
		case CHAR:   binstr.op = Execution::CHR_EQ; break;
		case BYTE:   binstr.op = Execution::BYT_EQ; break;			
		default:
			ASSERT (0);
			break;
		}
		
		binstr.r1    = SCRATCH_ROLE;
		binstr.c1    = scratchCols [a];
		binstr.e1    = 0;
		
		binstr.r2    = SCAN_ROLE;
		binstr.c2    = outCols [a];
		binstr.e2    = 0;
		
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

static int initOutIdx (Operator      *op,
					   MemoryManager *memMgr,
					   EvalContext   *evalContext,
					   HashIndex     *idx)
{	
	int rc;

	HEval *updateHash, *scanHash;
	HInstr hinstr;
	BEval *keyEqual;
	BInstr binstr;

	updateHash = new HEval();
	for (unsigned int a = 0 ; a < op -> numAttrs ; a++) {

		hinstr.type = op -> attrTypes [a];
		hinstr.r    = UPDATE_ROLE;
		hinstr.c    = outCols [a];
		
		if ((rc = updateHash -> addInstr (hinstr)) != 0)
			return rc;		
	}
	if ((rc = updateHash -> setEvalContext (evalContext)) != 0)
		return rc;
	
	scanHash = new HEval ();
	for (unsigned int a = 0 ; a < op -> numAttrs ; a++) {

		hinstr.type = op -> attrTypes [a];
		hinstr.r    = COUNT_ROLE;
		hinstr.c    = outCols [a];
		
		if ((rc = scanHash -> addInstr (hinstr)) != 0)
			return rc;
	}
	if ((rc = scanHash -> setEvalContext (evalContext)) != 0)
		return rc;

	keyEqual = new BEval ();
	for (unsigned int a = 0 ; a < op -> numAttrs ; a++) {

		switch (op -> attrTypes [a]) {
		case INT:    binstr.op = Execution::INT_EQ; break;
		case FLOAT:  binstr.op = Execution::FLT_EQ; break;
		case CHAR:   binstr.op = Execution::CHR_EQ; break;
		case BYTE:   binstr.op = Execution::BYT_EQ; break;			
		default:
			ASSERT (0);
			break;
		}

		binstr.r1    = COUNT_ROLE;
		binstr.c1    = outCols [a];
		binstr.e1    = 0;
		
		binstr.r2    = SCAN_ROLE;
		binstr.c2    = outCols [a];
		binstr.e2    = 0;
		
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
