#ifndef _PLAN_MGR_IMPL_
#include "metadata/plan_mgr_impl.h"
#endif

#ifndef _CONSTANTS_
#include "common/constants.h"
#endif

#ifndef _BIN_JOIN_
#include "execution/operators/bin_join.h"
#endif

#ifndef _HASH_INDEX_
#include "execution/indexes/hash_index.h"
#endif

#ifndef _REL_SYN_IMPL_
#include "execution/synopses/rel_syn_impl.h"
#endif

#ifndef _LIN_SYN_IMPL_
#include "execution/synopses/lin_syn_impl.h"
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

#ifndef _STORE_ALLOC_
#include "execution/stores/store_alloc.h"
#endif

static const unsigned int OUTER_ROLE = 2;
static const unsigned int INNER_ROLE = 3;
static const unsigned int OUTPUT_ROLE = 4;
static const unsigned int UPDATE_ROLE = 6;
static const unsigned int SCAN_ROLE = 7;
static const unsigned int FI_SCAN_ROLE = 8;
extern double INDEX_THRESHOLD;

using namespace Metadata;

using Physical::BExpr;
using Physical::Expr;
using Execution::HashIndex;
using Execution::RelationSynopsisImpl;
using Execution::LineageSynopsisImpl;
using Execution::AEval;
using Execution::BEval;
using Execution::HEval;
using Execution::AInstr;
using Execution::BInstr;
using Execution::HInstr;
using Execution::EvalContext;
using Execution::BinaryJoin;
using Execution::StorageAlloc;
using Execution::MemoryManager;

static int computeLayout (Operator *op);

static int splitPred (BExpr *pred, BExpr *&eqPred, BExpr *&nePred);

static int initOuterIndex (Physical::Operator *op, BExpr *eqPred,
						   MemoryManager *memMgr,
						   EvalContext *evalContext, HashIndex *idx);					

static int initInnerIndex (Physical::Operator *op, BExpr *eqPred,
						   MemoryManager *memMgr,
						   EvalContext *evalContext, HashIndex *idx);					

static int getSimpleOutEval (Operator *op, AEval *&outEval);

// Output layout
static unsigned int numOutCols;
static unsigned int outCols [MAX_ATTRS];

// Outer input layout
static unsigned int numLeftCols;        
static unsigned int leftCols [MAX_ATTRS];

// Inner input layout
static unsigned int numRightCols;       
static unsigned int rightCols [MAX_ATTRS];

int PlanManagerImpl::inst_join (Physical::Operator *op)
{
	int rc;

	unsigned int roleMap [2];
	EvalContextInfo evalCxt;
	BExpr *pred, *eqPred, *nePred;	
	Physical::Synopsis *p_outSyn, *p_inSyn;	
	Expr **projs;	
	TupleLayout *st_layout;
	ConstTupleLayout *ct_layout;
	unsigned int st_size, ct_size;
	char *scratchTuple, *constTuple;	
	Physical::Synopsis *p_joinSyn;
	TupleLayout *dataLayout;
	
	BinaryJoin                  *join;
	EvalContext                 *evalContext;	
	AEval                       *outEval;
	BEval                       *neEval_in, *neEval_out;
	LineageSynopsisImpl         *e_joinSyn;
	RelationSynopsisImpl        *e_outSyn;
	RelationSynopsisImpl        *e_inSyn;
	unsigned int                 inScanId;
	unsigned int                 outScanId;
	StorageAlloc                *store;
	HashIndex                   *outIdx;
	HashIndex                   *inIdx;
	
	ASSERT (op);
	ASSERT (op -> kind == PO_JOIN_PROJECT || op -> kind == PO_JOIN);

	// compute the layout of output & input tuples
	if ((rc = computeLayout (op)) != 0)
		return rc;
	
	// Shared evaluation context 
	evalContext = new EvalContext ();

	// State required for transforming expressions to BEval & AEval
	st_layout         = new TupleLayout ();
	ct_layout         = new ConstTupleLayout ();
	evalCxt.st_layout = st_layout;
	evalCxt.ct_layout = ct_layout;

	// Join predicate
	pred = (op -> kind == PO_JOIN)? 
		op -> u.JOIN.pred : op -> u.JOIN_PROJECT.pred;
	
    // Split the join pred into equality and non-equality predicates
	if ((rc = splitPred (pred, eqPred, nePred)) != 0)
		return rc;
	
	// Construct an index on inner input for equality predicate attributes
	if (eqPred) {
		
		// Too many indexes
		if (numIndexes + 1 >= MAX_INDEXES)
			return -1;
		
		// Construct & initialize the indexes
		outIdx = new HashIndex (numIndexes, LOG);
		indexes [numIndexes++] = outIdx;
		
		if ((rc = initOuterIndex (op, eqPred, memMgr,
								  evalContext, outIdx)) != 0)
			return rc;		
		
		inIdx = new HashIndex (numIndexes, LOG);
		indexes [numIndexes++] = inIdx;
		
		if ((rc = initInnerIndex (op, eqPred, memMgr,
								  evalContext, inIdx)) != 0)
			return rc;
	}
	
	// Construct evaluator to check non-equality predicates
	neEval_out = 0;
	neEval_in = 0;
	if (nePred) {
		
		roleMap [0] = FI_SCAN_ROLE;
		roleMap [1] = INNER_ROLE;
		
		if ((rc = inst_bexpr (nePred, roleMap, op, neEval_out,
							  evalCxt)) != 0)
			return rc;
		if ((rc = neEval_out -> setEvalContext (evalContext)) != 0)
			return rc;
		
		roleMap [0] = OUTER_ROLE;
		roleMap [1] = FI_SCAN_ROLE;
		
		if ((rc = inst_bexpr (nePred, roleMap, op, neEval_in,
							  evalCxt)) != 0)
			return rc;
		if ((rc = neEval_in -> setEvalContext (evalContext)) != 0)
			return rc;
	}
	
	// Construct outer synopsis
	p_outSyn = (op -> kind == PO_JOIN) ?
		op -> u.JOIN.outerSyn :
		op -> u.JOIN_PROJECT.outerSyn;
	
	ASSERT (p_outSyn);
	e_outSyn = new RelationSynopsisImpl (p_outSyn -> id, LOG);	
	p_outSyn -> u.relSyn = e_outSyn;
	
	if (eqPred) {
		if ((rc = e_outSyn -> setIndexScan (neEval_out, outIdx, outScanId)) != 0) {
			return rc;
		}
	}
	
	else {
		if ((rc = e_outSyn -> setScan (neEval_out, outScanId)) != 0) {
			return rc;
		}
	}
	if ((rc = e_outSyn -> setEvalContext (evalContext)) != 0)
		return rc;
	if ((rc = e_outSyn -> initialize ()) != 0)
		return rc;
	
	// Construct Inner Synopsis
	p_inSyn = (op -> kind == PO_JOIN) ?
		op -> u.JOIN.innerSyn :
		op -> u.JOIN_PROJECT.innerSyn;
	
	ASSERT (p_inSyn);
	
	e_inSyn = new RelationSynopsisImpl (p_inSyn -> id, LOG);
	p_inSyn -> u.relSyn = e_inSyn;
	
	if (eqPred) {
		if ((rc = e_inSyn -> setIndexScan (neEval_in, inIdx, inScanId)) != 0) {
			return rc;
		}
	}
	else {
		if ((rc = e_inSyn -> setScan (neEval_in, inScanId)) != 0) {
			return rc;
		}
	}
	if ((rc = e_inSyn -> setEvalContext (evalContext)) != 0)
		return rc;
	if ((rc = e_inSyn -> initialize ()) != 0)
		return rc;
	
	// Evaluator to construct the output tuples
	if (op -> kind == PO_JOIN_PROJECT) {
		projs = op -> u.JOIN_PROJECT.projs;
		
		roleMap [0] = OUTER_ROLE;
		roleMap [1] = INNER_ROLE;

		outEval = 0;
		for (unsigned int a = 0 ; a < op -> numAttrs ; a++) {
			if ((rc = inst_expr_dest (projs [a],
									  roleMap,
									  op,
									  OUTPUT_ROLE,
									  outCols [a],
									  outEval,
									  evalCxt)) != 0)
				return rc;			
		}
		
		ASSERT (outEval);
	}
	
	else {
		if ((rc = getSimpleOutEval (op, outEval)) != 0)
			return rc;
	}	
	if ((rc = outEval -> setEvalContext (evalContext)) != 0)
		return rc;	
	
	// Set the constant and scratch tuples
	st_size = st_layout -> getTupleLen ();
	ct_size = ct_layout -> getTupleLen ();
	
	if (st_size > 0) {
		if ((rc = getStaticTuple (scratchTuple, st_size)) != 0)
			return rc;
		evalContext -> bind (scratchTuple, SCRATCH_ROLE);
	}
	
	if (ct_size > 0) {
		if ((rc = getStaticTuple (constTuple, ct_size)) != 0)
			return rc;
		
		if ((rc = ct_layout -> genTuple (constTuple)) != 0)
			return rc;
		
		evalContext -> bind (constTuple, CONST_ROLE);
	}			
	
	// (Optional) output synopsis
	p_joinSyn = (op -> kind == PO_JOIN) ?
		op -> u.JOIN.joinSyn :
		op -> u.JOIN_PROJECT.joinSyn;
	
	e_joinSyn = 0;
	if (p_joinSyn) {
		
		ASSERT (!op -> bStream);
		ASSERT (p_joinSyn -> store == op -> store);
		ASSERT (p_joinSyn -> kind == LIN_SYN);
		
		e_joinSyn = new LineageSynopsisImpl (p_joinSyn -> id, LOG);
		p_joinSyn -> u.linSyn = e_joinSyn;
	}
	
	// Output store
	ASSERT (op -> store);
	ASSERT (op -> bStream && (op -> store -> kind == SIMPLE_STORE ||
							  op -> store -> kind == WIN_STORE)
			||
			!op -> bStream && op -> store -> kind == LIN_STORE);
	
	dataLayout = new TupleLayout (op);
	if (op -> store -> kind == LIN_STORE) {		
		if ((rc = inst_lin_store (op -> store, dataLayout)) != 0)
			return rc;
		store = op -> store -> instStore;		
	}	
	else {
		if ((rc = inst_simple_store (op -> store, dataLayout)) != 0)
			return rc;
		store = op -> store -> instStore;
	}
	
	// Construct the join operator & perform initializations
	join = new BinaryJoin (op -> id, LOG);

	if ((rc = join -> setOuterSynopsis (e_outSyn)) != 0)
		return rc;
	if ((rc = join -> setInnerSynopsis (e_inSyn)) != 0)
		return rc;
	if ((rc = join -> setJoinSynopsis (e_joinSyn)) != 0)
		return rc;
	if ((rc = join -> setOuterScan (outScanId)) != 0)
		return rc;
	if ((rc = join -> setInnerScan (inScanId)) != 0)
		return rc;	
	if ((rc = join -> setEvalContext (evalContext)) != 0)
		return rc;
	if ((rc = join -> setOutputConstructor (outEval)) != 0)
		return rc;
	if ((rc = join -> setOutStore (store)) != 0)
		return rc;
	
	op -> instOp = join;
	
	delete st_layout;
	delete ct_layout;
	delete dataLayout;
	
	return 0;
}
	
static int splitPred (BExpr *pred, BExpr *&eqPred, BExpr *&nePred)
{
	BExpr *next;
	Expr *temp;
	
	eqPred = nePred = 0;
	
	while (pred) {
		next = pred -> next;
		
		if ((pred -> left -> kind == ATTR_REF) &&
			(pred -> right -> kind == ATTR_REF) &&
			(pred -> op == EQ) && 
			(pred -> left -> u.attr.input !=
			 pred -> right -> u.attr.input)) {
			
			ASSERT (pred -> left -> u.attr.input == 0 ||
					pred -> left -> u.attr.input == 1);
			ASSERT (pred -> right -> u.attr.input == 0 ||
					pred -> right -> u.attr.input == 1);

			// We will bring the predicates to normalized form to help
			// later processing
			if (pred -> left -> u.attr.input == 1) {
				temp = pred -> left;
				pred -> left = pred -> right;
				pred -> right = temp;
			}
			
			pred -> next = eqPred;
			eqPred = pred;
		}			
		
		else {
			pred -> next = nePred;
			nePred = pred;
		}
		
		pred = next;
	}
	
	return 0;
}

static int initOuterIndex (Operator      *op,
						   BExpr         *eqPred,
						   MemoryManager *memMgr,
						   EvalContext   *evalContext,
						   HashIndex     *idx)
{
	int rc;
	Operator *leftChild, *rightChild;
	
	// isEqAttr [a] is true iff a'th attribute on left (outer) is used in
	// an equality join
	bool isEqAttr [MAX_ATTRS];
	
	// if isEqAttr[a] is true, then rightJoinPos[a] is *one* of the
	// attribute positions on the inner which joins with 'a' col
	unsigned int rightJoinPos [MAX_ATTRS];
	
	HEval *updateHash, *scanHash;	
	HInstr hinstr;
	BEval *keyEqual;
	BInstr binstr;
	
	leftChild = op -> inputs [0];
	rightChild = op -> inputs [1];
	
	// Populate isEqAttr[] & rightJoinPos[]
	for (unsigned int a = 0 ; a < numLeftCols ; a++) 
		isEqAttr [a] = false;
	
	BExpr *p = eqPred;
	while (p) {
		
		ASSERT (p -> left -> kind == ATTR_REF);
		ASSERT (p -> right -> kind == ATTR_REF);
		ASSERT (p -> op == EQ);
		ASSERT (p -> left -> u.attr.input == 0);
		ASSERT (p -> right -> u.attr.input == 1);
		
		isEqAttr [p -> left -> u.attr.pos] = true;
		rightJoinPos [p -> left -> u.attr.pos] =
			p -> right -> u.attr.pos;
		
		p = p -> next;
	}
	
	updateHash = new HEval ();	
	for (unsigned int a = 0 ; a < numLeftCols ; a++) {

		if (!isEqAttr [a])
			continue;
		
		hinstr.type = leftChild -> attrTypes [a];
		hinstr.r    = UPDATE_ROLE;
		hinstr.c    = leftCols [a];
		
		if ((rc = updateHash -> addInstr (hinstr)) != 0)
			return rc;		
	}
	if ((rc = updateHash -> setEvalContext (evalContext)) != 0)
		return rc;
	
	scanHash = new HEval ();
	for (unsigned int a = 0 ; a < numLeftCols ; a++) {

		if (!isEqAttr [a])
			continue;
		
		hinstr.type = leftChild -> attrTypes [a];
		hinstr.r    = INNER_ROLE;
		hinstr.c    = rightCols [rightJoinPos[a]];
		
		if ((rc = scanHash -> addInstr (hinstr)) != 0)
			return rc;
	}
	if ((rc = scanHash -> setEvalContext (evalContext)) != 0)
		return rc;
	
	// Boolean evaluator used by index during scans.
	keyEqual = new BEval ();	
	for (unsigned int a = 0 ; a < numLeftCols ; a++) {

		if (!isEqAttr [a])
			continue;
		
		switch (leftChild -> attrTypes [a]) {
		case INT:    binstr.op = Execution::INT_EQ; break;
		case FLOAT:  binstr.op = Execution::FLT_EQ; break;
		case CHAR:   binstr.op = Execution::CHR_EQ; break;
		case BYTE:   binstr.op = Execution::BYT_EQ; break;

		default:
			ASSERT (0);
			break;
		}
		
		binstr.r1 = SCAN_ROLE;
		binstr.c1 = leftCols [a];
		binstr.e1 = 0;
		
		binstr.r2 = INNER_ROLE;
		binstr.c2 = rightCols [rightJoinPos[a]];
		binstr.e2 = 0;
		
		if ((rc = keyEqual -> addInstr (binstr)) != 0)
			return rc;
	}
	if ((rc = keyEqual -> setEvalContext (evalContext)) != 0)
		return rc;

	if ((rc = idx -> setMemoryManager (memMgr)) != 0)
		return rc;
	if ((rc = idx -> setUpdateHashEval (updateHash)) != 0)
		return rc;
	if ((rc = idx -> setScanHashEval (scanHash)) != 0)
		return rc;
	if ((rc = idx -> setKeyEqual (keyEqual)) != 0)
		return rc;
	if ((rc = idx -> setThreshold (INDEX_THRESHOLD)) != 0)
		return rc;
	if ((rc = idx -> setEvalContext (evalContext)) != 0)
		return rc;
	if ((rc = idx -> initialize()) != 0)
		return rc;

	return 0;
}

static int initInnerIndex (Operator      *op,
						   BExpr         *eqPred,
						   MemoryManager *memMgr,
						   EvalContext   *evalContext,
						   HashIndex     *idx)
{
	int rc;
	Operator *leftChild, *rightChild;
	
	// isEqAttr [a] is true iff a'th attribute on right (inner) is used in
	// an equality join
	bool  isEqAttr [MAX_ATTRS];
	
	// if isEqAttr[a] is true, then leftJoinPos[a] is *one* of the
	// attribute positions on the outer which joins with 'a'
	unsigned int leftJoinPos [MAX_ATTRS];
	
	HEval *updateHash, *scanHash;	
	HInstr hinstr;
	BEval *keyEqual;
	BInstr binstr;

	leftChild = op -> inputs [0];
	rightChild = op -> inputs [1];
	
	// Populate isEqAttr[] array
	for (unsigned int a = 0 ; a < numRightCols ; a++) 
		isEqAttr[a] = false;
	
	BExpr *p = eqPred;	
	while (p) {
		
		ASSERT (p -> left -> kind == ATTR_REF);
		ASSERT (p -> right -> kind == ATTR_REF);
		ASSERT (p -> op == EQ);
		ASSERT (p -> left -> u.attr.input == 0);
		ASSERT (p -> right -> u.attr.input == 1);
		
		isEqAttr [p -> right -> u.attr.pos] = true;
		leftJoinPos [p -> right -> u.attr.pos] =
			p -> left -> u.attr.pos;
		
		p = p -> next;
	}
	
	updateHash = new HEval ();
	for (unsigned int a = 0 ; a < numRightCols ; a++) {
		
		if (!isEqAttr[a])
			continue;
		
		hinstr.type = rightChild -> attrTypes [a];
		hinstr.r    = UPDATE_ROLE;
		hinstr.c    = rightCols [a];
		
		if ((rc = updateHash -> addInstr (hinstr)) != 0)
			return rc;
	}	
	if ((rc = updateHash -> setEvalContext (evalContext)) != 0)
		return rc;	

	
	scanHash = new HEval ();	
	for (unsigned int a = 0 ; a < numRightCols ; a++) {

		if (!isEqAttr[a])
			continue;
		
		hinstr.type = rightChild -> attrTypes [a];
		hinstr.r    = OUTER_ROLE;
		hinstr.c    = leftCols [leftJoinPos [a]];
		
		if ((rc = scanHash -> addInstr (hinstr)) != 0)
			return rc;
	}	
	if ((rc = scanHash -> setEvalContext (evalContext)) != 0)
		return rc;
	

	keyEqual = new BEval ();
	for (unsigned int a = 0 ; a < numRightCols ; a++) {

		if (!isEqAttr[a])
			continue;

		// Operation: equality
		switch (rightChild -> attrTypes [a]) {
			
		case INT:    binstr.op = Execution::INT_EQ; break;
		case FLOAT:  binstr.op = Execution::FLT_EQ; break;
		case CHAR:   binstr.op = Execution::CHR_EQ; break;
		case BYTE:   binstr.op = Execution::BYT_EQ; break;
			
		default:
			ASSERT (0);
			break;
		}

		// lhs: a'th col of inner tuple stored in hash index
		binstr.r1 = SCAN_ROLE;
		binstr.c1 = rightCols [a];
		binstr.e1 = 0;

		// rhs: col of outer tuple that joins with a'th inner col
		binstr.r2 = OUTER_ROLE;
		binstr.c2 = leftCols [leftJoinPos[a]];
		binstr.e2 = 0;
		
		if ((rc = keyEqual -> addInstr (binstr)) != 0)
			return rc;
	}	
	if ((rc = keyEqual -> setEvalContext (evalContext)) != 0)
		return rc;	

	if ((rc = idx -> setMemoryManager (memMgr)) != 0)
		return rc;
	if ((rc = idx -> setUpdateHashEval (updateHash)) != 0)
		return rc;
	if ((rc = idx -> setScanHashEval (scanHash)) != 0)
		return rc;
	if ((rc = idx -> setKeyEqual (keyEqual)) != 0)
		return rc;
	if ((rc = idx -> setThreshold (INDEX_THRESHOLD)) != 0)
		return rc;
	if ((rc = idx -> setEvalContext (evalContext)) != 0)
		return rc;
	if ((rc = idx -> initialize ()) != 0)
		return rc;
	
	return 0;
}

static int computeLayout (Operator *op)
{
	TupleLayout *outLayout, *leftLayout, *rightLayout;

	outLayout = new TupleLayout (op);
	leftLayout = new TupleLayout (op -> inputs [0]);
	rightLayout = new TupleLayout (op -> inputs [1]);

	numOutCols = op -> numAttrs;
	
	numLeftCols = op -> inputs [0] -> numAttrs;
	numRightCols = op -> inputs [1] -> numAttrs;
	
	for (unsigned int a = 0 ; a < numOutCols ; a++)
		outCols [a] = outLayout -> getColumn (a);

	for (unsigned int a = 0 ; a < numLeftCols ; a++)
		leftCols [a] = leftLayout -> getColumn (a);

	for (unsigned int a = 0 ; a < numRightCols ; a++)
		rightCols [a] = rightLayout -> getColumn (a);

	delete outLayout;
	delete leftLayout;
	delete rightLayout;
	
	return 0;
}

static int getSimpleOutEval (Operator *op, AEval *&outEval)
{
	int rc;
	AInstr instr;

	ASSERT (op -> kind == PO_JOIN);
	ASSERT (numOutCols ==
			op -> u.JOIN.numOuterAttrs +
			op -> u.JOIN.numInnerAttrs);
	
	outEval = new AEval ();
	
	// Copy the attributes of the left input
	for (unsigned int a = 0 ; a < op -> u.JOIN.numOuterAttrs ; a++) {
		
		// Operation: copy 
		switch (op -> attrTypes [a]) {
		case INT:   instr.op = Execution::INT_CPY; break;
		case FLOAT: instr.op = Execution::FLT_CPY; break;
		case BYTE:  instr.op = Execution::BYT_CPY; break;
		case CHAR:  instr.op = Execution::CHR_CPY; break;				
		default:
			ASSERT (0); break;
		}
		
		// Source: a'th column of left
		instr.r1 = OUTER_ROLE;
		instr.c1 = leftCols [a];
		
		// Destn: a'th column of output
		instr.dr = OUTPUT_ROLE;
		instr.dc = outCols [a];
		
		if ((rc = outEval -> addInstr (instr)) != 0)
			return rc;		
	}

	// Copy the attributes of the right input
	for (unsigned int a = 0 ; a < op -> u.JOIN.numInnerAttrs ; a++) {
		
		// Operation: copy 
		switch (op -> attrTypes [a + numLeftCols]) {
		case INT:   instr.op = Execution::INT_CPY; break;
		case FLOAT: instr.op = Execution::FLT_CPY; break;
		case BYTE:  instr.op = Execution::BYT_CPY; break;
		case CHAR:  instr.op = Execution::CHR_CPY; break;				
		default:
			ASSERT (0); break;
		}

		// Source: a'th column of right
		instr.r1 = INNER_ROLE;
		instr.c1 = rightCols [a];
		
		// Destn: (a + numLeftAttr) column of output
		instr.dr = OUTPUT_ROLE;
		instr.dc = outCols [a + numLeftCols];
		
		if ((rc = outEval -> addInstr (instr)) != 0)
			return rc;
	}
	
	return 0;
}
