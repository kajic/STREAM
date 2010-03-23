#ifndef _PLAN_MGR_IMPL_
#include "metadata/plan_mgr_impl.h"
#endif

#ifndef _CONSTANTS_
#include "common/constants.h"
#endif

#ifndef _BIN_STR_JOIN_
#include "execution/operators/bin_str_join.h"
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
using Physical::Operator;
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
using Execution::BinStreamJoin;
using Execution::StorageAlloc;
using Execution::MemoryManager;

// Output layout
static unsigned int numOutCols;
static unsigned int outCols [MAX_ATTRS];

// Outer input layout
static unsigned int numLeftCols;        
static unsigned int leftCols [MAX_ATTRS];

// Inner input layout
static unsigned int numRightCols;       
static unsigned int rightCols [MAX_ATTRS];

static int computeLayout (Operator *op);

static int splitPred (BExpr *pred, BExpr *&eqPred, BExpr *&nePred);

static int initInnerIndex (Operator      *op,
						   BExpr         *eqPred,
						   MemoryManager *memMgr,
						   EvalContext   *evalContext,
						   HashIndex     *idx);					

static int getSimpleOutEval (Operator *op, AEval *&eval);

int PlanManagerImpl::inst_str_join (Physical::Operator *op)
{
	int rc;
	
	BExpr *pred, *eqPred, *nePred;
	Physical::Synopsis *p_inSyn;
	Expr **projs;
	
	unsigned int             roleMap [2];
	EvalContextInfo          evalCxt;	
	TupleLayout             *st_layout;
	unsigned int             st_size;
	ConstTupleLayout        *ct_layout;
	unsigned int             ct_size;
	TupleLayout             *outLayout;
	
	BinStreamJoin           *join;
	EvalContext             *evalContext;
	RelationSynopsisImpl    *e_inSyn;
	HashIndex               *inIdx;			
	unsigned int             inScanId;
	BEval                   *neEval;
	AEval                   *outEval;		
	char                    *scratchTuple;
	char                    *constTuple;
	StorageAlloc            *outStore;
	
	ASSERT (op);
	ASSERT (op -> kind == PO_STR_JOIN_PROJECT || op -> kind == PO_STR_JOIN);

	// compute the layout of output & input tuples
	if ((rc = computeLayout (op)) != 0)
		return rc;
	
	// Shared evaluation context 
	evalContext = new EvalContext ();	
	
	// State required for transforming expressions to AEval & BEval
	st_layout         = new TupleLayout ();
	ct_layout         = new ConstTupleLayout ();
	evalCxt.st_layout = st_layout;
	evalCxt.ct_layout = ct_layout;
	
	// Join predicate
	pred = (op -> kind == PO_STR_JOIN)? 
		op -> u.STR_JOIN.pred : op -> u.STR_JOIN_PROJECT.pred;
	
	// Split the join pred into equality and non-equality predicates
	if ((rc = splitPred (pred, eqPred, nePred)) != 0)
		return rc;
	
	// Construct an index on inner input for equality predicate attributes
	if (eqPred) {
		
		// Too many indexes
		if (numIndexes  >= MAX_INDEXES)
			return -1;
		
		// Construct and initialize the inner index
		inIdx = new HashIndex (numIndexes, LOG);
		indexes [numIndexes++] = inIdx;
		
		if ((rc = initInnerIndex (op, eqPred, memMgr,
								  evalContext, inIdx)) != 0)
			return rc;
	}
	
	// If there are non-equality (or non-join) predicates, construct an
	// evaluator to check these predicates, while scanning the inner
	// tuples 
	neEval = 0;
	if (nePred) {
		
		roleMap [0]       = OUTER_ROLE;
		roleMap [1]       = FI_SCAN_ROLE;
		
		if ((rc = inst_bexpr (nePred, roleMap, op, neEval, evalCxt)) != 0)
			return rc;
		
		if ((rc = neEval -> setEvalContext (evalContext)) != 0)
			return rc;
	}
	
	// Construct the synopsis that stores the inner tuples.
	p_inSyn = (op -> kind == PO_STR_JOIN) ?
		op -> u.STR_JOIN.innerSyn :
		op -> u.STR_JOIN_PROJECT.innerSyn;	
	ASSERT (p_inSyn);
	
	e_inSyn = new RelationSynopsisImpl (p_inSyn -> id, LOG);
	p_inSyn -> u.relSyn = e_inSyn;

	if ((rc = e_inSyn -> setEvalContext (evalContext)) != 0)
		return rc;
	
	// Scan over inner synopsis that produces all join tuples for a given
	// outer tuple
	if (eqPred) {
		if ((rc = e_inSyn -> setIndexScan (neEval, inIdx, inScanId)) != 0) {
			return rc;
		}
	}
	
	else {
		if ((rc = e_inSyn -> setScan (neEval, inScanId)) != 0) {
			return rc;
		}
	}
	if ((rc = e_inSyn -> initialize ()) != 0)
		return rc;
	
	// Evaluator to construct the output tuples
	if (op -> kind == PO_STR_JOIN_PROJECT) {
		projs = op -> u.JOIN_PROJECT.projs;

		roleMap [0] = OUTER_ROLE;
		roleMap [1] = INNER_ROLE;

		outEval = 0;
		// The output tuples are arbitrary expressions over input attibutes
		for (unsigned int a = 0 ; a < op -> numAttrs ; a++) {
			if ((rc = inst_expr_dest (projs [a],
									  roleMap,
									  op,
									  OUTPUT_ROLE,
									  outCols[a],
									  outEval,
									  evalCxt)) != 0) {
				return rc;
			}
		}	
		ASSERT (outEval);
	}
	
	else {
		// The output tuples are concatenation of input attributes
		if ((rc = getSimpleOutEval (op, outEval)) != 0)
			return rc;				
	}
	if ((rc = outEval -> setEvalContext (evalContext)) != 0)
		return rc;	
	
	// Set the constant and scratch tuples used in various evals
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
	
	// Construct the storage allocator for the output.
	outLayout = new TupleLayout (op);	
	
	ASSERT (op -> store);
	ASSERT (op -> store -> kind == SIMPLE_STORE ||
			op -> store -> kind == WIN_STORE);
	
	if (op -> store -> kind == SIMPLE_STORE) {
		if ((rc = inst_simple_store (op -> store, outLayout)) != 0)
			return rc;		
	}
	
	else {
		if ((rc = inst_win_store (op -> store, outLayout)) != 0)
			return rc;		
	}
	outStore = op -> store -> instStore;
	ASSERT (outStore);
	delete outLayout;
	
	// Construct the stream join operator and initialize
	join = new BinStreamJoin (op -> id, LOG);
	
	if ((rc = join -> setSynopsis (e_inSyn)) != 0)
		return rc;
	if ((rc = join -> setScan (inScanId)) != 0)
		return rc;
	if ((rc = join -> setEvalContext (evalContext)) != 0)
		return rc;
	if ((rc = join -> setOutputConstructor (outEval)) != 0)
		return rc;
	if ((rc = join -> setOutputStore (outStore)) != 0)
		return rc;
	
	delete st_layout;
	delete ct_layout;
	
	op -> instOp = join;
	
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
	
	ASSERT (op -> kind == PO_STR_JOIN);
	ASSERT (numOutCols ==
			op -> u.STR_JOIN.numOuterAttrs +
			op -> u.STR_JOIN.numInnerAttrs);
	
	outEval = new AEval ();
	
	// Copy the attributes of the left input
	for (unsigned int a = 0 ; a < op->u.STR_JOIN.numOuterAttrs ; a++) {
		
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
	for (unsigned int a = 0 ; a < op->u.STR_JOIN.numInnerAttrs ; a++) {
		
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
