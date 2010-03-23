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

#ifndef _GROUP_AGGR_
#include "execution/operators/group_aggr.h"
#endif

#ifndef _STORE_ALLOC_
#include "execution/store/store_alloc.h"
#endif

static const unsigned int SCRATCH_ROLE = 0;
static const unsigned int CONST_ROLE = 1;
static const unsigned int NEW_OUTPUT_ROLE = 2;
static const unsigned int OLD_OUTPUT_ROLE = 3;
static const unsigned int INPUT_ROLE = 4;
static const unsigned int UPDATE_ROLE = 6;
static const unsigned int SCAN_ROLE = 7;
//static const float THRESHOLD = 0.85;
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
using Execution::GroupAggr;
using Execution::StorageAlloc;
using Execution::MemoryManager;

//----------------------------------------------------------------------
// Grouping and aggregating information.  (Prevent us from tying
// op -> u.GROUP_AGGR.* everywhere
//----------------------------------------------------------------------

/// Number of grouping attributes
static unsigned int numGroupAttrs;

/// Number of aggregated attributes
static unsigned int numAggrAttrs;

/// Positions of the grouping attributes in the input schema
static unsigned int groupPos [MAX_GROUP_ATTRS];

/// Input Columns corresponding to the positions
static unsigned int groupCols [MAX_GROUP_ATTRS];

/// Positions of the aggr. attributes in the input schema
static unsigned int aggrPos [MAX_AGGR_ATTRS];

/// Input Columns corresponding to the aggr. attr positions
static unsigned int aggrCols [MAX_AGGR_ATTRS];

/// The aggr. functions
static AggrFn fn [MAX_AGGR_ATTRS];

/// if set, fn [countPos] == COUNT
static unsigned int countPos;

#ifdef _DM_
static bool countPosSet;
#endif

/// If fn [a] == AVG, then sumPos [a] contains the position of the sum
/// aggr. function used to compute the avg.
static unsigned int sumPos [MAX_AGGR_ATTRS];

/// The output columns
static unsigned int outCols [MAX_ATTRS];

static int initGroupAggrInfo (Physical::Operator *op);
static int getCountPos (Physical::Operator *op);
static int getSumPos (Physical::Operator *op);
static int computeInputTupleLayout (Physical::Operator *op);
static int computeOutputTupleLayout (Physical::Operator *op);

static int getPlusEval (Physical::Operator *op,
						ConstTupleLayout *ct_layout,
						AEval *&eval);

static int getUpdateEval (Physical::Operator *op,
						  ConstTupleLayout *ct_layout,
						  AEval *&eval);

static int getInitEval (Physical::Operator *op,
						ConstTupleLayout *ct_layout,
						AEval *&eval);

static int getMinusEval (Physical::Operator *op,
						 ConstTupleLayout *ct_layout,
						 AEval *&eval);

static int getScanNotReqEval (Physical::Operator *op,
							  BEval *&scanNotReqEval);

static int getEmptyGroupEval (Physical::Operator *op,
							  ConstTupleLayout *ct_layout,
							  BEval *&emptyGroupEval);

static int initOutIndex (Physical::Operator *op,
						 MemoryManager *memMgr,
						 EvalContext *evalContext,
						 HashIndex *idx);

static int initInIndex (Physical::Operator *op,
						MemoryManager *memMgr,
						EvalContext *evalContext,
						HashIndex *idx);

static bool inputSynReq (Physical::Operator *op);

int PlanManagerImpl::inst_aggr (Physical::Operator *op)
{
	int rc;


	ConstTupleLayout *ct_layout;
	bool bInputRel;
	TupleLayout *tupleLayout;
	
	GroupAggr              *groupAggr;
	EvalContext            *evalContext;
	RelationSynopsisImpl   *inSyn;
	HashIndex              *inIdx;
	unsigned int            inScanId;
	RelationSynopsisImpl   *outSyn;
	HashIndex              *outIdx;
	unsigned int            outScanId;
	StorageAlloc           *outStore;
	AEval                  *plusEval;
	AEval                  *minusEval;
	AEval                  *updateEval;
	AEval                  *initEval;
	BEval                  *bScanNotReq;
	BEval                  *emptyGroupEval;
	char                   *constTuple;
	
	bInputRel = !op -> inputs[0] -> bStream;
	
	// Transform the grouping & aggr. information to a form easier to code :)
	if ((rc = initGroupAggrInfo (op)) != 0)
		return rc;
	
	// Populate countPos [] array
	if ((rc = getCountPos (op)) != 0)
		return rc;
	
	// Populate sumPos [] array
	if ((rc = getSumPos (op)) != 0)
		return rc;
	
	// Determine the layout of tuples in the input.  (populate the
	// groupCols[] & aggrCols[] arrays)
	if ((rc = computeInputTupleLayout (op)) != 0)
		return rc;

	// Determine the layout of the output tuple
	if ((rc = computeOutputTupleLayout (op)) != 0)
		return rc;

	// Eval context ...
	evalContext = new EvalContext ();
	
	ct_layout = new ConstTupleLayout ();
	
	// Get the plusEvaluator
	if ((rc = getPlusEval (op, ct_layout, plusEval)) != 0)
		return rc;
	if ((rc = plusEval -> setEvalContext (evalContext)) != 0)
		return rc;
	
	// Get the update Evaluator
	if (bInputRel) {
		if ((rc = getUpdateEval (op, ct_layout, updateEval)) != 0)			
			return rc;
		if ((rc = updateEval -> setEvalContext (evalContext)) != 0)
			return rc;
	}
	else {
		updateEval = 0;
	}
	
	// Get the init evaluator
	if ((rc = getInitEval (op, ct_layout, initEval)) != 0)
		return rc;
	if ((rc = initEval -> setEvalContext (evalContext)) != 0)
		return rc;
	
	// Get the minusEvaluator
	if (bInputRel) {
		if ((rc = getMinusEval (op, ct_layout, minusEval)) != 0)			
			return rc;
		if ((rc = minusEval -> setEvalContext (evalContext)) != 0)
			return rc;
	}
	else {
		minusEval = 0;
	}
	
	// Get the scanNotReq evaluator
	if (bInputRel) {
		if ((rc = getScanNotReqEval (op, bScanNotReq)) != 0)
			return rc;
		if ((rc = bScanNotReq -> setEvalContext (evalContext)) != 0)
			return rc;
	}
	else {
		bScanNotReq = 0;
	}
	
	// Get the empty group evaluator
	if (bInputRel) {
		if ((rc = getEmptyGroupEval (op, ct_layout, emptyGroupEval)) != 0)			
			return rc;
		
		if ((rc = emptyGroupEval -> setEvalContext (evalContext)) != 0)
			return rc;
	}
	else {
		emptyGroupEval = 0;
	}
	
	// Output synopsis
	outSyn = new RelationSynopsisImpl (op -> u.GROUP_AGGR.outSyn -> id,
									   LOG);
	op -> u.GROUP_AGGR.outSyn -> u.relSyn = outSyn;
	
	// Create an index for output synopsis if necessary (there exist
	// grouping attributes)
	if (numGroupAttrs > 0) {
		
		if (numIndexes >= MAX_INDEXES)
			return -1;
		
		outIdx = new HashIndex (numIndexes, LOG);
		indexes [numIndexes ++] = outIdx;
		
		if ((rc = initOutIndex (op, memMgr, evalContext, outIdx)) != 0)
			return rc;
	}
	else {
		outIdx = 0;
	}
	
	// Register a scan with the output synopsis for reading the
	// aggregation tuple for a particular group
	if (outIdx) {
		if ((rc = outSyn -> setIndexScan (0, outIdx, outScanId)) != 0) {
			return rc;
		}
	}
	
	else {
		// Assert: outSyn contains only one tuple
		if ((rc = outSyn -> setScan (0, outScanId)) != 0) {
			return rc;
		}
	}

	if ((rc = outSyn -> setEvalContext (evalContext)) != 0)
		return rc;
	if ((rc = outSyn -> initialize ()) != 0)
		return rc;
	

	inSyn = 0;
	inScanId = 0;
	if (inputSynReq (op)) {

		ASSERT (op -> u.GROUP_AGGR.inSyn);
		ASSERT (op -> u.GROUP_AGGR.inSyn -> kind == REL_SYN);
		
		// create an input synopsis
		inSyn = new RelationSynopsisImpl (op -> u.GROUP_AGGR.inSyn -> id,
										  LOG);
		op -> u.GROUP_AGGR.inSyn -> u.relSyn = inSyn;				
		
		// create an index if necessary
		if (numGroupAttrs > 0) {
			if (numIndexes >= MAX_INDEXES)
				return -1;
			
			inIdx = new HashIndex (numIndexes, LOG);
			indexes [numIndexes ++] = inIdx;
			
			if ((rc = initInIndex (op, memMgr, evalContext, inIdx)) != 0)
				return rc;			
		}
		else {
			inIdx = 0;
		}
		
		if (inIdx) {
			if ((rc = inSyn -> setIndexScan (0, inIdx, inScanId)) != 0)
				return rc;
		}
		
		else {
			if ((rc = inSyn -> setScan (0, inScanId)) != 0)
				return rc;
		}

		if ((rc = inSyn -> setEvalContext (evalContext)) != 0)
			return rc;
		if ((rc = inSyn -> initialize ()) != 0)
			return rc;
	}
	
	if ((rc = getStaticTuple (constTuple,
							  ct_layout -> getTupleLen())) != 0) 
		return rc;
	if ((rc = ct_layout -> genTuple (constTuple)) != 0)
		return rc;
	
	evalContext -> bind (constTuple, CONST_ROLE);
	
	// output Storage allocator
	ASSERT (op -> store);
	ASSERT (op -> store -> kind == REL_STORE);
	tupleLayout = new TupleLayout (op);
	
	if ((rc = inst_rel_store (op -> store, tupleLayout)) != 0)
		return rc;
	outStore = op -> store -> instStore;
	ASSERT (outStore);
	
	groupAggr = new GroupAggr (op -> id, LOG);
	
	if ((rc = groupAggr -> setOutputSynopsis (outSyn, outScanId)) != 0)
		return rc;
	if ((rc = groupAggr -> setInputSynopsis (inSyn, inScanId)) != 0)
		return rc;
	if ((rc = groupAggr -> setEvalContext (evalContext)) != 0)
		return rc;
	if ((rc = groupAggr -> setPlusEvaluator (plusEval)) != 0)
		return rc;
	if ((rc = groupAggr -> setMinusEvaluator (minusEval)) != 0)
		return rc;
	if ((rc = groupAggr -> setInitEvaluator (initEval)) != 0)
		return rc;
	if ((rc = groupAggr -> setEmptyGroupEvaluator (emptyGroupEval)) != 0)
		return rc;
	if ((rc = groupAggr -> setRescanEvaluator (bScanNotReq)) != 0)
		return rc;
	if ((rc = groupAggr -> setUpdateEvaluator (updateEval)) != 0)
		return rc;
	if ((rc = groupAggr -> setOutStore (outStore)) != 0)
		return rc;
	
	op -> instOp = groupAggr;

	delete ct_layout;
	delete tupleLayout;
	
	return 0;
}
	
static int initGroupAggrInfo (Physical::Operator *op)
{
	Attr attr;
	
	numGroupAttrs = op -> u.GROUP_AGGR.numGroupAttrs;
	numAggrAttrs = op -> u.GROUP_AGGR.numAggrAttrs;

	ASSERT (numGroupAttrs < MAX_GROUP_ATTRS);
	ASSERT (numAggrAttrs < MAX_AGGR_ATTRS);
	
	for (unsigned int a = 0 ; a < numGroupAttrs ; a++) {
		attr = op -> u.GROUP_AGGR.groupAttrs [a];
		ASSERT (attr.input == 0);
		groupPos [a] = attr.pos;
	}

	for (unsigned int a = 0 ; a < numAggrAttrs ; a++) {
		attr = op -> u.GROUP_AGGR.aggrAttrs [a];
		ASSERT (attr.input == 0);
		aggrPos [a] = attr.pos;
		fn [a] = op -> u.GROUP_AGGR.fn [a];
	}
	
	return 0;
}
		
static int getCountPos (Physical::Operator *op)
{

#ifdef _DM_
	countPosSet = false;
#endif
	
	for (unsigned int a = 0 ; a < numAggrAttrs ; a++) {
		if (fn [a] == COUNT) {
			countPos = a;

#ifdef _DM_
			countPosSet = true;
#endif
			
			break;
		}
	}
	
#ifdef _DM_
	// We ensure in our earlier stages of processing that there exists
	// only one count per operator (there could be none)
	for (unsigned int a = 0 ; a < numAggrAttrs ; a++) {
		ASSERT ((a == countPos) || (fn [a] != COUNT));
	}
#endif
	
	return 0;
}

static int getSumPos (Physical::Operator *op)
{
	bool bFoundSum;
	
	for (unsigned int a = 0 ; a < numAggrAttrs ; a++) {
		if (fn [a] == AVG) {			
			bFoundSum = false;			
			for (unsigned int b = 0 ; b < numAggrAttrs ; b++) {
				if (fn [b] == SUM && aggrPos [b] == aggrPos [a]) {
					bFoundSum = true;
					sumPos [a] = b;
					break;
				}
			}			
			ASSERT (bFoundSum);
		}
	}
	
	return 0;
}

static int computeInputTupleLayout (Physical::Operator *op)
{
	TupleLayout *inputLayout;
	
	ASSERT (op -> inputs [0]);
	
	inputLayout = new TupleLayout (op -> inputs [0]);

	for (unsigned int g = 0 ; g < numGroupAttrs ; g++)
		groupCols [g] = inputLayout -> getColumn (groupPos [g]);

	for (unsigned int a = 0 ; a < numAggrAttrs ; a++)
		aggrCols [a] = inputLayout -> getColumn (aggrPos [a]);

	delete inputLayout;
	return 0;	
}

static int computeOutputTupleLayout (Physical::Operator *op)
{
	TupleLayout *outputLayout;

	outputLayout = new TupleLayout (op);

	ASSERT (op -> numAttrs == numGroupAttrs + numAggrAttrs);
	
	for (unsigned int a = 0 ; a < op -> numAttrs ; a++)
		outCols [a] = outputLayout -> getColumn (a);

	delete outputLayout;
	return 0;
}

static int getPlusEval (Physical::Operator *op,
						ConstTupleLayout *ct_layout,
						AEval *&eval)
{
	int rc;
	AInstr instr;
	Type type;
	
	eval = new AEval ();
	
	// Copy the group attributes from the new plus tuple to the new output
	// tuple 
	instr.r1 = INPUT_ROLE;
	instr.dr = NEW_OUTPUT_ROLE;	
	for (unsigned int g = 0 ; g < numGroupAttrs ; g++) {
		instr.c1 = groupCols [g];  // source col
		instr.dc = outCols [g];    // destn col
		
		switch (op -> attrTypes [g]) {
		case INT:    instr.op = Execution::INT_CPY; break;
		case FLOAT:  instr.op = Execution::FLT_CPY; break;
		case CHAR:   instr.op = Execution::CHR_CPY; break;
		case BYTE:   instr.op = Execution::BYT_CPY; break;
#ifdef _DM_
		default:
			ASSERT (0);
			break;
#endif
		}
		
		if ((rc = eval -> addInstr (instr)) != 0)
			return rc;
	}
	
	// Generate  the aggr.  attributes.   In this  "pass"  we only  handle
	// non-avg. aggregates.  Avg. aggregates are handled in the next pass.
	// The reason for this is that avg. aggr. are dependant on SUM & COUNT
	// aggr	
	for (unsigned int a = 0 ; a < numAggrAttrs ; a++) {
		type = op -> inputs [0] -> attrTypes [aggrPos [a]];
		
		// Currently we only allow aggrs over integers & floats
		ASSERT (fn [a] == COUNT || type == INT || type == FLOAT);
		
		switch (fn [a]) {
			
		case SUM:

			// operation: ADD
			if (type == INT) 
				instr.op = Execution::INT_ADD;
			else if (type == FLOAT)
				instr.op = Execution::FLT_ADD;
#ifdef _DM_
			else {
				ASSERT (false);
			}
#endif

			// input1: old aggr val
			instr.r1 = OLD_OUTPUT_ROLE;
			instr.c1 = outCols [a + numGroupAttrs];

			// input2: input attr val
			instr.r2 = INPUT_ROLE;
			instr.c2 = aggrCols [a];

			// result: updated aggr val
			instr.dr = NEW_OUTPUT_ROLE;
			instr.dc = outCols [a + numGroupAttrs];

			if ((rc = eval -> addInstr (instr)) != 0)
				return rc;
			break;
			
		case COUNT:
			// operation: COUNT
			instr.op = Execution::INT_ADD;

			// input1: old aggr val (old count)
			instr.r1 = OLD_OUTPUT_ROLE;
			instr.c1 = outCols [a + numGroupAttrs];

			// input2: 1 (a const value)
			instr.r2 = CONST_ROLE;
			rc = ct_layout -> addInt (1, instr.c2);			
			if (rc != 0) return rc;

			// result: updated aggr val
			instr.dr = NEW_OUTPUT_ROLE;
			instr.dc = outCols [a + numGroupAttrs];
			
			if ((rc = eval -> addInstr (instr)) != 0)
				return rc;
			
			break;
			
		case AVG:
			// handled in the next iteration
			break;
			
		case MAX:
			// operation: check for new max + update
			if (type == INT)
				instr.op = Execution::INT_UMX;
			else if (type == FLOAT)
				instr.op = Execution::FLT_UMX;
#ifdef _DM_
			else {
				ASSERT (false);
			}
#endif
			
			// input1: old aggr val
			instr.r1 = OLD_OUTPUT_ROLE;
			instr.c1 = outCols [a + numGroupAttrs];
			
			// input2: input attr val
			instr.r2 = INPUT_ROLE;
			instr.c2 = aggrCols [a];
			
			// result: updated aggr val
			instr.dr = NEW_OUTPUT_ROLE;
			instr.dc = outCols [a + numGroupAttrs];			

			if ((rc = eval -> addInstr (instr)) != 0)
				return rc;
			
			break;
			
		case MIN:
			// operation: check for new min + update
			if (type == INT)
				instr.op = Execution::INT_UMN;
			else if (type == FLOAT)
				instr.op = Execution::FLT_UMN;
#ifdef _DM_
			else {
				ASSERT (false);
			}
#endif

			// input1: old aggr val
			instr.r1 = OLD_OUTPUT_ROLE;
			instr.c1 = outCols [a + numGroupAttrs];

			// input2: input attr val
			instr.r2 = INPUT_ROLE;
			instr.c2 = aggrCols [a];
			
			// result: updated aggr val
			instr.dr = NEW_OUTPUT_ROLE;
			instr.dc = outCols [a + numGroupAttrs];

			if ((rc = eval -> addInstr (instr)) != 0)
				return rc;
			
			break;						
			
#ifdef _DM_
		default:
			ASSERT (0);
			break;
#endif
		}				
	}
	
	for (unsigned int a = 0 ; a < numAggrAttrs ; a++) {		
		type = op -> inputs [0] -> attrTypes [aggrPos [a]];
		
		if (fn [a] != AVG)
			continue;

		// Operation: Note that INT_AVG & FLT_AVG are different from
		// INT_DIV and FLT_DIV.  For example, INT_DIV divides two integers
		// and produces an integer.  INT_AVG divides two integer &
		// produces a float
		
		if (type == INT) {
			instr.op = Execution::INT_AVG;
		}
		
		else if (type == FLOAT) {
			instr.op = Execution::FLT_AVG;
		}
		
#ifdef _DM_
		else {
			ASSERT (0);
		}
#endif
		
		// input1: updated sum aggr.
		instr.r1 = NEW_OUTPUT_ROLE;
		instr.c1 = outCols [sumPos [a] + numGroupAttrs];
		
		// input2: updated count aggr
		instr.r2 = NEW_OUTPUT_ROLE;
		instr.c2 = outCols [countPos + numGroupAttrs];
		
		// result: updated avg
		instr.dr = NEW_OUTPUT_ROLE;
		instr.dc = outCols [a + numGroupAttrs];
		
		if ((rc = eval -> addInstr (instr)) != 0)
			return rc;
	}
	
	return 0;
}

static int getUpdateEval (Physical::Operator *op,
						  ConstTupleLayout *ct_layout,
						  AEval *&eval)
{
	int rc;
	AInstr instr;
	Type type;
	
	eval = new AEval ();
	
	// Generate  the aggr.  attributes.   In this  "pass"  we only  handle
	// non-avg. aggregates.  Avg. aggregates are handled in the next pass.
	// The reason for this is that avg. aggr. are dependant on SUM & COUNT
	// aggr	
	
	for (unsigned int a = 0 ; a < numAggrAttrs ; a++) {
		type = op -> inputs [0] -> attrTypes [aggrPos [a]];
		
		// Currently we only allow aggrs over integers & floats
		ASSERT (fn [a] == COUNT || type == INT || type == FLOAT);
		
		switch (fn [a]) {
			
		case SUM:
			
			// Operation: ADD
			if (type == INT) 
				instr.op = Execution::INT_ADD;
			else if (type == FLOAT)
				instr.op = Execution::FLT_ADD;
			// else: never comes
			
			// input1: input attr val
			instr.r1 = INPUT_ROLE;
			instr.c1 = aggrCols [a];
			
			// input2: existing aggr val
			instr.r2 = NEW_OUTPUT_ROLE;
			instr.c2 = outCols [a + numGroupAttrs];
			
			// result: updated aggr val
			instr.dr = NEW_OUTPUT_ROLE;
			instr.dc = outCols [a + numGroupAttrs];

			if ((rc = eval -> addInstr (instr)) != 0)
				return rc;
			
			break;
			
		case COUNT:
			
			// Operation: ADD			
			instr.op = Execution::INT_ADD;
			
			// input1: existing aggr val
			instr.r1 = NEW_OUTPUT_ROLE;
			instr.c1 = outCols [a + numGroupAttrs];
			
			// input2: 1 (const val)
			instr.r2 = CONST_ROLE;
			rc = ct_layout -> addInt (1, instr.c2);
			if (rc != 0) return rc;
			
			// result: updated count
			instr.dr = NEW_OUTPUT_ROLE;
			instr.dc = outCols [a + numGroupAttrs];

			if ((rc = eval -> addInstr (instr)) != 0)
				return rc;
			
			break;
			
		case AVG:
			break;
			
		case MAX:
			// operation: check for new max + update
			if (type == INT)
				instr.op = Execution::INT_UMX;
			else if (type == FLOAT)
				instr.op = Execution::FLT_UMX;
			// else: never comes
			
			// input1: input attr val
			instr.r1 = INPUT_ROLE;
			instr.c1 = aggrCols [a];

			// input2: existing aggr val
			instr.r2 = NEW_OUTPUT_ROLE;
			instr.c2 = outCols [a + numGroupAttrs];
			
			// result: updated aggr val
			instr.dr = NEW_OUTPUT_ROLE;
			instr.dc = outCols [a + numGroupAttrs];			

			if ((rc = eval -> addInstr (instr)) != 0)
				return rc;
			
			break;
			
		case MIN:
			if (type == INT)
				instr.op = Execution::INT_UMN;
			else if (type == FLOAT)
				instr.op = Execution::FLT_UMN;
			// else: never comes

			// input1: input attr val
			instr.r1 = INPUT_ROLE;
			instr.c1 = aggrCols [a];

			// input2: existing aggr val
			instr.r2 = NEW_OUTPUT_ROLE;
			instr.c2 = outCols [a + numGroupAttrs];

			// result: updated aggr val
			instr.dr = NEW_OUTPUT_ROLE;
			instr.dc = outCols [a + numGroupAttrs];						
			
			if ((rc = eval -> addInstr (instr)) != 0)
				return rc;
			break;						
			
#ifdef _DM_
		default:
			ASSERT (0);
			break;
#endif
		}		
	}

	// In this "pass" we compute the avg. aggrs
	for (unsigned int a = 0 ; a < numAggrAttrs ; a++) {		
		
		if (fn [a] != AVG)
			continue;
		
		type = op -> inputs [0] -> attrTypes [aggrPos [a]];
		
		// Operation: AVG
		if (type == INT) {
			instr.op = Execution::INT_AVG;
		}
		else if (type == FLOAT) {
			instr.op = Execution::FLT_AVG;
		}
#ifdef _DM_
		else {
			ASSERT (0);
		}
#endif		
		
		// input1: updated sum aggr.
		instr.r1 = NEW_OUTPUT_ROLE;
		instr.c1 = outCols [sumPos [a] + numGroupAttrs];
		
		// input2: updated count aggr
		instr.r2 = NEW_OUTPUT_ROLE;
		instr.c2 = outCols [countPos + numGroupAttrs];
		
		// result: updated avg
		instr.dr = NEW_OUTPUT_ROLE;
		instr.dc = outCols [a + numGroupAttrs];
		
		if ((rc = eval -> addInstr (instr)) != 0)
			return rc;
	}
	
	return 0;
}

static int getInitEval (Physical::Operator *op,
						ConstTupleLayout *ct_layout,
						AEval *&eval)
{
	int rc;

	AInstr instr;
	Type type;
	
	eval = new AEval ();
	
	// Copy the group attributes from the new plus tuple to the new output
	// tuple 
	for (unsigned int g = 0 ; g < numGroupAttrs ; g++) {

		// Operation: copy
		switch (op -> attrTypes [g]) {
		case INT:    instr.op = Execution::INT_CPY; break;
		case FLOAT:  instr.op = Execution::FLT_CPY; break;
		case CHAR:   instr.op = Execution::CHR_CPY; break;
		case BYTE:   instr.op = Execution::BYT_CPY; break;
			
#ifdef _DM_
		default:
			ASSERT (0);
			break;
#endif
		}
		
		// input1 (source): new input attr
		instr.r1 = INPUT_ROLE;
		instr.c1 = groupCols [g];
		
		// result (dest): copy of the new attr
		instr.dr = NEW_OUTPUT_ROLE;
		instr.dc = outCols [g];
		
		if ((rc = eval -> addInstr (instr)) != 0)
			return rc;
	}
	
	for (unsigned int a = 0 ; a < numAggrAttrs ; a++) {
		
		type = op -> inputs [0] -> attrTypes [aggrPos [a]];
		
		ASSERT (type == INT || type == FLOAT || fn [a] == COUNT);
		
		switch (fn [a]) {
		case SUM:
		case MAX:
		case MIN:
			
			// Operation: Copy
			if (type == INT) {
				instr.op = Execution::INT_CPY;
			}
			
			else if (type == FLOAT) {
				instr.op = Execution::FLT_CPY;
			}

			// Source: input attr
			instr.r1 = INPUT_ROLE;
			instr.c1 = aggrCols [a];

			// Destn: 
			instr.dr = NEW_OUTPUT_ROLE;
			instr.dc = outCols [a + numGroupAttrs];
			
			break;
			
		case COUNT:
			
			// Operation: copy
			instr.op = Execution::INT_CPY;
			
			// source: 1 (const val)
			instr.r1 = CONST_ROLE;
			rc = ct_layout -> addInt (1, instr.c1);
			if (rc != 0) return rc;
			
			// Destn: 
			instr.dr = NEW_OUTPUT_ROLE;
			instr.dc = outCols [a + numGroupAttrs];
			
			break;
			
		case AVG:

			// Operation
			if (type == INT) {
				instr.op = Execution::INT_AVG;
			}
			else {
				instr.op = Execution::FLT_AVG;
			}

			// input1:
			instr.r1 = INPUT_ROLE;
			instr.c1 = aggrCols [a];

			// input2: (const val 1 )
			instr.r2 = CONST_ROLE;
			rc = ct_layout -> addInt (1, instr.c2);
			if (rc != 0) return rc;

			// Destn:
			instr.dr = NEW_OUTPUT_ROLE;
			instr.dc = outCols [a + numGroupAttrs];

			break;

#ifdef _DM_
		default:
			ASSERT (0);
			break;
#endif
		}
		
		if ((rc = eval -> addInstr (instr)) != 0)
			return rc;
	}
	
	return 0;
}
	
static int getMinusEval (Physical::Operator *op,
						 ConstTupleLayout *ct_layout,
						 AEval *&eval)
{
	int rc;
	AInstr instr;
	Type type;
	
	eval = new AEval ();	
	
	// Copy the grouping attributes from the new input tuple to the new
	// output aggregated tuple
	// Copy the group attributes from the new plus tuple to the new output
	// tuple 
	for (unsigned int g = 0 ; g < numGroupAttrs ; g++) {
		
		// Operation: copy
		switch (op -> attrTypes [g]) {
		case INT:    instr.op = Execution::INT_CPY; break;
		case FLOAT:  instr.op = Execution::FLT_CPY; break;
		case CHAR:   instr.op = Execution::CHR_CPY; break;
		case BYTE:   instr.op = Execution::BYT_CPY; break;
			
#ifdef _DM_
		default:
			ASSERT (0);
			break;
#endif
		}
		
		// input1 (source): new input attr
		instr.r1 = INPUT_ROLE;
		instr.c1 = groupCols [g];
		
		// result (dest): copy of the new attr
		instr.dr = NEW_OUTPUT_ROLE;
		instr.dc = outCols [g];
		
		if ((rc = eval -> addInstr (instr)) != 0)
			return rc;
	}	
	
	// Generate  the aggr.  attributes.   In this  "pass"  we only  handle
	// non-avg. aggregates.  Avg. aggregates are handled in the next pass.
	// The reason for this is that avg. aggr. are dependant on SUM & COUNT
	// aggr	
	for (unsigned int a = 0 ; a < numAggrAttrs ; a++) {
		type = op -> inputs [0] -> attrTypes [aggrPos [a]];
		
		// Currently we only allow aggrs over integers & floats
		ASSERT (fn [a] == COUNT || type == INT || type == FLOAT);
		
		switch (fn [a]) {
			
		case SUM:
			
			// operation: SUB
			if (type == INT) 
				instr.op = Execution::INT_SUB;
			else if (type == FLOAT)
				instr.op = Execution::FLT_SUB;
			// else: never comes
			
			// input1: old aggr val
			instr.r1 = OLD_OUTPUT_ROLE;
			instr.c1 = outCols [a + numGroupAttrs];
			
			// input2: input attr val
			instr.r2 = INPUT_ROLE;
			instr.c2 = aggrCols [a];
			
			// result: updated aggr val
			instr.dr = NEW_OUTPUT_ROLE;
			instr.dc = outCols [a + numGroupAttrs];
			
			if ((rc = eval -> addInstr (instr)) != 0)
				return rc;
			
			break;
			
		case COUNT:
			// operation: COUNT
			instr.op = Execution::INT_SUB;
			
			// input1: old aggr val (old count)
			instr.r1 = OLD_OUTPUT_ROLE;
			instr.c1 = outCols [a + numGroupAttrs];
			
			// input2: 1 (a const value)
			instr.r2 = CONST_ROLE;
			rc = ct_layout -> addInt (1, instr.c2);			
			if (rc != 0) return rc;
			
			// result: updated aggr val
			instr.dr = NEW_OUTPUT_ROLE;
			instr.dc = outCols [a + numGroupAttrs];
			
			if ((rc = eval -> addInstr (instr)) != 0)
				return rc;
			
			break;
			
		case AVG:
			// handled in the next iteration
			break;
			
		case MAX:
		case MIN:

			// Operation: copy
			switch (op -> attrTypes [a + numGroupAttrs]) {
			case INT:    instr.op = Execution::INT_CPY; break;
			case FLOAT:  instr.op = Execution::FLT_CPY; break;
			case CHAR:   instr.op = Execution::CHR_CPY; break;
			case BYTE:   instr.op = Execution::BYT_CPY; break;
				
#ifdef _DM_
			default:
				ASSERT (0);
				break;
#endif
			}

			instr.r1 = OLD_OUTPUT_ROLE;
			instr.c1 = outCols [a + numGroupAttrs];

			instr.dr = NEW_OUTPUT_ROLE;
			instr.dc = outCols [a + numGroupAttrs];			

			if ((rc = eval -> addInstr (instr)) != 0)
				return rc;
			
			break;						
			
#ifdef _DM_
		default:
			ASSERT (0);
			break;
#endif
		}		
	}	
	
	// In this "pass" we compute the avg. aggrs
	for (unsigned int a = 0 ; a < numAggrAttrs ; a++) {		
		
		if (fn [a] != AVG)
			continue;
		
		type = op -> inputs [0] -> attrTypes [aggrPos [a]];
		
		// Operation: AVG
		if (type == INT) {
			instr.op = Execution::INT_AVG;
		}
		else if (type == FLOAT) {
			instr.op = Execution::FLT_AVG;
		}
#ifdef _DM_
		else {
			ASSERT (0);
		}
#endif		
		
		// input1: updated sum aggr.
		instr.r1 = NEW_OUTPUT_ROLE;
		instr.c1 = outCols [sumPos [a] + numGroupAttrs];
		
		// input2: updated count aggr
		instr.r2 = NEW_OUTPUT_ROLE;
		instr.c2 = outCols [countPos + numGroupAttrs];
		
		// result: updated avg
		instr.dr = NEW_OUTPUT_ROLE;
		instr.dc = outCols [a + numGroupAttrs];
		
		if ((rc = eval -> addInstr (instr)) != 0)
			return rc;
	}
	
	return 0;
}	

static int getScanNotReqEval (Physical::Operator *op,
							  BEval *&scanNotReqEval)
{
	int rc;
	BInstr instr;
	Type type;
	
	scanNotReqEval = new BEval();
	
	for (unsigned int a = 0 ; a < numAggrAttrs ; a++) {
		
		// Only max & min attributes can lead to rescan
		if (fn[a] != MAX && fn[a] != MIN)
			continue;
		
		type = op -> inputs [0] -> attrTypes [aggrPos [a]];
		
		// Operator
		if (type == INT)
			instr.op = Execution::INT_NE;
		else if (type == FLOAT)
			instr.op = Execution::FLT_NE;
		
#ifdef _DM_
		else {
			ASSERT (0);
		}
#endif
		
		// LHS: old aggr attr
		instr.r1 = OLD_OUTPUT_ROLE;
		instr.c1 = outCols [a + numGroupAttrs];
		instr.e1 = 0;
		
		// RHS: new input tuple
		instr.r2 = INPUT_ROLE;
		instr.c2 = aggrCols [a];
		instr.e2 = 0;
		
		if ((rc = scanNotReqEval -> addInstr (instr)) != 0)
			return rc;
	}
	
	return 0;
}

static int getEmptyGroupEval (Physical::Operator *op,
							  ConstTupleLayout *ct_layout,
							  BEval *&emptyGroupEval)
{
	int rc;
	BInstr instr;
	
	ASSERT (countPosSet);
	
	emptyGroupEval = new BEval ();
	
	instr.op = Execution::INT_EQ;

	// lhs
	instr.r1 = OLD_OUTPUT_ROLE;
	instr.c1 = outCols [countPos + numGroupAttrs];
	instr.e1 = 0;
	
	// rhs
	instr.r2 = CONST_ROLE;
	if ((rc = ct_layout -> addInt (1, instr.c2)) != 0)
		return rc;
	instr.e2 = 0;
	
	if ((rc = emptyGroupEval -> addInstr (instr)) != 0)
		return rc;
	
	return 0;
}

static int initOutIndex (Physical::Operator *op,
						 MemoryManager *memMgr,
						 EvalContext *evalContext,
						 HashIndex *idx)
{
	int rc;	
	HEval *updateHash, *scanHash;
	HInstr hinstr;
	BEval *keyEqual;
	BInstr binstr;
	Type attrType;
	
	updateHash = new HEval ();
	for (unsigned int g = 0 ; g < numGroupAttrs ; g++) {
		
		hinstr.type = op -> attrTypes [g];
		hinstr.r = UPDATE_ROLE;
		hinstr.c = outCols [g];
		
		if ((rc = updateHash -> addInstr (hinstr)) != 0)
			return rc;
	}

	scanHash = new HEval ();
	for (unsigned int g = 0 ; g < numGroupAttrs ; g++) {

		hinstr.type = op -> attrTypes [g];
		hinstr.r = INPUT_ROLE;
		hinstr.c = groupCols [g];
		
		if ((rc = scanHash -> addInstr (hinstr)) != 0)
			return rc;
	}
	
	keyEqual = new BEval ();
	for (unsigned int g = 0 ; g < numGroupAttrs ; g++) {

		attrType = op -> attrTypes [g];

		switch (attrType) {
		case INT:    binstr.op = Execution::INT_EQ; break;
		case FLOAT:  binstr.op = Execution::FLT_EQ; break;
		case CHAR:   binstr.op = Execution::CHR_EQ; break;
		case BYTE:   binstr.op = Execution::BYT_EQ; break;

#ifdef _DM_
		default:
			ASSERT (0);
			break;
#endif
		}

		// Lhs
		binstr.r1 = INPUT_ROLE;
		binstr.c1 = groupCols [g];
		binstr.e1 = 0;
		
		// Rhs
		binstr.r2 = SCAN_ROLE;
		binstr.c2 = outCols [g];
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

static int initInIndex (Physical::Operator *op,
						MemoryManager *memMgr,
						EvalContext *evalContext,
						HashIndex *idx)
{
	int rc;

	HEval *updateHash, *scanHash;
	HInstr hinstr;
	BEval *keyEqual;
	BInstr binstr;
	Type attrType;
	
	updateHash = new HEval ();
	for (unsigned int g = 0 ; g < numGroupAttrs ; g++) {
		
		hinstr.type = op -> attrTypes [g];		
		hinstr.r = UPDATE_ROLE;
		hinstr.c = groupCols [g];
		
		if ((rc = updateHash -> addInstr (hinstr)) != 0)
			return rc;
	}
	
	scanHash = new HEval ();
	for (unsigned int g = 0 ; g < numGroupAttrs ; g++) {
		hinstr.type = op -> attrTypes [g];		
		hinstr.r = INPUT_ROLE;
		hinstr.c = groupCols [g];
		
		if ((rc = updateHash -> addInstr (hinstr)) != 0)
			return rc;
	}
	
	keyEqual = new BEval ();
	for (unsigned int g = 0 ; g < numGroupAttrs ; g++) {
		
		attrType = op -> attrTypes [g];
		
		switch (attrType) {
		case INT:    binstr.op = Execution::INT_EQ; break;
		case FLOAT:  binstr.op = Execution::FLT_EQ; break;
		case CHAR:   binstr.op = Execution::CHR_EQ; break;
		case BYTE:   binstr.op = Execution::BYT_EQ; break;
			
#ifdef _DM_
		default:
			ASSERT (0);
			break;
#endif
		}
		
		// lhs
		binstr.r1 = INPUT_ROLE;
		binstr.c1 = groupCols [g];
		binstr.e1 = 0;
		
		// rhs
		binstr.r2 = SCAN_ROLE;
		binstr.c2 = groupCols [g];
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
		
static bool inputSynReq (Physical::Operator *op)
{
	if (op -> inputs [0] -> bStream)
		return false;
	
	for (unsigned int a = 0 ; a < numAggrAttrs ; a++)
		if (fn [a] == MAX || fn [a] == MIN)
			return true;
	
	return false;
}
			
