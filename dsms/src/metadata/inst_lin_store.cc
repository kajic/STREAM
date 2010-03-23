#ifndef _PLAN_MGR_IMPL_
#include "metadata/plan_mgr_impl.h"
#endif

#ifndef _LIN_STORE_IMPL_
#include "execution/stores/lin_store_impl.h"
#endif

#ifndef _HASH_INDEX_
#include "execution/indexes/hash_index.h"
#endif

#ifndef _EVAL_CONTEXT_
#include "execution/internals/eval_context.h"
#endif

#ifndef _HEVAL_
#include "execution/internals/heval.h"
#endif

#ifndef _BEVAL_
#include "execution/internals/aeval.h"
#endif

using namespace Metadata;
using Execution::MemoryManager;
using Execution::LinStoreImpl;
using Execution::HashIndex;
using Execution::EvalContext;
using Execution::HEval;
using Execution::HInstr;
using Execution::BEval;
using Execution::BInstr;

static const unsigned int MAX_LINEAGE = 2;

static const unsigned int LIN_ROLE = 2;
static const unsigned int UPDATE_ROLE = 6;
static const unsigned int SCAN_ROLE = 7;
extern double INDEX_THRESHOLD;

/// Number of lineages
static unsigned int numLineage;
static unsigned int linCols [MAX_LINEAGE];

static int initIdx (TupleLayout *dataLayout,
					EvalContext *evalContext,
					MemoryManager *memMgr,
					HashIndex *index);

int PlanManagerImpl::inst_lin_store (Physical::Store *store,
									 TupleLayout *dataLayout)
{
	int rc;
	unsigned int tupleLen;
	unsigned int usageCol, nextCol, prevCol, refCountCol;
	
	LinStoreImpl *linStore;
	EvalContext *evalContext;
	HashIndex *idx;
	char *linTuple;
	
	ASSERT (store);
	ASSERT (store -> kind == LIN_STORE);

	numLineage = store -> u.LIN_STORE.numLineage;
	ASSERT (numLineage <= MAX_LINEAGE);
	
	//----------------------------------------------------------------------
	// Determine the layout of the storage tuples including the metadata
	// with each tuple.
	//----------------------------------------------------------------------
	
	// Usage column
	if ((rc = dataLayout -> addFixedLenAttr (INT, usageCol)) != 0)
		return rc;
	
	// "Next" pointer column
	if ((rc = dataLayout -> addCharPtrAttr (nextCol)) != 0)
		return rc;

	// "Prev" pointer column
	if ((rc = dataLayout -> addCharPtrAttr (prevCol)) != 0)
		return rc;
	
	// Refcount column
	if ((rc = dataLayout -> addFixedLenAttr (INT, refCountCol)) != 0)
		return rc;
	
	// Lineage columns	
	for (unsigned int l = 0 ; l < store -> u.LIN_STORE.numLineage ; l++) {
		if ((rc = dataLayout -> addFixedLenAttr (INT, linCols [l])) != 0) {
			return rc;
		}
	}
	
	// Length of the entire tuple
	tupleLen = dataLayout -> getTupleLen ();
	
	// Shared evaluation context
	evalContext = new EvalContext ();
	
	// Create the index over lineage cols
	if (numIndexes >= MAX_INDEXES)
		return -1;	
	idx = new HashIndex (numIndexes, LOG);
	indexes [numIndexes ++] = idx;		
	if ((rc = initIdx (dataLayout, evalContext, memMgr, idx)) != 0)
		return rc;
	
	// Generate the static lineage tuple
	if ((rc = getStaticTuple (linTuple, tupleLen)) != 0)
		return rc;
	evalContext -> bind (linTuple, LIN_ROLE);
	
	// Create the store object
	linStore = new LinStoreImpl (store -> id, LOG);

	if ((rc = linStore -> setMemoryManager (memMgr)) != 0)
		return rc;	
	if ((rc = linStore -> setTupleLen (tupleLen)) != 0)
		return rc;
	if ((rc = linStore -> setNextCol (nextCol)) != 0)
		return rc;
	if ((rc = linStore -> setPrevCol (prevCol)) != 0)
		return rc;
	if ((rc = linStore -> setUsageCol (usageCol)) != 0)
		return rc;
	if ((rc = linStore -> setRefCountCol (refCountCol)) != 0)
		return rc;
	if ((rc = linStore -> setLineageTuple (linTuple)) != 0)
		return rc;
	
	for (unsigned int l = 0 ; l < store -> u.LIN_STORE.numLineage ; l++) {
		if ((rc = linStore -> addLineage (linCols [l])) != 0) {
			return rc;
		}
	}
	
	if ((rc = linStore -> setEvalContext (evalContext)) != 0)
		return rc;
	
	if ((rc = linStore -> setIndex (idx)) != 0)
		return rc;
	
	if ((rc = linStore -> setNumStubs (store -> numStubs)) != 0)
		return rc;
	
	if ((rc = linStore -> initialize ()) != 0)
		return rc;
	
	store -> instStore = linStore;
	
	return 0;
}

static int initIdx (TupleLayout *dataLayout,
					EvalContext *evalContext,
					MemoryManager *memMgr,
					HashIndex *idx)
{
	int rc;

	HEval *updateHash, *scanHash;
	BEval *keyEqual;
	HInstr hinstr;
	BInstr binstr;
	
	updateHash = new HEval ();
	for (unsigned int l = 0 ; l < numLineage ; l++) {

		hinstr.type = INT;
		hinstr.r = UPDATE_ROLE;
		hinstr.c = linCols [l];

		if ((rc = updateHash -> addInstr (hinstr)) != 0)
			return rc;
	}
	if ((rc = updateHash -> setEvalContext (evalContext)) != 0)
		return rc;

	scanHash = new HEval ();
	for (unsigned int l = 0 ; l < numLineage ; l++) {
		
		hinstr.type = INT;
		hinstr.r = LIN_ROLE;
		hinstr.c = linCols [l];

		if ((rc = scanHash -> addInstr (hinstr)) != 0)
			return rc;
	}
	if ((rc = scanHash -> setEvalContext (evalContext)) != 0)
		return rc;
	
	keyEqual = new BEval ();
	for (unsigned int l = 0 ; l < numLineage ; l++) {

		binstr.op = Execution::INT_EQ;
		
		binstr.r1 = SCAN_ROLE;
		binstr.c1 = linCols [l];
		binstr.e1 = 0;

		binstr.r2 = LIN_ROLE;
		binstr.c2 = linCols [l];
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
	
