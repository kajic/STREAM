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

#ifndef _REL_SOURCE_
#include "execution/operators/rel_source.h"
#endif

#ifndef _STORE_ALLOC_
#include "execution/stores/store_alloc.h"
#endif

static const unsigned int SCRATCH_ROLE = 0;
static const unsigned int CONST_ROLE = 1;
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
using Execution::RelSource;
using Interface::TableSource;
using Execution::StorageAlloc;

static int initIndex (Physical::Operator *op,
					  MemoryManager      *memMgr,
					  EvalContext        *evalContext,
					  HashIndex          *idx);

int PlanManagerImpl::inst_rel_source (Physical::Operator *op)
{
	int rc;

	TupleLayout *tupleLayout;
	EvalContext *evalContext;
	RelationSynopsisImpl *outSyn;
	unsigned int scanId;
	HashIndex *idx;
	RelSource *relSource;
	TableSource *source;
	StorageAlloc *outStore;
	unsigned int dataCol;
	char *minusTuple;
	
	// Evaluation context
	evalContext = new EvalContext ();
	
	// Create the output synopsis
	ASSERT (op -> u.RELN_SOURCE.outSyn);
	ASSERT (op -> u.RELN_SOURCE.outSyn -> kind == REL_SYN);	
	outSyn = new RelationSynopsisImpl (op -> u.RELN_SOURCE.outSyn -> id,
									   LOG);
	op -> u.RELN_SOURCE.outSyn -> u.relSyn = outSyn;

	// Create an index over this synopsis
	if (numIndexes > MAX_INDEXES)
		return -1;	
	idx = new HashIndex (numIndexes, LOG);
	indexes [numIndexes ++] = idx;
	
	if ((rc = initIndex (op, memMgr, evalContext, idx)) != 0)
		return rc;
	
	// Equality scan 
	if ((rc = outSyn -> setIndexScan (0, idx, scanId)) != 0)
		return rc;
	if ((rc = outSyn -> setEvalContext (evalContext)) != 0)
		return rc;
	if ((rc = outSyn -> initialize ()) != 0)
		return rc;	
	
	// Table Source
	source = baseTableSources [op -> u.RELN_SOURCE.srcId];

	relSource = new RelSource (op -> id, LOG);
		
	if ((rc = relSource -> setSource (source)) != 0)
		return rc;

	if ((rc = relSource -> setSynopsis (outSyn)) != 0)
		return rc;

	if ((rc = relSource -> setScan (scanId)) != 0)
		return rc;

	if ((rc = relSource -> setEvalContext (evalContext)) != 0)
		return rc;

	tupleLayout = new TupleLayout (op);
	
	// Storage allocator
	ASSERT (op -> store -> kind == REL_STORE);

	if ((rc = inst_rel_store (op -> store, tupleLayout)) != 0)
		return rc;

	outStore = op -> store -> instStore;
	ASSERT (outStore);	

	if ((rc = relSource -> setStore (outStore)) != 0)
		return rc;

	// Static minus tuple
	if ((rc = getStaticTuple (minusTuple, tupleLayout -> getTupleLen())) != 0)
		return rc;
	
	if ((rc = relSource -> setMinusTuple (minusTuple)) != 0)
		return rc;
	
	for (unsigned int a = 0 ; a < op -> numAttrs ; a++) {

		dataCol = tupleLayout -> getColumn (a);
		if ((rc = relSource -> addAttr (op -> attrTypes [a],
										op -> attrLen [a],
										dataCol)) != 0)
			return rc;
		
	}
	delete tupleLayout;

	if ((rc = relSource -> initialize ()) != 0)
		return rc;
	
	op -> instOp = relSource;
	
	return 0;
}

static int initIndex (Physical::Operator *op,
					  MemoryManager      *memMgr,
					  EvalContext        *evalContext,
					  HashIndex          *idx)
{
	int rc;
	TupleLayout *tupleLayout;
	HEval *updateHash, *scanHash;
	HInstr hinstr;
	BEval *keyEqual;
	BInstr binstr;

	tupleLayout = new TupleLayout (op);
	
	updateHash = new HEval ();
	for (unsigned int a = 0 ; a < op -> numAttrs ; a++) {

		hinstr.type = op -> attrTypes [a];
		hinstr.r = UPDATE_ROLE;
		hinstr.c = tupleLayout -> getColumn (a);

		if ((rc = updateHash -> addInstr (hinstr)) != 0)
			return rc;
	}

	if ((rc = updateHash -> setEvalContext (evalContext)) != 0)
		return rc;
	
	
	scanHash = new HEval ();
	for (unsigned int a = 0 ; a < op -> numAttrs ; a++) {
		
		hinstr.type = op -> attrTypes [a];
		hinstr.r = UPDATE_ROLE;
		hinstr.c = tupleLayout -> getColumn (a);
		
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

		// lhs
		binstr.r1 = INPUT_ROLE;
		binstr.c1 = tupleLayout -> getColumn (a);
		binstr.e1 = 0;
		
		// rhs
		binstr.r2 = SCAN_ROLE;
		binstr.c2 = tupleLayout -> getColumn (a);
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

	delete tupleLayout;
	
	return 0;
}
