#ifndef _PLAN_MGR_IMPL_
#include "metadata/plan_mgr_impl.h"
#endif

#ifndef _CONSTANTS_
#include "common/constants.h"
#endif

#ifndef _EVAL_CONTEXT_
#include "execution/internals/eval_context.h"
#endif

#ifndef _AEVAL_
#include "execution/internals/aeval.h"
#endif

#ifndef _HASH_INDEX_
#include "execution/indexes/hash_index.h"
#endif

#ifndef _PWIN_STORE_IMPL_
#include "execution/stores/pwin_store_impl.h"
#endif

static const unsigned int UPDATE_ROLE = 6;
static const unsigned int SCAN_ROLE = 7;
extern double INDEX_THRESHOLD;

static const unsigned int DATA_ROLE = 2;
static const unsigned int HEADER_ROLE = 3;

using namespace Metadata;
using Execution::EvalContext;
using Execution::AEval;
using Execution::HEval;
using Execution::BEval;
using Execution::AInstr;
using Execution::BInstr;
using Execution::HInstr;
using Execution::PwinStoreImpl;
using Execution::HashIndex;

int PlanManagerImpl::inst_pwin_store (Physical::Store *store)
{
	int rc;
	Physical::Operator *window, *child;
	Physical::Attr attr;
	Type attrType;
	unsigned int attrLen;
	
	unsigned int numPartnAttrs;
	
	TupleLayout *hdrTupLayout, *dataTupLayout;
	unsigned int hdrTupleLen;
	unsigned int hdrCols [MAX_ATTRS];
	unsigned int hdrOldestCol, hdrNewestCol, hdrCountCol;
	unsigned int dataTupleLen;
	unsigned int dataNextCol, dataUsageCol, dataRefCountCol;
	unsigned int dataCols [MAX_ATTRS];
	
	EvalContext *evalContext;
	AEval *copyEval;
	AInstr ainstr;
	PwinStoreImpl *pwinStore;
	
	HashIndex *idx;
	HEval *updateHashEval, *scanHashEval;
	HInstr hinstr;
	BEval *scanKeyEqual;
	BInstr binstr;
	
	ASSERT (store);
	ASSERT (store -> kind == PARTN_WIN_STORE);
	ASSERT (store -> ownOp);
	ASSERT (store -> ownOp -> u.PARTN_WIN.numPartnAttrs > 0);
	ASSERT (store -> ownOp -> u.PARTN_WIN.numPartnAttrs < MAX_ATTRS);
	
	numPartnAttrs = store -> ownOp -> u.PARTN_WIN.numPartnAttrs;
	
	// Window operator that owns the store
	window = store -> ownOp;
	
	// Child operator of the owning partition window operator
	child = window -> inputs [0];
	
	// Determine the header tuple layout.  A header tuple consists of the
	// partition attributes, a char pointer (current oldest data tuple in
	// the partition), and another char pointer (current newest data
	// tuple).
	hdrTupLayout = new TupleLayout ();
	
	// Partition attributes
	for (unsigned int a = 0 ; a < numPartnAttrs ; a++) {	   
		
		// Partition attribute
		attr = window -> u.PARTN_WIN.partnAttrs [a];
		ASSERT (attr.input == 0);
		ASSERT (child -> numAttrs > attr.pos);
		attrType = child -> attrTypes [attr.pos];
		attrLen = child -> attrLen [attr.pos];
		
		if ((rc = hdrTupLayout -> addAttr (attrType, attrLen,
										   hdrCols [a])) != 0)
			return rc;
	}
	
	// Oldest partition tuple column
	if ((rc = hdrTupLayout -> addCharPtrAttr (hdrOldestCol)) != 0)
		return rc;
	
	if ((rc = hdrTupLayout -> addCharPtrAttr (hdrNewestCol)) != 0)
		return rc;
	
	if ((rc = hdrTupLayout -> addFixedLenAttr (INT, hdrCountCol)) != 0)
		return rc;
	
	// Length of the header tuple
	hdrTupleLen = hdrTupLayout -> getTupleLen ();
	
	delete hdrTupLayout;
	
	// Determine the data tuple layout.  A data tuple consists of the
	// actual data attributes, a data usage column (int), a char pointer
	// column (for next pointers)
	dataTupLayout = new TupleLayout ();
	for (unsigned int a = 0 ; a < child -> numAttrs ; a ++)
		if ((rc = dataTupLayout -> addAttr (child -> attrTypes [a],
											child -> attrLen [a],
											dataCols [a])) != 0)
			return rc;
	
	// usage col
	if ((rc = dataTupLayout -> addFixedLenAttr (INT, dataUsageCol)) != 0)
		return rc;
	
	// next col
	if ((rc = dataTupLayout -> addCharPtrAttr (dataNextCol)) != 0)
		return rc;

	// refcount col
	if ((rc = dataTupLayout -> addFixedLenAttr (INT, dataRefCountCol))
		!= 0)		
		return rc;
	
	// Data tuple length
	dataTupleLen = dataTupLayout -> getTupleLen ();
	
	delete dataTupLayout;
	
	// Shared evaluation context between the store, its index, and its
	// various evaluators
	evalContext = new EvalContext ();

	// Copy evaluator: copies the partition attributes from a data tuple
	// to a header tuple
	copyEval = new AEval();
	ainstr.r1 = DATA_ROLE;
	ainstr.dr = HEADER_ROLE;	
	
	if ((rc = copyEval -> setEvalContext (evalContext)) != 0)
		return rc;
	
	for (unsigned int a = 0 ; a < numPartnAttrs ; a++) {
		
		attr = window -> u.PARTN_WIN.partnAttrs [a];
		attrType = child -> attrTypes [attr.pos];
		
		switch (attrType) {
		case INT:   ainstr.op = Execution::INT_CPY; break;
		case FLOAT: ainstr.op = Execution::FLT_CPY; break;
		case CHAR:  ainstr.op = Execution::CHR_CPY; break;
		case BYTE:  ainstr.op = Execution::BYT_CPY; break;
#ifdef _DM_
		default:
			ASSERT(0);
			break;
#endif
		}
		
		ainstr.c1 = dataCols [attr.pos];
		ainstr.dc = hdrCols [a];
		
		if ((rc = copyEval -> addInstr (ainstr)) != 0)
			return rc;														 
	}
	
	// Create & initialize the store
	pwinStore = new PwinStoreImpl (store -> id, LOG);

	if ((rc = pwinStore -> setMemoryManager (memMgr)) != 0)
		return rc;
	
	if ((rc = pwinStore -> setHdrTupleLen (hdrTupleLen)) != 0)
		return rc;

	if ((rc = pwinStore -> setOldestCol (hdrOldestCol)) != 0)
		return rc;
	
	if ((rc = pwinStore -> setNewestCol (hdrNewestCol)) != 0)
		return rc;

	if ((rc = pwinStore -> setCountCol (hdrCountCol)) != 0)
		return rc;
	
	if ((rc = pwinStore -> setDataTupleLen (dataTupleLen)) != 0)
		return rc;

	if ((rc = pwinStore -> setUsageCol (dataUsageCol)) != 0)
		return rc;
	
	if ((rc = pwinStore -> setNextCol (dataNextCol)) != 0)
		return rc;

	if ((rc = pwinStore -> setRefCountCol (dataRefCountCol)) != 0)
		return rc;
	
	if ((rc = pwinStore -> setEvalContext (evalContext)) != 0)
		return rc;
	
	if ((rc = pwinStore -> setCopyEval (copyEval)) != 0)
		return rc;
	
	if ((rc = pwinStore -> setNumStubs (store -> numStubs)) != 0)
		return rc;

	if ((rc = pwinStore -> initialize ()) != 0)
		return rc;
	
	// Index for the header tuples
	if (numIndexes >= MAX_INDEXES)
		return -1;	
	idx = new HashIndex (numIndexes, LOG);
	indexes [numIndexes ++] = idx;
	
	// Hash evaluator used by idx during updates
	updateHashEval = new HEval();	
	
	hinstr.r = UPDATE_ROLE;
	for (unsigned int a = 0 ; a < numPartnAttrs ; a++) {
		
		attr = window -> u.PARTN_WIN.partnAttrs [a];
		attrType = child -> attrTypes [attr.pos];
		
		hinstr.c = hdrCols [a];
		hinstr.type = attrType;
		
		if ((rc = updateHashEval -> addInstr (hinstr)) != 0)
			return rc;
	}
	
	if ((rc = updateHashEval -> setEvalContext (evalContext)) != 0)
		return rc;	
	
	// Hash evaluator used by idx during scans
	scanHashEval = new HEval ();
	
	hinstr.r = DATA_ROLE;
	for (unsigned int a = 0 ; a < numPartnAttrs ; a++) {
		
		attr = window -> u.PARTN_WIN.partnAttrs [a];
		attrType = child -> attrTypes [attr.pos];
		
		hinstr.c = dataCols [attr.pos];
		hinstr.type = attrType;
		
		if ((rc = scanHashEval -> addInstr (hinstr)) != 0)
			return rc;
	}

	if ((rc = scanHashEval -> setEvalContext (evalContext)) != 0)
		return rc;

	
	scanKeyEqual = new BEval ();	
	
	for (unsigned int a = 0 ; a < numPartnAttrs ; a++) {
		
		attr = window -> u.PARTN_WIN.partnAttrs [a];
		attrType = child -> attrTypes [attr.pos];

		binstr.r1 = DATA_ROLE;
		binstr.c1 = dataCols [attr.pos];
		binstr.e1 = 0;

		binstr.r2 = SCAN_ROLE;
		binstr.c2 = hdrCols [a];
		binstr.e2 = 0;
		
		switch (attrType) {
		case INT:   binstr.op = Execution::INT_EQ; break;
		case FLOAT: binstr.op = Execution::FLT_EQ; break;
		case CHAR:  binstr.op = Execution::CHR_EQ; break;
		case BYTE:  binstr.op = Execution::BYT_EQ; break;
			
#ifdef _DM_
		default:
			ASSERT(0);
			break;
#endif
		}
		
		if ((rc = scanKeyEqual -> addInstr (binstr)) != 0)
			return rc;
	}
	
	if ((rc = scanKeyEqual -> setEvalContext (evalContext)) != 0)
		return rc;	

	if ((rc = idx -> setMemoryManager (memMgr)) != 0)
		return rc;
	
	if ((rc = idx -> setEvalContext (evalContext)) != 0)
		return rc;
	
	if ((rc = idx -> setUpdateHashEval (updateHashEval)) != 0)
		return rc;
	
	if ((rc = idx -> setScanHashEval (scanHashEval)) != 0)
		return rc;
	
	if ((rc = idx -> setKeyEqual (scanKeyEqual)) != 0)
		return rc;
	
	if ((rc = idx -> setThreshold (INDEX_THRESHOLD)) != 0)
		return rc;
	
	if ((rc = idx -> initialize ()) != 0)
		return rc;
	
	if ((rc = pwinStore -> setHdrIndex (idx)) != 0)
		return rc;
	
	store -> instStore = pwinStore;
	
	return 0;
}
