/**
 * @file      plan_store.cc
 * @date      Aug. 25, 2004
 * @brief     Implementation of routines that manage store space for
 *            synopses.  This includes routines for sharing state.
 */

#ifndef _PLAN_MGR_IML_
#include "metadata/plan_mgr_impl.h"
#endif

#ifndef _DEBUG_
#include "common/debug.h"
#endif

using namespace Physical;
using namespace Metadata;
using namespace std;

static unsigned int getInputIndex (Operator *parent, Operator *child);

int PlanManagerImpl::getSharedSynType (Operator *op,
									   unsigned int inputIdx, 
									   bool &bSynReq,
									   SynopsisKind &synKind)
{
	ASSERT (op);
	ASSERT (op -> numInputs > inputIdx);

	// pessimism ..
	bSynReq = false;
	
	switch (op -> kind) {
		// These operators do not need a synopsis
	case PO_SELECT:		
	case PO_PROJECT:
	case PO_STREAM_SOURCE: 		
	case PO_RELN_SOURCE: 				
	case PO_QUERY_SOURCE: 		
	case PO_OUTPUT:
	case PO_SINK:
	case PO_SS_GEN:
		// These operators require synopses but they are not "shared" -
 		// the operator below does not need to provide space for the
 		// synopsis
		
	case PO_PARTN_WIN: 
	case PO_ISTREAM: 
	case PO_DSTREAM:
	case PO_DISTINCT:
	case PO_UNION:
	case PO_EXCEPT:
		break;
		
	case PO_JOIN:
	case PO_JOIN_PROJECT:
		
		ASSERT (op -> numInputs == 2);
		bSynReq = true;
		synKind = REL_SYN;
		break;
		
	case PO_STR_JOIN:
	case PO_STR_JOIN_PROJECT:
		
		ASSERT (op -> numInputs == 2);
		
		if (inputIdx == 1) {
			bSynReq = true;
			synKind = REL_SYN;
		}
		
		break;
		
	case PO_GROUP_AGGR:
		
		if (op -> u.GROUP_AGGR.inSyn) {
			bSynReq = true;
			synKind = REL_SYN;
		}
		
		break;		
		
	case PO_ROW_WIN: 		
	case PO_RANGE_WIN: 		
		bSynReq = true;
		synKind = WIN_SYN;
		break;
				
	case PO_RSTREAM: 
		bSynReq = true;
		synKind = REL_SYN;
		break;		
		
	default:
		break;		
	};
	
	return 0;
}

static int mk_stub (Synopsis *synopsis, Store *store)
{
	ASSERT (synopsis);
	ASSERT (store);

	if (store -> numStubs >= MAX_STUBS)
		return -1;
	
	synopsis -> store = store;

	store -> stubs [store -> numStubs ++] = synopsis;
	return 0;
}

int PlanManagerImpl::setSharedStore (Operator *op, unsigned int inputIdx,
									 Store *store)
{
	int rc;
	ASSERT (op);
	ASSERT (op -> numInputs > inputIdx);
	
	switch (op -> kind) {
		// These operators do not need a synopsis - do nothing
	case PO_SELECT:		
	case PO_PROJECT:
	case PO_STREAM_SOURCE: 		
	case PO_RELN_SOURCE: 				
	case PO_QUERY_SOURCE: 		
	case PO_OUTPUT:
	case PO_SINK:
	case PO_SS_GEN:
		// These operators require synopses but they are not "shared" -
 		// the operator below does not need to provide space for the
 		// synopsis
		
	case PO_PARTN_WIN: 
	case PO_ISTREAM: 
	case PO_DSTREAM:
	case PO_DISTINCT:
	case PO_UNION:
	case PO_EXCEPT:
		break;
		
	case PO_JOIN:
		ASSERT (op -> numInputs == 2);
		ASSERT (op -> u.JOIN.outerSyn);
		ASSERT (op -> u.JOIN.innerSyn);
		
		// outer: the store for the outer is @param store
		if (inputIdx == 0) {			
			if ((rc = mk_stub (op -> u.JOIN.outerSyn, store)) != 0)
				return rc;
		}
		
		// inputIdx == 1
		else {
			if ((rc = mk_stub (op -> u.JOIN.innerSyn, store)) != 0)
				return rc;
		}
		
		break;
		
	case PO_JOIN_PROJECT:
		ASSERT (op -> numInputs == 2);
		ASSERT (op -> u.JOIN_PROJECT.outerSyn);
		ASSERT (op -> u.JOIN_PROJECT.innerSyn);

		// outer: the store for the outer is @param store
		if (inputIdx == 0) {			
			if ((rc = mk_stub (op -> u.JOIN_PROJECT.outerSyn,
							   store)) != 0) {
				return rc;
			}
		}
		
		// inputIdx == 1
		else {
			if ((rc = mk_stub (op -> u.JOIN_PROJECT.innerSyn,
							   store)) != 0) {				
				return rc;
			}
		}
		
		break;
		
	case PO_STR_JOIN:

		ASSERT (op -> u.STR_JOIN.innerSyn);
		if (inputIdx == 1) {
			if ((rc = mk_stub (op -> u.STR_JOIN.innerSyn, store)) != 0)
				return rc;
		}
		break;		
		
	case PO_STR_JOIN_PROJECT:
		
		ASSERT (op -> u.STR_JOIN_PROJECT.innerSyn);
		if (inputIdx == 1) {
			if ((rc = mk_stub (op -> u.STR_JOIN_PROJECT.innerSyn,
							   store)) != 0) {				
				return rc;
			}
		}
		break;				
		
	case PO_GROUP_AGGR:
		
		if (op -> u.GROUP_AGGR.inSyn) {
			if ((rc = mk_stub (op -> u.GROUP_AGGR.inSyn, store)) != 0)
				return rc;
		}
		
		break;		
				
	case PO_ROW_WIN:
		ASSERT (op -> u.ROW_WIN.winSyn);
		if ((rc = mk_stub (op -> u.ROW_WIN.winSyn, store)) != 0)
			return rc;

		for (unsigned int o = 0 ; o < op -> numOutputs ; o++) {
			inputIdx = getInputIndex (op -> outputs [o], op);
			
			if ((rc = setSharedStore (op -> outputs [o],
									  inputIdx, store)) != 0)
				return rc;
		}
		
		break;
		
	case PO_RANGE_WIN:
		ASSERT (op -> u.RANGE_WIN.winSyn);
		if ((rc = mk_stub (op -> u.RANGE_WIN.winSyn, store)) != 0)
			return rc;
		
		for (unsigned int o = 0 ; o < op -> numOutputs ; o++) {
			inputIdx = getInputIndex (op -> outputs [o], op);
			
			if ((rc = setSharedStore (op -> outputs [o],
									  inputIdx, store)) != 0)
				return rc;
		}		
		
		break;				
				
	case PO_RSTREAM: 
		ASSERT (op -> u.RSTREAM.inSyn);
		if ((rc = mk_stub (op -> u.RSTREAM.inSyn, store)) != 0)
			return rc;
		break;
		
	default:
		break;		
	};
	
	return 0;
}

static unsigned int getInputIndex (Operator *parent, Operator *child)
{
	ASSERT (child);
	ASSERT (parent);
	
	for (unsigned int i = 0 ; i < parent -> numInputs ; i++)
		if (parent -> inputs [i] == child)
			return i;
	
	// should never come here
	ASSERT (0);
	return 0;
}

Store *PlanManagerImpl::new_store (StoreKind kind)
{
	Store *store;
	
	if (numStores >= MAX_STORES)
		return 0;

	store = stores + numStores;
	
	store -> id = numStores;
	store -> kind = kind;
	store -> numStubs = 0;
	store -> instStore = 0;
	
	numStores ++;
	return store;
}										   

int PlanManagerImpl::add_store_select (Operator *op)
{
	int rc;
	Operator *outOp;
	
	bool bssReq [MAX_OUT_BRANCHING];
	SynopsisKind ssKind [MAX_OUT_BRANCHING];
	unsigned int inputIdx [MAX_OUT_BRANCHING];
	
	bool bStoreReq;
	
	ASSERT (op);
	ASSERT (op -> kind == PO_SELECT);
	
	// Determine the sharing requirements of the operators above me
	bStoreReq = false;
	for (unsigned int o = 0 ; o < op -> numOutputs ; o++) {
		ASSERT (op -> outputs [o]);
		
		outOp = op -> outputs [o];
		inputIdx [o] = getInputIndex (outOp, op);
		
		if((rc = getSharedSynType (outOp, inputIdx [o], bssReq [o],	
								   ssKind [o])) != 0) {
			return rc;
		}
		
		if (bssReq [o])
			bStoreReq = true;
	}

	// If no operator above me has a synopsis for which I have to allocate
	// space, then I don't need to do anything ...
	if (!bStoreReq)
		return 0;
	
	// If some operator above me has a synopsis for which I have to
	// allocate space then I need to create a new dummy project operator
	// above me to do this job.  Select operators do not allocate space.

	// dummy project operator: output == input.  But the operator
	// allocates space (newly) for its output tuples.
	Operator *project;
	
	// create a the dummy project operator
	if ((rc = mk_dummy_project (op, project)) != 0)
		return rc;
	
	// Note: op -> numAttrs would have increased by 1 within
	// mk_dummy_projectj.  To be consistent we update bssReq and inputIdx
	ASSERT (op -> outputs [op -> numOutputs - 1] == project);
	ASSERT (project -> inputs [0] == op);	
	bssReq [op -> numOutputs - 1] = false;
	inputIdx [op -> numOutputs - 1] = 0;
	
	// All the operators above me who required space for synopsis will
	// now read from the project
	for (unsigned int o = 0 ; o < op -> numOutputs ; o++) {
		
		// outputs [o] reads from me (as before)
		if (!bssReq [o])
			continue;
		
		// outputs [o] reads from project
		ASSERT (op -> outputs [o] -> numInputs > inputIdx [o]);
		ASSERT (op -> outputs [o] -> inputs [inputIdx [o]] == op);
		
		op -> outputs [o] -> inputs [inputIdx [o]] = project;
		
		rc = addOutput (project, op -> outputs [o]);
		if (rc != 0)
			return rc;
	}
	
	// There should be at least one operator which reads from project now,
	// otherwise we would not have been here.
	ASSERT (project -> numOutputs > 0);
	
	// Add the necessary synopses to the project
	if ((rc = add_syn_proj (project)) != 0)
		return rc;
	
	// Add the necessary store to the project
	if ((rc = add_store (project)) != 0)
		return rc;
	
	// My state could be inconsistent now wrt outputs []
	unsigned int numOldOutputs;
	
	numOldOutputs = op -> numOutputs;
	op -> numOutputs = 0;
	for (unsigned int o = 0 ; o < numOldOutputs ; o++) {
		if (bssReq[o])
			continue;
		
		op -> outputs [op -> numOutputs++] = op -> outputs [o];
	}	
	
	return 0;
}

int PlanManagerImpl::add_store (Operator *op)
{
	int rc;
	Operator *outOp;
	unsigned int inputIdx;
	StoreKind storeKind;
	Store *store;
	
	// bReqStore [o] is true iff op->outputs[o] requires me to allocate
	// space for one of its synopses.
	bool bReqStore [MAX_OUT_BRANCHING];
	
	// If bReqStores[o] is true, then ssKind[o] specifies the type of the
	// synopsis for which I need to allocate space
	SynopsisKind ssKind [MAX_OUT_BRANCHING];
	
	ASSERT (op);
	
	// Selects are handled in a different way: they cannot allocate space,
	// so we need to add a project operator above select to do this.
	if (op -> kind == PO_SELECT)
		return add_store_select(op);

	// The window operators ROW_WIN, RANGE_WIN, never have a
	// store. Nor does the output operator
	if ((op -> kind == PO_ROW_WIN) ||
		(op -> kind == PO_RANGE_WIN) ||
		(op -> kind == PO_OUTPUT ) ||
		(op -> kind == PO_SINK)) {
		return 0;
	}	

	// For sys stream gen operator we generate a store only
	// when we come here through a monitor query [[ Explanation ]]
	if (op -> kind == PO_SS_GEN && op -> numOutputs == 0)
		return 0;
	
	// Determine if any of the operators above me have synopses which
	// require that I assign memory for their tuples.	
	for (unsigned int o = 0 ; o < op -> numOutputs ; o++) {
		ASSERT (op -> outputs [o]);
		
		outOp = op -> outputs [o];
		inputIdx = getInputIndex (outOp, op);
		
		if((rc = getSharedSynType (outOp, inputIdx, bReqStore [o],	
								   ssKind [o])) != 0) {
			return rc;
		}
	}

	store = 0;
	switch (op -> kind) {
		
	case PO_PROJECT:
		if (op -> bStream) {
			storeKind = SIMPLE_STORE;
			
			for (unsigned int o = 0 ; o < op -> numOutputs ; o++) {
				if (bReqStore [o]) {
					storeKind = WIN_STORE;
					break;
				}
			}
			
			store = new_store (storeKind);
		}
		
		else {
			store = new_store (LIN_STORE);
			store -> u.LIN_STORE.idx = 0;
			store -> u.LIN_STORE.numLineage = op -> numInputs;
			
			if ((rc = mk_stub (op -> u.PROJECT.outSyn, store)) != 0)
				return rc;			
		}
		
		break;
		
	case PO_JOIN:
		if (op -> bStream) {
			storeKind = SIMPLE_STORE;
			
			for (unsigned int o = 0 ; o < op -> numOutputs ; o++) {
				if (bReqStore [o]) {
					storeKind = WIN_STORE;
					break;
				}
			}
			
			store = new_store (storeKind);
		}
		
		else {
			store = new_store (LIN_STORE);
			store -> u.LIN_STORE.idx = 0;
			store -> u.LIN_STORE.numLineage = op -> numInputs;

			if ((rc = mk_stub (op -> u.JOIN.joinSyn, store)) != 0)
				return rc;
		}
		
		break;
		
	case PO_JOIN_PROJECT:
		if (op -> bStream) {
			storeKind = SIMPLE_STORE;
			
			for (unsigned int o = 0 ; o < op -> numOutputs ; o++) {
				if (bReqStore [o]) {
					storeKind = WIN_STORE;
					break;
				}
			}
			
			store = new_store (storeKind);
		}
		
		else {
			store = new_store (LIN_STORE);
			store -> u.LIN_STORE.idx = 0;
			store -> u.LIN_STORE.numLineage = op -> numInputs;
			
			if ((rc = mk_stub (op -> u.JOIN_PROJECT.joinSyn, store)) != 0)
				return rc;
		}
		
		break;

		
	case PO_STR_JOIN:
	case PO_STR_JOIN_PROJECT:
		if (op -> bStream) {
			storeKind = SIMPLE_STORE;
			
			for (unsigned int o = 0 ; o < op -> numOutputs ; o++) {
				if (bReqStore [o]) {
					storeKind = WIN_STORE;
					break;
				}
			}
			
			store = new_store (storeKind);
		}
		
		else {
			store = new_store (LIN_STORE);
			store -> u.LIN_STORE.idx = 0;
			store -> u.LIN_STORE.numLineage = op -> numInputs;
		}
		
		break;
		
	case PO_ISTREAM:
		storeKind = SIMPLE_STORE;
		
		for (unsigned int o = 0 ; o < op -> numOutputs ; o++) {
			if (bReqStore [o]) {
				storeKind = WIN_STORE;
				break;
			}
		}

		store = new_store (storeKind);
		
		// Stores for the internal state of istream
		ASSERT (op -> u.ISTREAM.nowSyn);
		
		op -> u.ISTREAM.nowStore = new_store (REL_STORE);
		op -> u.ISTREAM.nowStore -> ownOp = op;
		if ((rc = mk_stub (op -> u.ISTREAM.nowSyn,
						   op -> u.ISTREAM.nowStore)) != 0)
			return rc;
		
		break;
		
	case PO_DSTREAM:
		storeKind = SIMPLE_STORE;
		
		for (unsigned int o = 0 ; o < op -> numOutputs ; o++) {
			if (bReqStore [o]) {
				storeKind = WIN_STORE;
				break;
			}
		}
		
		store = new_store (storeKind);

		// Store for the internal state of dstream
		ASSERT (op -> u.DSTREAM.nowSyn);
		
		op -> u.DSTREAM.nowStore = new_store (REL_STORE);
		op -> u.DSTREAM.nowStore -> ownOp = op;
		if ((rc = mk_stub (op -> u.DSTREAM.nowSyn,
						   op -> u.DSTREAM.nowStore)) != 0)
			return rc;
		
		break;
		
	case PO_RSTREAM:
	case PO_STREAM_SOURCE:
		
		storeKind = SIMPLE_STORE;
		
		for (unsigned int o = 0 ; o < op -> numOutputs ; o++) {
			if (bReqStore [o]) {
				storeKind = WIN_STORE;
				break;
			}
		}
		
		store = new_store (storeKind);
		
		break;
		
	case PO_SELECT:
	case PO_ROW_WIN:
	case PO_RANGE_WIN:
		
		// We should not come here
		ASSERT (0);
		break;
		
	case PO_GROUP_AGGR:

		storeKind = REL_STORE;
		store = new_store (storeKind);

		if ((rc = mk_stub (op -> u.GROUP_AGGR.outSyn,
						   store)) != 0)
			return rc;
		break;
		
	case PO_DISTINCT:
		
		if (op -> bStream) {
			store = new_store (WIN_STORE);
		}
		
		else {
			store = new_store (REL_STORE);			
		}

		if ((rc = mk_stub (op -> u.DISTINCT.outSyn, store)) != 0)
			return rc;		
		break;
		
	case PO_RELN_SOURCE:

		storeKind = REL_STORE;
		store = new_store (storeKind);
		
		ASSERT (op -> u.RELN_SOURCE.outSyn);
		if ((rc = mk_stub (op -> u.RELN_SOURCE.outSyn, store)) != 0)
			return rc;
		
		break;

		
	case PO_PARTN_WIN:
		
		storeKind = PARTN_WIN_STORE;
		store = new_store (storeKind);
		store -> u.PWIN_STORE.hdrIdx = 0;
		
		ASSERT (op -> u.PARTN_WIN.winSyn);
		if ((rc = mk_stub (op -> u.PARTN_WIN.winSyn, store)) != 0)
			return rc;
		
		break;

	case PO_UNION:
		
		if (op -> bStream) {
			ASSERT (!op -> u.UNION.outSyn);
			
			storeKind = SIMPLE_STORE;
			
			for (unsigned int o = 0 ; o < op -> numOutputs ; o++) {
				if (bReqStore [o]) {
					storeKind = WIN_STORE;
					break;
				}
			}
			
			store = new_store (storeKind);			
		}
		
		else {
			store = new_store (LIN_STORE);
			
			ASSERT (op -> u.UNION.outSyn);
			if ((rc = mk_stub (op -> u.UNION.outSyn, store)) != 0)
				return rc;			
		}		
		
		break;
	case PO_EXCEPT:

		store = new_store (REL_STORE);
		
		ASSERT (op -> u.EXCEPT.outSyn);
		if ((rc = mk_stub (op -> u.EXCEPT.outSyn, store)) != 0)
			return rc;
		
		op -> u.EXCEPT.countStore = new_store (REL_STORE);
		op -> u.EXCEPT.countStore -> ownOp = op;
		
		if ((rc = mk_stub (op -> u.EXCEPT.countSyn,
						   op -> u.EXCEPT.countStore)) != 0)
			return rc;
		
		break;

	case PO_SS_GEN:
		ASSERT (op -> numOutputs > 0);
		
		storeKind = SIMPLE_STORE;
		
		for (unsigned int o = 0 ; o < op -> numOutputs ; o++) {
			if (bReqStore [o]) {
				storeKind = WIN_STORE;
				break;
			}
		}
		
		store = new_store (storeKind);
		
		// We are using 'store' in a subtle way [[ Explain ]]
		ASSERT (!op -> store);
		
		break;
	default:
		ASSERT (0);
		break;
	};
	
	// add the store to the operator
	op -> store = store;
	store -> ownOp = op;
	
	// All the synopses upstream whose tuples are assigned by this store
	// should be updated
	// Set the store for all the shared synopses upstream	
	for (unsigned int o = 0 ; o < op -> numOutputs ; o++) {
		outOp = op -> outputs [o];
		
		if (bReqStore [o]) {
			inputIdx = getInputIndex (outOp, op);
			
			if ((rc = setSharedStore (outOp, inputIdx, store)) != 0)
				return rc;
		}
	}
	
	return 0;
}	

int PlanManagerImpl::add_store ()
{
	int rc;
	Operator *op;

	op = usedOps;
	while(op) {
		if ((rc = add_store (op)) != 0)
			return rc;
				
		op = op -> next;
	}

	return 0;
}

int PlanManagerImpl::add_store_mon (Operator *&plan,
									Operator **opList,
									unsigned int &numOps)
{
	int rc;
	Operator *op;
	
	for (unsigned int o = 0 ; o < numOps ; o++) {
		op = opList [o];
		
		if ((rc = add_store (op)) != 0)
			return rc;
	}
	
	return 0;
}

static int set_in_stores_s (Operator *op)
{
	int rc;
	Operator *child;

	ASSERT (op);
	for (unsigned int i = 0 ; i < op -> numInputs ; i++) {
		
		// Stores already set (called recursively by one of op's parents
		if (op -> inStores [i]) {
			ASSERT (i == 0);
			break;
		}
		
		child = op -> inputs [i];
		ASSERT (child);
		
		if (child -> store) {
			op -> inStores [i] = child ->store;			
		}
		
		else {
			ASSERT (child -> kind == PO_SELECT    ||
					child -> kind == PO_RANGE_WIN ||
					child -> kind == PO_ROW_WIN);
			
			ASSERT (child -> numInputs == 1);
			
			if (!child -> inStores [0]) {
				rc = set_in_stores_s (child);
				if (rc != 0) return rc;				
			}
			ASSERT (child -> inStores [0]);
			
			op -> inStores [i] = child -> inStores [0];
		}
		
		// Ugly code: for correct tuple ref informtion we need to
		// set the store that is generating the tuples for a
		// sharedQueueWriter as well.
		if (op -> inQueues [i] -> kind == READER_Q) {
			ASSERT (op -> inputs [i] -> numOutputs > 0);
			ASSERT (op -> inputs [i] -> outQueue);
			ASSERT (op -> inputs [i] -> outQueue -> kind == WRITER_Q);
			
			Queue *writer = op -> inputs [i] -> outQueue;
			writer -> u.WRITER.store = op -> inStores [i];
		}
	}
	
	return 0;
}

int PlanManagerImpl::set_in_stores ()
{
	int rc;
	
	Operator *op;
	
	// Initialize all the inStores to 0
	op = usedOps;
	while (op) {
		
		for (unsigned int i = 0 ; i < op -> numInputs ; i++)
			op -> inStores [i] = 0;
		
		op = op -> next;
	}
	
	// set the input stores for each op
	op = usedOps;
	while (op) {
		
		if ((rc = set_in_stores_s (op)) != 0)
			return rc;
		
		op = op -> next;
	}
	
	return 0;
}

int PlanManagerImpl::set_in_stores_mon (Physical::Operator *&plan,
										Physical::Operator **opList,
										unsigned int &numOps)
{
	int rc;
	Operator *op;
	
	// Initialize all the inStores to 0
	for (unsigned int o = 0 ; o < numOps ; o++) {
		op = opList [o];
		
		for (unsigned int i = 0 ; i < op -> numInputs ; i++)
			op -> inStores [i] = 0;
	}		

	for (unsigned int o = 0 ; o < numOps ; o++) {
		op = opList [o];
		
		if ((rc = set_in_stores_s (op)) != 0)
			return rc;
	}
	
	return 0;
}
