#ifdef _SYS_STR_

#ifndef _PLAN_MGR_IMPL_
#include "metadata/plan_mgr_impl.h"
#endif

#ifndef _SYS_STREAM_GEN_
#include "execution/operators/sys_stream_gen.h"
#endif

using Execution::SysStreamGen;
using namespace Metadata;

int PlanManagerImpl::inst_ss_gen (Physical::Operator *op)
{
	int rc;
	SysStreamGen *ssgen;
	
	ASSERT (op);
	ASSERT (op -> kind == PO_SS_GEN);
	
	ssgen = new SysStreamGen (op -> id, LOG);
	op -> instOp = ssgen;
	
	// Add all operators for ssgen to monitor
	Physical::Operator *top;
	top = usedOps;
	for (top = usedOps ; top ; top = top -> next) {

		ASSERT (top -> instOp);
		if ((rc = ssgen -> addOpEntity (top -> id, top -> instOp)) != 0)
			return rc;
		
		if (top -> kind == PO_JOIN ||
			top -> kind == PO_STR_JOIN ||
			top -> kind == PO_JOIN_PROJECT ||
			top -> kind == PO_STR_JOIN_PROJECT) {

			rc = ssgen -> addJoinEntity (top -> id, top -> instOp);
			if (rc != 0) return rc;
		}
	}
	
	// Add all queues for ssgen to monitor
	for (unsigned int q = 0 ; q < numQueues ; q++) {
		ASSERT (queues [q].instQueue);
		
		if ((rc = ssgen -> addQueueEntity (q, queues [q].instQueue)) != 0)
			return rc;
	}
	
	// Add all synopses
	for (unsigned int s = 0 ; s < numSyns ; s++) {
		switch (syns [s].kind) {
		case REL_SYN:
			ASSERT (syns[s].u.relSyn);
			if ((rc = ssgen -> addSynEntity (s, syns[s].u.relSyn)) != 0)
				return rc;
			break;
			
		case WIN_SYN:
			ASSERT (syns[s].u.winSyn);
			if ((rc = ssgen -> addSynEntity (s, syns[s].u.winSyn)) != 0)
				return rc;
			break;
			
		case PARTN_WIN_SYN:
			ASSERT (syns[s].u.pwinSyn);
			if ((rc = ssgen -> addSynEntity (s, syns[s].u.pwinSyn)) != 0)
				return rc;
			break;
			
		case LIN_SYN:
			ASSERT (syns[s].u.linSyn);
			if ((rc = ssgen -> addSynEntity (s, syns[s].u.linSyn)) != 0)
				return rc;
			break;
			
		default:
			ASSERT (0);
			return -1;			
		}
	}
	
	// Add stores
	for (unsigned int s = 0 ; s < numStores ; s++) {
		ASSERT (stores[s].instStore);
		
		if ((rc = ssgen -> addStoreEntity (s, stores[s].instStore)) != 0)
			return rc;
	}
	
	return 0;
}

int PlanManagerImpl::inst_ss_gen_store (Physical::Operator *p_op)
{
	int rc;
	TupleLayout *tupleLayout;
	
	ASSERT (p_op);
	ASSERT (p_op -> kind == PO_SS_GEN);
	ASSERT (p_op -> numOutputs > 0);
	ASSERT (p_op -> store);
	ASSERT (p_op -> store -> kind == SIMPLE_STORE ||
			p_op -> store -> kind == WIN_STORE);
	
	tupleLayout = new TupleLayout (p_op);
	if (p_op -> store -> kind == SIMPLE_STORE) {
		if ((rc = inst_simple_store (p_op -> store, tupleLayout)) != 0) {
			return rc;
		}
	}
	else {
		if ((rc = inst_win_store (p_op -> store, tupleLayout)) != 0) {
			return rc;
		}
	}
	ASSERT (p_op -> store -> instStore);
	delete tupleLayout;
	
	return 0;	
}

int PlanManagerImpl::update_ss_gen (Physical::Operator *p_op,
									Physical::Operator **opList,
									unsigned int numOps,
									Physical::Queue **queueList,
									unsigned int l_numQueues,
									Physical::Store **storeList,
									unsigned int l_numStores,
									Physical::Synopsis **synList,
									unsigned int l_numSyns)
{
	int rc;
	
	ASSERT (p_op);
	ASSERT (p_op -> kind == PO_SS_GEN);
	ASSERT (p_op -> store);
	ASSERT (p_op -> store -> instStore);
	ASSERT (p_op -> outQueue);
	ASSERT (p_op -> outQueue -> instQueue);
	ASSERT (p_op -> instOp);
	
	SysStreamGen *e_op = (SysStreamGen*)p_op -> instOp;
	
	if ((rc = e_op -> addOutput (p_op -> outQueue -> instQueue,
								 p_op -> store -> instStore)) != 0)
		return rc;
	
	p_op -> u.SS_GEN.outStores [p_op -> u.SS_GEN.numOutput] =
		p_op -> store;
	p_op -> u.SS_GEN.outQueues [p_op -> u.SS_GEN.numOutput] =
		p_op -> outQueue;
	p_op -> u.SS_GEN.numOutput ++;
	
	p_op -> store = 0;
	p_op -> outQueue = 0;
	p_op -> numOutputs = 0;
	
	// add the new entities to ss_gen
	Physical::Operator *top;
	for (unsigned int o = 0 ; o < numOps ; o++) {
		top = opList [o];

		if (top == p_op)
			continue;
		
		ASSERT (top -> instOp);
		if ((rc = e_op -> addOpEntity (top -> id, top -> instOp)) != 0)
			return rc;

		if (top -> kind == PO_JOIN ||
			top -> kind == PO_STR_JOIN ||
			top -> kind == PO_JOIN_PROJECT ||
			top -> kind == PO_STR_JOIN_PROJECT) {

			rc = e_op -> addJoinEntity (top -> id, top -> instOp);
			if (rc != 0) return rc;
		}
	}

	// Add all queues for ssgen to monitor
	for (unsigned int q = 0 ; q < l_numQueues ; q++) {
		ASSERT (queueList[q] -> instQueue);
		
		if ((rc = e_op -> addQueueEntity (queueList[q] -> id,
										  queueList[q] -> instQueue)) != 0)
			return rc;
	}
	
	// Add all synopses
	for (unsigned int s = 0 ; s < l_numSyns ; s++) {
		switch (synList[s] -> kind) {
			
		case REL_SYN:
			ASSERT (synList[s] -> u.relSyn);
			if ((rc = e_op -> addSynEntity (synList[s] -> id,
											synList[s] -> u.relSyn)) != 0)
				return rc;
			break;
			
		case WIN_SYN:
			ASSERT (synList[s] -> u.winSyn);
			if ((rc = e_op -> addSynEntity (synList[s] -> id,
											synList[s] -> u.winSyn)) != 0)
				return rc;
			break;
			
		case PARTN_WIN_SYN:
			ASSERT (synList[s] -> u.pwinSyn);
			if ((rc = e_op -> addSynEntity (synList[s] -> id,
											synList[s] -> u.pwinSyn)) != 0)
				return rc;
			break;
			
		case LIN_SYN:
			ASSERT (synList[s] -> u.linSyn);
			if ((rc = e_op -> addSynEntity (synList[s] -> id,
											synList[s] -> u.linSyn)) != 0)
				return rc;
			break;
			
		default:
			ASSERT (0);
			return -1;			
		}
	}
	
	// Add stores
	for (unsigned int s = 0 ; s < l_numStores ; s++) {
		ASSERT (storeList[s] -> instStore);
		
		if ((rc = e_op -> addStoreEntity (storeList[s] -> id,
										  storeList[s] -> instStore)) != 0)
			return rc;
	}
	
	return 0;
}

#endif
