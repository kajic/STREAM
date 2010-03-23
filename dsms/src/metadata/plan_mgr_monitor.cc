/**
 * @file        plan_mgr_monitor.cc
 * @date        Jan 12, 2005
 * @brief       Monitor related routines of plan manager
 */

#ifdef _SYS_STR_

#ifndef _PLAN_MGR_IMPL_
#include "metadata/plan_mgr_impl.h"
#endif

#ifndef _DEBUG_
#include "common/debug.h"
#endif

#ifndef _SYS_STREAM_
#include "common/sys_stream.h"
#endif

static int getStoreList (Operator **opList, unsigned int numOps,
						 Store **storeList, unsigned int& numStores);
static int getSynList (Operator **opList, unsigned int numOps,
					   Synopsis **synList, unsigned int& numStores);
static int getQueueList (Operator **opList, unsigned int numOps,
						 Queue **queueList, unsigned int& numStores);

using namespace Metadata;
using namespace Physical;
using namespace std;

static const unsigned int MAX_OPS_MON = 20;
static const unsigned int MAX_STORES_MON = 20;
static const unsigned int MAX_SYNS_MON = 20;
static const unsigned int MAX_QUEUES_MON = 20;

static int getOpList (Operator *plan, Operator **opList, unsigned int &numOps);

static bool isNewOp (Operator *op, Operator **opList, unsigned int numOps);

#ifdef _DM_
static bool checkMonitorPlan (Operator *plan);
static void checkSSGen (Operator *ssgen);
#endif

/**
 * Add a plan for monitoring a system property.
 */

int PlanManagerImpl::addMonitorPlan (unsigned int monitorId,
									 Logical::Operator *logPlan,
									 Interface::QueryOutput *output,
									 Execution::Scheduler *sched)
{
	int rc;
	Operator *phyPlan;
	Operator *opList [MAX_OPS_MON];
	Store *storeList [MAX_STORES_MON];
	Synopsis *synList [MAX_SYNS_MON];
	Queue *queueList [MAX_QUEUES_MON];
	Operator *ssgen;
	unsigned int numOps;
	unsigned int l_numStores;
	unsigned int l_numSyns, l_numQueues;
	
	ASSERT (output);

	/// debug
	LOG << "planmgr: begin " << endl;
	
#ifdef _DM_
	// check consistency of sys stream gen operator
	ASSERT (sourceOps [0].tableId == SS_ID);
#endif

	// sys-stream generator operator
	ssgen = ops + sourceOps [0].opId;

#ifdef _DM_
	checkSSGen (ssgen);
#endif
	
	// Generate the naive physical plan
	if ((rc = genPhysicalPlan_mon (monitorId, logPlan, output, phyPlan)) != 0) 
		return rc;
	
#ifdef _DM_
	ASSERT (checkMonitorPlan (phyPlan));
#endif
	
	// Get the list of operators in the plan
	numOps = 0;
	if ((rc = getOpList (phyPlan, opList, numOps)) != 0)
		return rc;
	
	// Optimize the plan
	if ((rc = optimize_plan_mon (phyPlan, opList, numOps)) != 0)
		return rc;
	
#ifdef _DM_
	ASSERT (checkMonitorPlan (phyPlan));
#endif
	
	// Add auxiliary structures (stores, synopses)
	if ((rc = add_aux_structures_mon (phyPlan, opList, numOps)) != 0)
		return rc;
	
	// Get a list of newly created stores
	if ((rc = getStoreList (opList, numOps, storeList, l_numStores)) != 0)
		return rc;

	if ((rc = getSynList (opList, numOps, synList, l_numSyns)) != 0)
		return rc;

	if ((rc = getQueueList (opList, numOps, queueList, l_numQueues)) != 0)
		return rc;
	
#ifdef _DM_
	/// debug
	printPlan ();	
#endif
	
	// instantiate the physical structures
	if ((rc = instantiate_mon (phyPlan, opList, numOps)) != 0)
		return rc;
	
	// update ssgen operator to produce output to the new plan
	if ((rc = update_ss_gen (ssgen,
							 opList, numOps,
							 queueList, l_numQueues,
							 storeList, l_numStores,
							 synList, l_numSyns)) != 0)
		return rc;
	
	// update the scheduler with the new operators
	for (unsigned int o = 0 ; o < numOps ; o++) {
		ASSERT (opList [o] -> instOp);
		
		if ((rc = sched -> addOperator (opList[o] -> instOp)) != 0)
			return rc;
	}
	
	LOG << "planmgr: done" << endl;
	return 0;
}

int PlanManagerImpl::genPhysicalPlan_mon (unsigned int monitorId,
										  Logical::Operator *logPlan,
										  Interface::QueryOutput *output,
										  Physical::Operator *&phyPlan)
{
	int rc;
	Physical::Operator *outputOp;
	
	// Generate the physical plan (minus the output operator)
	if ((rc = genPhysicalPlan (logPlan, phyPlan)) != 0)
		return rc;

	// Store the mapping from monitorId to the output operator
	// of the query.  This mapping is used by getQuerySchema()
	// method to determine the output schema of the query.	
	if (numQueries >= MAX_QUERIES) {
		LOG << "PlanManagerImpl:: too many queries" << endl;
		return -1;
	}
	
	queryOutOps [numQueries].queryId = monitorId;
	queryOutOps [numQueries].opId = phyPlan -> id;
	numQueries ++;
	
	// Add the output operator
	if (numOutputs >= MAX_OUTPUT) {
		LOG << "PlanManagerImpl::too man outputs" << endl;
		return -1;
	}
	
	// Create an output operator that reads from the query
	if ((rc = mk_output (phyPlan, outputOp)) != 0)
		return rc;
	queryOutputs [numOutputs] = output;		
	outputOp -> u.OUTPUT.outputId = numOutputs;
	outputOp -> u.OUTPUT.queryId = monitorId;
	numOutputs ++;	
	
	phyPlan = outputOp;
	
	return 0;	
}

int PlanManagerImpl::optimize_plan_mon (Operator *&plan,
										Operator **opList,
										unsigned int &numOps)
{
	int rc;
	
	if ((rc = mergeSelects_mon (plan, opList, numOps)) != 0)
		return rc;
	if ((rc = mergeProjectsToJoin_mon (plan, opList, numOps)) != 0)
		return rc;
	
	return 0;
}

int PlanManagerImpl::add_aux_structures_mon (Physical::Operator *&plan,
											 Physical::Operator **opList,
											 unsigned int &numOps)
{
	int rc;
	
	if ((rc = addIntAggrs_mon (plan, opList, numOps)) != 0)
		return rc;
	if ((rc = add_queues_mon (plan, opList, numOps)) != 0)
		return rc;
	if ((rc = add_syn_mon (plan, opList, numOps)) != 0)
		return rc;
	if ((rc = add_store_mon (plan, opList, numOps)) != 0)
		return rc;
	if ((rc = set_in_stores_mon (plan, opList, numOps)) != 0)
		return rc;

	return 0;
}

static const unsigned int MAX_STORE_MON = 20;

int PlanManagerImpl::instantiate_mon (Physical::Operator *&plan,
									  Physical::Operator **opList,
									  unsigned int &numOps)
{
	int rc;
	Store *storeList [MAX_STORE_MON];
	unsigned int numMonStores;
	
	if ((rc = inst_ops_mon (plan, opList, numOps)) != 0)
		return rc;
	if ((rc = inst_queues ()) != 0)
		return rc;

	// Get a list of stores created for the monitor plan
	numMonStores = 0;
	for (unsigned int s = 0 ; (s < numStores) && (numMonStores < MAX_STORE_MON)
			 ; s++) {				
		if (isNewOp (stores[s].ownOp, opList, numOps)) {
			storeList [numMonStores++] = (stores + s);
		}
	}
	if (numMonStores >= MAX_STORE_MON)
		return -1;
	
	if ((rc = link_syn_store_mon (storeList, numMonStores)) != 0)
		return rc;
	
	if ((rc = link_in_stores_mon (opList, numOps)) != 0)
		return rc;

	return 0;
}									   

static int getOpList (Operator *plan, Operator **opList, unsigned int &numOps)
{
	int rc;
	
	// Internal error: more operators than we predicted
	if (numOps >= MAX_OPS_MON)
		return -1;
	
	// add the current operator
	opList [numOps++] = plan;
	
	// get the list for the children
	for (unsigned int c = 0 ; c < plan -> numInputs ; c++)
		if ((rc = getOpList (plan -> inputs [c], opList, numOps)) != 0)
			return rc;
	
	return 0;
}

static bool isNewOp (Operator *op, Operator **opList, unsigned int numOps)
{
	for (unsigned int o = 0 ; o < numOps ; o++)
		if (op == opList [o])
			return true;
	return false;
}

static int getStoreList (Operator **opList, unsigned int numOps,
						 Store **storeList, unsigned int& numStores)
{
	numStores = 0;
	for (unsigned int o = 0 ; o < numOps ; o++) {
		
		// the operator contains a store
		if (opList [o] -> store) {
			
			// we don't have space for adding a store
			if (numStores >= MAX_STORES_MON)
				return -1;
			
			storeList [numStores ++] = opList [o] -> store;
		}
		
		// some operators contain special stores
		if (opList [o] -> kind == PO_ISTREAM &&
			opList [o] -> u.ISTREAM.nowStore) {
			
			// we don't have space for adding a store
			if (numStores >= MAX_STORES_MON)
				return -1;
			
			storeList [numStores++] = opList [o] -> u.ISTREAM.nowStore;
		}
		
		if (opList [o] -> kind == PO_DSTREAM &&
			opList [o] -> u.DSTREAM.nowStore) {
			
			// we don't have space for adding a store
			if (numStores >= MAX_STORES_MON)
				return -1;
			
			storeList [numStores++] = opList [o] -> u.DSTREAM.nowStore;
		}
		
		if (opList [o] -> kind == PO_EXCEPT &&
			opList [o] -> u.EXCEPT.countStore) {
			
			// we don't have space for adding a store
			if (numStores >= MAX_STORES_MON)
				return -1;
			
			storeList [numStores++] = opList [o] -> u.EXCEPT.countStore;
		}
	}

	return 0;
}

#define TEST_AND_ADD(syn) {              \
   if (syn) {                            \
	   if (numSyns >= MAX_SYNS_MON)      \
		   return -1;                    \
	   synList [numSyns++] = syn;        \
   }                                     \
} 

static int getSynList (Operator **opList, unsigned int numOps,
					   Synopsis **synList, unsigned int& numSyns)
{
	Operator *op;
	
	numSyns = 0;

	for (unsigned int o = 0 ; o < numOps ; o++) {
		op = opList [o];

		switch (op -> kind) {

			// no synopsis
		case PO_SELECT:
		case PO_STREAM_SOURCE:
		case PO_OUTPUT:
		case PO_PROJECT:
		case PO_SINK:
		case PO_SS_GEN:
		case PO_QUERY_SOURCE:
			break;
			
		case PO_JOIN:
			TEST_AND_ADD(op -> u.JOIN.innerSyn);
			TEST_AND_ADD(op -> u.JOIN.outerSyn);
			TEST_AND_ADD(op -> u.JOIN.joinSyn);
			break;
			
		case PO_STR_JOIN:
			TEST_AND_ADD(op -> u.STR_JOIN.innerSyn);
			break;			
			
		case PO_JOIN_PROJECT:
			TEST_AND_ADD(op -> u.JOIN_PROJECT.innerSyn);
			TEST_AND_ADD(op -> u.JOIN_PROJECT.outerSyn);
			TEST_AND_ADD(op -> u.JOIN_PROJECT.joinSyn);
			break;
			
		case PO_STR_JOIN_PROJECT:
			TEST_AND_ADD(op -> u.STR_JOIN_PROJECT.innerSyn);
			break;
			
		case PO_GROUP_AGGR:
			TEST_AND_ADD(op -> u.GROUP_AGGR.inSyn);
			TEST_AND_ADD(op -> u.GROUP_AGGR.outSyn);
			break;
			
		case PO_DISTINCT:
			TEST_AND_ADD(op -> u.DISTINCT.outSyn);
			break;
			
		case PO_ROW_WIN:
			TEST_AND_ADD(op -> u.ROW_WIN.winSyn);
			break;
			
		case PO_RANGE_WIN:
			TEST_AND_ADD(op -> u.RANGE_WIN.winSyn);
			break;
			
		case PO_PARTN_WIN:
			TEST_AND_ADD(op -> u.PARTN_WIN.winSyn);
			break;
			
		case PO_ISTREAM:
			TEST_AND_ADD(op -> u.ISTREAM.nowSyn);
			break;
			
		case PO_DSTREAM:
			TEST_AND_ADD(op -> u.DSTREAM.nowSyn);
			break;
			
		case PO_RSTREAM:
			TEST_AND_ADD(op -> u.RSTREAM.inSyn);
			break;
			
		case PO_UNION:
			TEST_AND_ADD(op -> u.UNION.outSyn);
			break;
			
		case PO_EXCEPT:
			TEST_AND_ADD(op -> u.EXCEPT.countSyn);
			TEST_AND_ADD(op -> u.EXCEPT.outSyn);
			break;
			
		case PO_RELN_SOURCE:
			TEST_AND_ADD(op -> u.RELN_SOURCE.outSyn);
			break;
			

		default:
			ASSERT (0);
			break;
		}
	}

	return 0;
}

static int getQueueList (Operator **opList, unsigned int numOps,
						 Queue **queueList, unsigned int& numQueues)
{
	Operator *op;

	numQueues = 0;
	for (unsigned int o = 0 ; o < numOps ; o++) {
		op = opList [o];
		
		for (unsigned int i = 0 ; i < op -> numInputs ; i++) {
			if (op -> inQueues [i]) {
				if (numQueues >= MAX_QUEUES_MON)
					return -1;
				queueList [numQueues++] = op -> inQueues [i];
			}
		}
	}
	
	return 0;
}

#ifdef _DM_
static bool checkMonitorPlan (Operator *plan)
{
	ASSERT (plan);

	if (plan -> numInputs == 0)
		return (plan -> kind == PO_SS_GEN);
	
	for (unsigned int i = 0 ; i < plan -> numInputs ; i++) {
		if (!checkMonitorPlan (plan -> inputs [i]))
			return false;
	}
	
	return true;
}

static void checkSSGen (Operator *ssgen)
{
	ASSERT (ssgen);
	ASSERT (ssgen -> kind == PO_SS_GEN);
	
	ASSERT (ssgen -> numOutputs == 0);
	for (unsigned int o = 0 ; o < ssgen -> numOutputs ; o++)
		ASSERT (ssgen -> outputs [o] == 0);

	ASSERT (ssgen -> numInputs == 0);
	ASSERT (ssgen -> store == 0);
	ASSERT (ssgen -> outQueue == 0);
	
	return;	
}
#endif

#endif
