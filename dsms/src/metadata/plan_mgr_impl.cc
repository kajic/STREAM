#ifndef _PLAN_MGR_IMPL_
#include "metadata/plan_mgr_impl.h"
#endif

#ifndef _DEBUG_
#include "common/debug.h"
#endif

#ifndef _OP_MONITOR_
#include "execution/monitors/op_monitor.h"
#endif

#ifndef _STORE_MONITOR_
#include "execution/monitors/store_monitor.h"
#endif

#ifndef _PROPERTY_
#include "execution/monitors/property.h"
#endif

#ifndef _SYS_STREAM_
#include "common/sys_stream.h"
#endif

using namespace Metadata;
using namespace Physical;
using namespace std;

PlanManagerImpl::PlanManagerImpl(TableManager *tableMgr, ostream& _LOG)
	: LOG (_LOG)
{
	ASSERT (tableMgr);
	
	this -> tableMgr = tableMgr;
	this -> freeOps = 0;
	this -> usedOps = 0;
	this -> numQueues = 0;
	this -> numSyns = 0;
	this -> numStores = 0;
	this -> numIndexes = 0;
	this -> numExprs = 0;
	this -> numBExprs = 0;
	this -> numBaseTables = 0;
	this -> numOutputs = 0;
	this -> numTables = 0;
	this -> numQueries = 0;
	this -> memMgr = 0;
	this -> staticTupleAlloc = 0;
	
	// Organize all the ops as a linked list
	init_ops ();

#ifdef _SYS_STR_
	// Create the operator that produces system stream (SS)
	createSSGen ();
#endif
}

void PlanManagerImpl::createSSGen ()
{
	Operator *op;
	
	op = new_op (PO_SS_GEN);
	ASSERT (op);
	
	// Schema of SysStream
	op -> numAttrs = SS_NUM_ATTRS;
	for (unsigned int a = 0 ; a < SS_NUM_ATTRS ; a++) {
		op -> attrTypes [a] = SS_ATTR_TYPES [a];
		op -> attrLen [a] = SS_ATTR_LEN [a];
	}
	
	op -> bStream = true;
	op -> store = 0;
	op -> outQueue = 0;
	
	op -> u.SS_GEN.numOutput = 0;
	
	// Register the mapping from SysStream -> op
	ASSERT (numTables < MAX_TABLES);
	sourceOps [numTables].tableId = SS_ID;
	sourceOps [numTables].opId = op -> id;
	numTables ++;	
}

PlanManagerImpl::~PlanManagerImpl() {
	if (memMgr)
		delete memMgr;

	if (staticTupleAlloc)
		delete staticTupleAlloc;
	
	// free ops
	Operator *op = usedOps;
	while (op) {
		if (op -> instOp)
			delete (op -> instOp);
		op = op -> next;
	}
	
	// free queues
	for (unsigned int q = 0 ; q < numQueues ; q++)
		if (queues[q].instQueue)
			delete queues[q].instQueue;

	for (unsigned int s = 0 ; s < numStores ; s++)
		if (stores [s].instStore)
			delete stores[s].instStore;
	
	for (unsigned int i = 0 ; i < numIndexes ; i++)
		delete indexes[i];

	for (unsigned int s = 0 ; s < numSyns ; s++) {
		switch (syns[s].kind) {
		case REL_SYN:
			if (syns[s].u.relSyn)
				delete syns[s].u.relSyn;
			break;
			
		case WIN_SYN:
			if (syns[s].u.winSyn)
				delete syns[s].u.winSyn;
			break;
			
		case PARTN_WIN_SYN:
			if (syns[s].u.pwinSyn)
				delete syns[s].u.pwinSyn;
			break;
			
		case LIN_SYN:
			if (syns[s].u.linSyn)
				delete syns[s].u.linSyn;
			break;
			
		default:
			break;
		}

	}
	
}

/**
 * Create a new operator which acts as the operator-source for this
 * table.  Every plan that uses this base table involves this operator. 
 */ 

int PlanManagerImpl::addBaseTable(unsigned int tableId,
								  Interface::TableSource *source)
{
	Operator *op;	
	
	// The base table is a stream:
	if(tableMgr -> isStream(tableId)) {
		
		// Get a new stream source operator
		op = new_op (PO_STREAM_SOURCE);
		
		// We ran out of resources!
		if (!op) {
			LOG << "PlanManager: out of space for operators" << endl;
			return -1;
		}
		
		op -> store = 0;
		op -> instOp = 0;
		op -> u.STREAM_SOURCE.strId = tableId;
		op -> u.STREAM_SOURCE.srcId = numBaseTables;		
		op -> bStream = true;
	}
	
	// base table is a relation	
	else {
		
		// new relation source operator
		op = new_op (PO_RELN_SOURCE);
		
		// We ran out of resources!
		if (!op) {
			LOG << "PlanManager: out of space for operators" << endl;
			return -1;
		}
		
		op -> store = 0;
		op -> instOp = 0;
		op -> u.RELN_SOURCE.relId = tableId;
		op -> u.RELN_SOURCE.outSyn = 0;
		op -> u.RELN_SOURCE.srcId = numBaseTables;
		op -> bStream = false;
	}
	
	// Output schema of the operator = schema of the stream
	op -> numAttrs = tableMgr -> getNumAttrs (tableId);
	
	if (op -> numAttrs > MAX_ATTRS) {
		LOG << "PlanManager: too many attributes in the table" << endl;
		return -1;
	}
	
	for (unsigned int a = 0 ; a < op -> numAttrs ; a++) {
		op -> attrTypes [a] = tableMgr -> getAttrType (tableId, a);		
		op -> attrLen [a] = tableMgr -> getAttrLen (tableId, a);
	}
	
	// Store the tableSource
	if (numBaseTables >= MAX_TABLES) {
		LOG << "PlanManager: out of space for base tables" << endl;
		return -1;
	}
	baseTableSources [numBaseTables ++] = source;

#ifdef _DM_
	// Have we seen this guy before?
	for (unsigned int t = 0 ; t < numTables ; t++) 
		ASSERT (sourceOps [t].tableId != tableId);
#endif	
	
	// Store the fact that "op" is the source operator for tableId
	if(numTables >= MAX_TABLES) {
		LOG << "PlanManager: out of space for tables" << endl;
		return -1;
	}
	sourceOps [numTables].tableId = tableId;
	sourceOps [numTables].opId = op -> id;
	numTables ++;
	
	return 0;	
}

int PlanManagerImpl::addLogicalQueryPlan (unsigned int queryId,
										  Logical::Operator *logPlan,
										  Interface::QueryOutput *output)
{
	int rc;
	Physical::Operator *phyPlan, *rootOp;
	Physical::Operator *outputOp;
	Physical::Operator *querySource;
	
	// generate the physical plan for the query
	if ((rc = genPhysicalPlan (logPlan, phyPlan)) != 0)
		return rc;

	// root operator for the plan
	rootOp = phyPlan;

	// Generate  a query-source  operator  for this  query.  Query  source
	// operator  is a no-op  operator that  sits on  top of  queries.  any
	// other query that  uses the result of this query  reads off from the
	// query source operator.  This is  a dummy operator found only at the
	// metadata  level.    When  we  instantiate   the  actual  executable
	// operators, we  bypass this dummy operator.   This operator prevents
	// certain incorrect optimizations  (e.g., pushing selections from one
	// query to another) from happening.   
	if ((rc = mk_qry_src (phyPlan, querySource)) != 0)
		return rc;
	
	// Store the <queryId, querySrc> pair - this is used if the queryId is
	// an  intermediate query  whose results  are used  by  other queries.
	// Then we can "attach" the base of other queries to this operator.	
	if (numQueries >= MAX_QUERIES) {
		LOG << "PlanManagerImpl:: too many queries" << endl;
		return -1;
	}
	
	queryOutOps [numQueries].queryId = queryId;
	queryOutOps [numQueries].opId = querySource -> id;
	numQueries ++;
	
	// If the the output of this query is required externally (parameter
	// output is not null), we need to create a specific operator that
	// interfaces outside the system.
	if (output) {
		if (numOutputs >= MAX_OUTPUT) {
			LOG << "PlanManagerImpl:: too many outputs" << endl;
			return -1;
		}
		
		// Create an output operator that reads from the query
		if ((rc = mk_output (phyPlan, outputOp)) != 0)
			return rc;
		
		queryOutputs [numOutputs] = output;		
		outputOp -> u.OUTPUT.outputId = numOutputs;
		outputOp -> u.OUTPUT.queryId = queryId;
		numOutputs ++;
	}
	
	return 0;
}

int PlanManagerImpl::getQuerySchema (unsigned int queryId,
									 char *schemaBuf,
									 unsigned int schemaBufLen)
{
	bool outOpFound;
	unsigned int outOpId;
	Operator *op;
	
	if (!schemaBuf)
		return -1;
	
	// Get the operator that outputs the result of the query
	outOpFound = false;
	for (unsigned int q = 0 ; (q < numQueries) && !outOpFound ; q++) {
		if (queryOutOps [q].queryId == queryId) {
			outOpId = queryOutOps[q].opId;
			outOpFound = true;
		}
	}
	
	// Hmm... I don't know about the this query?
	if (!outOpFound) {
		return -1;
	}
	op = (ops + outOpId);
	
	char *ptr = schemaBuf;
	unsigned int nused;
	
	// Encode: <schema ...>
	const char *typeStr = (op -> bStream)? "stream" : "relation";
	nused = snprintf (ptr, schemaBufLen,
					  "<schema query =\"%d\" type = \"%s\">\n",
					  queryId, typeStr);
	if (nused == schemaBufLen - 1)
		return -1;
	schemaBufLen -= nused;
	ptr += nused;
	
	// Encode the columns
	for (unsigned int a = 0 ; a < op -> numAttrs ; a++) {
		switch (op -> attrTypes [a]) {
		case INT:
			
			nused = snprintf (ptr, schemaBufLen,
							  "<column type = \"int\"/>\n");
			if (nused == schemaBufLen - 1)
				return -1;
			schemaBufLen -= nused;
			ptr += nused;
			break;
			
		case FLOAT:
			
			nused = snprintf (ptr, schemaBufLen,
							  "<column type = \"float\"/>\n");
			if (nused == schemaBufLen - 1)
				return -1;
			schemaBufLen -= nused;
			ptr += nused;
			break;
						
		case CHAR:

			nused = snprintf (ptr, schemaBufLen,
							  "<column type = \"char\" len = \"%d\"/>\n",
							  op -> attrLen [a]);
			if (nused == schemaBufLen - 1)
				return -1;
			schemaBufLen -= nused;
			ptr += nused;
			break;
						
		case BYTE:

			nused = snprintf (ptr, schemaBufLen,
							  "<column type = \"byte\"/>\n");
			if (nused == schemaBufLen - 1)
				return -1;
			schemaBufLen -= nused;
			ptr += nused;
			break;
			
		default:
			return -1;
		}
	}

	nused = snprintf (ptr, schemaBufLen, "</schema>\n");
	if (nused == schemaBufLen - 1)
		return -1;
	
	return 0;
}

int PlanManagerImpl::map (unsigned int queryId, unsigned int tableId)
{
	Operator *queryOutOp;
	
	// Get the output operator for the query
	queryOutOp = 0;	
	for (unsigned int q = 0 ; q < numQueries ; q++) {
		if (queryOutOps [q].queryId == queryId) {
			queryOutOp = ops + queryOutOps [q].opId;						
			break;
		}
	}

	// We did not find an outut operator for this query. (this should
	// never happen during correct execution
	if (!queryOutOp)
		return -1;
   	
	// We want to make sure that the schema of the table and the query are
	// identical.
	
	if (tableMgr -> getNumAttrs (tableId) != queryOutOp -> numAttrs)				
		return -1;
	
	for (unsigned int a = 0 ; a < queryOutOp -> numAttrs ; a++) {
		
		if (tableMgr -> getAttrType (tableId, a) !=
			queryOutOp -> attrTypes [a]) {
			return -1;
		}
		
		if (tableMgr -> getAttrLen (tableId, a) !=
			queryOutOp -> attrLen [a]) {
			LOG << "Reg len: " << tableMgr -> getAttrLen (tableId, a)
				<< endl;
			LOG << "Qry len: " << queryOutOp -> attrLen [a]
				<< endl;
			
			return -1;
		}
	}
	
	if (numTables >= MAX_TABLES) {
		LOG << "PlanManagerImpl:: too many sources" << endl;
		return -1;
	}		
	
	sourceOps [numTables].tableId = tableId;
	sourceOps [numTables].opId = queryOutOp -> id;
	numTables ++;
	
	return 0;
}

/**
 * [[ To be implemented ]]
 */
int PlanManagerImpl::optimize_plan ()
{
	int rc;
	
	LOG << endl << "Optimizing plan" << endl << endl;
	
	// remove all query sources
	if((rc = removeQuerySources()) != 0)
		return rc;
	
	if ((rc = addSinks ()) != 0)
		return rc;
	
	//LOG << endl << endl;
	//printPlan();
	
	if ((rc = mergeSelects ()) != 0)
		return rc;

	//LOG << endl << endl;
	//printPlan();
	
	if ((rc = mergeProjectsToJoin()) != 0)
		return rc;

	//LOG << endl << endl;
	//printPlan();
	
	return 0;
}

/**
 * This method is called after the optimize_plan() method.
 * This adds all the auxiliary (non-operator) structures lik
 * synopses, stores, queues to the plan.
 */ 
int PlanManagerImpl::add_aux_structures ()
{
	int rc;   
	
	if ((rc = addIntAggrs ()) != 0)
		return rc;
	
	if ((rc = add_syn ()) != 0)
		return rc;
	
	if ((rc = add_store ()) != 0)
		return rc;	
	
	if ((rc = add_queues ()) != 0)
		return rc;
	
	if ((rc = set_in_stores ()) != 0)
		return rc;
	
#ifdef _DM_
	LOG << endl << endl;
	printPlan();
#endif
	
	return 0;
}


/**
 * [[ To be implemented ]]
 */

int PlanManagerImpl::instantiate ()
{
	int rc;
	
	if ((rc = inst_mem_mgr ()) != 0)
		return rc;	
	
	if ((rc = inst_static_tuple_alloc ()) != 0)
		return rc;
	
	if ((rc = inst_ops ()) != 0)
		return rc;
	
	if ((rc = inst_queues ()) != 0)
		return rc;
	
	if ((rc = link_syn_store ()) != 0)
		return rc;
	
	if ((rc = link_in_stores ()) != 0)
		return rc;

	// instantiate system stream generator: first we need to locate
	// the ss_gen Physical::Operator
	Operator *ss_gen;
	ss_gen = usedOps;
	while (ss_gen) {
		if (ss_gen -> kind == PO_SS_GEN) 
			break;
		ss_gen = ss_gen -> next;
	}
	
	if (!ss_gen) {
		LOG << "PlanManager: SysStream Gen operator not found" << endl;
		return -1;
	}
	
	if ((rc = inst_ss_gen (ss_gen)) != 0)
		return rc;
	
	return 0;
}

int PlanManagerImpl::initScheduler (Execution::Scheduler *sched)
{
	int rc;
	Operator *op;

	op = usedOps;
	
	while (op) {

		ASSERT (op -> instOp);
		
		if ((rc = sched -> addOperator (op -> instOp)) != 0)
			return rc;
		
		op = op -> next;
	}
	
	return 0;
}



#ifdef _DM_
#include <iostream>
#include "metadata/phy_op_debug.h"

void PlanManagerImpl::printPlan()
{
	LOG << "PrintPlan: begin" << endl;
	Operator *op = usedOps;

	LOG <<
		"---------------------------- OPERATORS -----------------------"
		<< endl;
	
	while (op) {
		LOG << "Op: " << op -> id << endl;
		LOG << op << endl;
		op = op -> next;
	}

	LOG << "PrintPlan: after ops" << endl;
	
	LOG <<
		"---------------------------- SYNOPSES -----------------------"
		<< endl;
	
	for (unsigned int s = 0 ; s < numSyns ; s++)
		LOG << syns + s << endl;
	
	LOG <<
		"---------------------------- STORES -----------------------"
		<< endl;
	
	for (unsigned int s = 0 ; s < numStores ; s++)
		LOG << stores + s << endl;	

	LOG << "PrintPlan: end" << endl;
	return;
}
#endif

int PlanManagerImpl::printStat ()
{
	int rc;
	
#ifdef _MONITOR_

	// Operator times
	Operator *op = usedOps;
	double timeTaken;
	int lastOutTs;
	double total = 0;
	
	while (op) {

		ASSERT (op -> instOp);
		if ((rc = op -> instOp -> getDoubleProperty (Monitor::OP_TIME_USED,
													 timeTaken)) != 0)
			return rc;

		if ((rc = op -> instOp -> getIntProperty (Monitor::OP_LAST_OUT_TS,
												  lastOutTs)) != 0)
			return rc;
		
		total += timeTaken;
		LOG << "Operator [" << op -> id << "]"
			<< " Time = " 
			<< timeTaken
			<< " Lot = "
			<< lastOutTs
			<< endl;
		
		op = op -> next;
	}
	
	LOG << "Total Time: " << total << endl;
	
	// Store memories
	int maxPages, numPages;
	for (unsigned int s = 0 ; s < numStores ; s++) {
		ASSERT (stores [s].instStore);
		if ((rc = stores [s].instStore ->
			 getIntProperty (Monitor::STORE_MAX_PAGES, maxPages)) != 0)
			return rc;
		
		if ((rc = stores [s].instStore ->
			 getIntProperty (Monitor::STORE_NUM_PAGES, numPages)) != 0)
			return rc;
		
		
		LOG << "Store [" << s << "]: <"
			<< maxPages
			<< ","
			<< numPages
			<< ">";
		
		LOG << endl;
	}

#if 0	
	int numEntries, numBuckets, numNonMt;
	for (unsigned int i = 0 ; i < numIndexes ; i++) {

		if ((rc = indexes [i] -> getIntProperty 
			 (Monitor::HINDEX_NUM_BUCKETS, numBuckets)) != 0)
			return rc;
		if ((rc = indexes [i] -> getIntProperty
			 (Monitor::HINDEX_NUM_NONMT_BUCKETS, numNonMt)) != 0)
			return rc;
		if ((rc = indexes [i] -> getIntProperty
			 (Monitor::HINDEX_NUM_ENTRIES, numEntries)) != 0)
			return rc;

		LOG << "Index [" << i << "]: <"
			<< numBuckets
			<< ","
			<< numNonMt
			<< ","
			<< numEntries
			<< ">"
			<< endl;
	}	
	
	// Synopsis sizez
	int maxTuples, numTuples;	
	for (unsigned int s = 0 ; s < numSyns ; s++) {

		switch (syns[s].kind) {
		case REL_SYN:
			ASSERT (syns [s].u.relSyn);
			rc = syns [s].u.relSyn ->
				getIntProperty (Monitor::SYN_MAX_TUPLES, maxTuples);
			if (rc != 0) return rc;
			
			rc = syns [s].u.relSyn ->
				getIntProperty (Monitor::SYN_NUM_TUPLES, numTuples);
			if (rc != 0) return rc;			
			
			break;
			
		case WIN_SYN:
			ASSERT (syns [s].u.winSyn);
			rc = syns [s].u.winSyn ->
				getIntProperty (Monitor::SYN_MAX_TUPLES, maxTuples);
			if (rc != 0) return rc;
			
			rc = syns [s].u.winSyn ->
				getIntProperty (Monitor::SYN_NUM_TUPLES, numTuples);
			if (rc != 0) return rc;
			break;
			
		case LIN_SYN:
			ASSERT (syns [s].u.linSyn);
			rc = syns [s].u.linSyn ->
				getIntProperty (Monitor::SYN_MAX_TUPLES, maxTuples);
			if (rc != 0) return rc;
			
			rc = syns [s].u.linSyn ->
				getIntProperty (Monitor::SYN_NUM_TUPLES, numTuples);
			if (rc != 0) return rc;
			break;
			
		case PARTN_WIN_SYN:
			ASSERT (syns [s].u.pwinSyn);
			rc = syns [s].u.pwinSyn ->
				getIntProperty (Monitor::SYN_MAX_TUPLES, maxTuples);
			if (rc != 0) return rc;
			
			rc = syns [s].u.pwinSyn ->
				getIntProperty (Monitor::SYN_NUM_TUPLES, numTuples);
			if (rc != 0) return rc;
			break;
			
		default:
			ASSERT (0);
			break;
		}

		LOG << "Synopsis [" << s << "]: <"
			<< maxTuples
			<< ","
			<< numTuples
			<< ">"
			;
		
		LOG << endl;					
	}
#endif

	
#endif
	
	return 0;
}
