/**
 * @file       server_impl.cc
 * @date       May 22, 2004
 * @brief      Implementation of the STREAM server.
 */

/// debug
#include <iostream>
using namespace std;


#ifndef _ERROR_
#include "interface/error.h"
#endif

#ifndef _DEBUG_
#include "common/debug.h"
#endif

#ifndef _SERVER_IMPL_
#include "server/server_impl.h"
#endif

#ifndef _PARSER_
#include "parser/parser.h"
#endif

#ifndef _NODES_
#include "parser/nodes.h"
#endif

#ifndef _QUERY_
#include "querygen/query.h"
#endif

#ifndef _LOGOP_
#include "querygen/logop.h"
#endif

#ifndef _ROUND_ROBIN_
#include "execution/scheduler/round_robin.h"
#endif

#ifndef _CONFIG_FILE_READER_
#include "server/config_file_reader.h"
#endif

#ifndef _PARAMS_
#include "server/params.h"
#endif

#ifdef _DM_
#include "querygen/query_debug.h"
#include "parser/nodes_debug.h"
#include "querygen/logop_debug.h"
#endif

using namespace std;

// Helper functions ...
static int registerRelation (NODE *parseTree,
							 Metadata::TableManager *tableMgr,
							 unsigned int &relId);

static int registerStream (NODE *parseTree,
						   Metadata::TableManager *tableMgr,
						   unsigned int &strId);


ServerImpl::ServerImpl(ostream &_LOG)
	: LOG (_LOG)
{
	state             = S_INIT;
	tableMgr          = 0;
	qryMgr            = 0;
	planMgr           = 0;
	scheduler         = 0;
	
	// Set default values of various server params
	MEMORY            = MEMORY_DEFAULT;
	QUEUE_SIZE        = QUEUE_SIZE_DEFAULT;
	SHARED_QUEUE_SIZE = SHARED_QUEUE_SIZE_DEFAULT;
	INDEX_THRESHOLD   = INDEX_THRESHOLD_DEFAULT;
	SCHEDULER_TIME    = SCHEDULER_TIME_DEFAULT;
	CPU_SPEED         = CPU_SPEED_DEFAULT;
	
	pthread_mutex_init (&mutex, NULL);
	pthread_cond_init (&mainThreadWait, NULL);
	pthread_cond_init (&interruptThreadWait, NULL);
}

ServerImpl::~ServerImpl()
{
	if (logPlanGen)
		delete logPlanGen;

	if (semInterpreter)
		delete semInterpreter;
	
	if (tableMgr)
		delete tableMgr;
	
	if (planMgr)
		delete planMgr;
	
	if (qryMgr)
		delete qryMgr;

	if (scheduler)
		delete scheduler;
}


/**
 * Begin the specification of an application.
 *
 * Server should be in INIT state when this method id called.  It moves to
 * S_APP_SPEC state when it returns from this method.  
 *
 * Also server starts up:
 *
 * 1. Table manager to manage named streams and relations (input &
 *    derived)
 */

int ServerImpl::beginAppSpecification() 
{
	// State transition
	if(state != S_INIT)
		return INVALID_USE_ERR;
	state = S_APP_SPEC;
	
	// Table manager
	tableMgr = new Metadata::TableManager();
	if(!tableMgr)
		return INTERNAL_ERR;

	// Query Manager
	qryMgr = new Metadata::QueryManager(LOG);
	if (!qryMgr)
		return INTERNAL_ERR;
	
	// Plan manager
	planMgr = Metadata::PlanManager::newPlanManager(tableMgr, LOG);
	if(!planMgr)
		return INTERNAL_ERR;
	
	// Semantic Interpreter
	semInterpreter = new SemanticInterpreter(tableMgr);
	if(!semInterpreter)
		return INTERNAL_ERR;
	
	// Logical plan generator
	logPlanGen = new LogPlanGen (tableMgr);
	if (!logPlanGen)
		return INTERNAL_ERR;
	
	return 0;
}

/**
 * Register a table (stream or a relation):
 *
 * The informatin about a table is encoded as a string.  This string is
 * first parsed using the parser.
 */

int ServerImpl::registerBaseTable(const char *tableInfo,
								  unsigned int tableInfoLen,
								  Interface::TableSource *input)
{
	int rc;
	NODE  *parseTree;
	unsigned int tableId;

	// Tables can be registered only in S_APP_SPEC mode (before
	// endApplicationSpec() has been called)
	if (state != S_APP_SPEC)
		return INVALID_USE_ERR;	
	
	// Sanity check
	if (!tableInfo)
		return INVALID_PARAM_ERR;
	
	// Parse table info
	parseTree = Parser::parseCommand(tableInfo, tableInfoLen);
	if(!parseTree)
		return PARSE_ERR;
	
	// Store the parsed info with the table manager
	if(parseTree -> kind == N_REL_SPEC) {
		if ((rc = registerRelation (parseTree, tableMgr, tableId)) != 0)
			return rc;
	}
	else if(parseTree -> kind == N_STR_SPEC) {
		if ((rc = registerStream (parseTree, tableMgr, tableId)) != 0)
			return rc;
	}
	
	// Unknown command: parse error
	else {
		return PARSE_ERR;
	}
	
	// Inform the plan manager about the new table
	if ((rc = planMgr -> addBaseTable (tableId, input)) != 0)
		return rc;
	
	return 0;	
}

/**
 *  [[ To be implemented ]] 
 */ 
int ServerImpl::registerQuery (const char *querySpec, 
							   unsigned int querySpecLen,
							   Interface::QueryOutput *output,
							   unsigned int &queryId)
{	
	NODE *parseTree;
	Semantic::Query semQuery;
	Logical::Operator *logPlan;
	int rc;
	
	// This method can be called only in S_APP_SPEC state
	if (state != S_APP_SPEC)
		return INVALID_USE_ERR;
	
	// Query String -> Parse Tree
	parseTree = Parser::parseCommand(querySpec, querySpecLen);
	if (!parseTree) {
		return PARSE_ERR;
	}
	
	// Parse Tree -> Semantic Query
	if((rc = semInterpreter -> interpretQuery (parseTree, semQuery)) != 0) {
		return rc;
	}

	/// Debug
#ifdef _DM_
	//printQuery (cout, semQuery, tableMgr);
#endif
	
	// At this point there are no errors in the query (syntactic or
	// semantic), so we register the query and get an internal id for it
	if ((rc = qryMgr -> registerQuery (querySpec, querySpecLen, queryId))
		!= 0) {
		return rc;
	}
	
	
	// SemanticQuery -> logical plan
	if ((rc = logPlanGen -> genLogPlan (semQuery, logPlan)) != 0) {
		return rc;
	}
	
#ifdef _DM_
	//cout << endl << endl << logPlan << endl;
#endif
	
	// Logical Plan -> Physical Plan (stored with plan manager)
	if ((rc = planMgr -> addLogicalQueryPlan (queryId, logPlan, output)) != 0){
		return rc;
	}
	
#ifdef _DM_
	//cout << endl << endl;
	//planMgr -> printPlan();
#endif
	
	return 0;
}

#ifdef _SYS_STR_
	
/**
 * Register a monitor query with the server.
 */

int ServerImpl::registerMonitor (const char *monitorSpec,
								 unsigned int monitorSpecLen,
								 Interface::QueryOutput *output,
								 unsigned int &monitorId)
{
	NODE *parseTree;
	Semantic::Query semQuery;
	Logical::Operator *logPlan;
	int rc;

	ASSERT (output);

	LOG << "Server: trying to interrupt exec ...";
	
	// Interrupt the server execution to register the monitor
	interruptExecution ();

	LOG << "done" << endl;
	
	// If we are not in the interrupted state, it means that
	// the server was not executing when we tried to interrupt
	if (state != S_INT) {
		LOG << "Register monitor when server not executing" << endl;		
		return 0;
	}

	LOG << "parsing..." << endl;
	
	// Query String -> Parse Tree
	parseTree = Parser::parseCommand(monitorSpec, monitorSpecLen);
	if (!parseTree) {
		resumeExecution ();
		return PARSE_ERR;
	}

	LOG << "Seminterp..." << endl;
	
	// Parse Tree -> Semantic Query
	if((rc = semInterpreter -> interpretQuery (parseTree, semQuery)) != 0) {
		resumeExecution ();
		return rc;
	}	

	LOG << "log plan..." << endl;
	
	// SemanticQuery -> logical plan
	if ((rc = logPlanGen -> genLogPlan (semQuery, logPlan)) != 0) {
		resumeExecution ();
		return rc;
	}

	LOG << "register query" << endl;
	
	// At this point there are no errors in the monitor query (syntactic or
	// semantic), so we register the query and get an internal id for it
	if ((rc = qryMgr -> registerQuery (monitorSpec, monitorSpecLen,
									   monitorId)) != 0) {
		resumeExecution ();
		return rc;
	}

	LOG << "add monitor" <<endl;
	
	if ((rc = planMgr -> addMonitorPlan (monitorId, logPlan,
										 output, scheduler)) != 0)
		return rc;

	LOG << "resume exec..." << endl;
	
	resumeExecution ();

	LOG << "done" << endl;
	
	return 0;
}

#endif


int ServerImpl::getQuerySchema (unsigned int queryId,
								char *schemaBuf,
								unsigned int schemaBufLen) {

#ifndef _SYS_STR_
	if (state != S_APP_SPEC)
		return INVALID_USE_ERR;
#endif
	
	return planMgr -> getQuerySchema (queryId, schemaBuf, schemaBufLen);
}

int ServerImpl::registerView(unsigned int queryId,
							 const char *tableInfo,
							 unsigned int tableInfoLen)
{
	int rc;
	NODE  *parseTree;
	unsigned int tableId;	
	
	// This method can be called only in S_APP_SPEC state
	if (state != S_APP_SPEC)
		return INVALID_USE_ERR;

	// Sanity check
	if (!tableInfo)
		return INVALID_PARAM_ERR;

	// Parse table info
	parseTree = Parser::parseCommand(tableInfo, tableInfoLen);
	if(!parseTree)
		return PARSE_ERR;
	
	// Store the parsed info with the table manager
	if(parseTree -> kind == N_REL_SPEC) {
		if ((rc = registerRelation (parseTree, tableMgr, tableId)) != 0)
			return rc;
	}
	
	else if(parseTree -> kind == N_STR_SPEC) {
		if ((rc = registerStream (parseTree, tableMgr, tableId)) != 0)
			return rc;
	}
	
	// Unknown command: parse error
	else {
		return PARSE_ERR;
	}
	
	// Indicate the mapping from query -> tableId to the plan manager
	if ((rc = planMgr -> map (queryId, tableId)) != 0)
		return rc;
	
	return 0;
}

/**
 * [[ to be implemented ]]
 */ 
int ServerImpl::endAppSpecification() 
{
	int rc;

	if (state != S_APP_SPEC)
		return INVALID_USE_ERR;
	
	if ((rc = planMgr -> optimize_plan ()) != 0)
		return rc;

	if ((rc = planMgr -> add_aux_structures ()) != 0)
		return rc;

	if ((rc = planMgr -> instantiate ()) != 0)
		return rc;
	
	scheduler = new Execution::RoundRobinScheduler();
	
	if ((rc = planMgr -> initScheduler (scheduler)) != 0)
		return rc;
	
	state = S_PLAN_GEN;
	
	return 0;
}

/**
 * Get the plan for execution.  Should be called after endAppSpec()
 * method is called
 */
int ServerImpl::getXMLPlan (char *planBuf, unsigned int planBufLen)
{
#ifdef _SYS_STR_
	if (state != S_PLAN_GEN && state != S_EXEC)
		return INVALID_USE_ERR;
#else
	if (state != S_PLAN_GEN)
		return INVALID_USE_ERR;
#endif
	
	return planMgr -> getXMLPlan (planBuf, planBufLen);
}

/**
 * [[ To be implemented ]]
 */
int ServerImpl::beginExecution()
{
	int rc;

	if (state != S_PLAN_GEN)
		return INVALID_USE_ERR;
		
	// Transition from S_PLAN_GEN to S_EXEC (Critical code)
	pthread_mutex_lock (&mutex);

	// normal sequence
	if (state == S_PLAN_GEN) {
		state = S_EXEC;
	}

	// the execution has been interrupted already 
	else if (state == S_INT) {
		// wake up the interrupting thread
		pthread_cond_signal (&interruptThreadWait);
		
		// sleep until the interruption is over
		pthread_cond_wait (&mainThreadWait, &mutex);
		
		// Resume ..
		state = S_EXEC;
	}
	
	else if (state == S_END) {
		// do nothing
	}
	
	pthread_mutex_unlock (&mutex);	
	
	while (state == S_EXEC) {
		if ((rc = scheduler -> run (SCHEDULER_TIME)) != 0)
			return rc;
		
		ASSERT (state == S_EXEC || state == S_INT || state == S_END);
		pthread_mutex_lock (&mutex);
		
		// natural termination
		if (state == S_EXEC) {
			state = S_END;
		}
		
		// interrupted by interruptExecution()
		else if (state == S_INT) {			
			// wake up the interrupting thread
			pthread_cond_signal (&interruptThreadWait);
			
			// sleep until the interruption is over
			pthread_cond_wait (&mainThreadWait, &mutex);
			
			// Resume ..
			state = S_EXEC;
		}
		
		// terminated by stopExecution()
		else if (state == S_END) {
			// do nothing
		}
		
		pthread_mutex_unlock (&mutex);
	}
	
	ASSERT (state == S_END);
	
	if ((rc = planMgr -> printStat()) != 0)
		return rc;   
	
	return 0;
}

int ServerImpl::stopExecution ()
{
	
	ASSERT (state == S_PLAN_GEN || state == S_EXEC || state == S_END);
	pthread_mutex_lock (&mutex);
	if (state == S_EXEC) {	
		scheduler -> stop ();
	}
	state = S_END;
	pthread_mutex_unlock (&mutex);
	
	return 0;
}

int ServerImpl::interruptExecution ()
{
	ASSERT (state == S_PLAN_GEN || state == S_EXEC || state == S_END);
	
	pthread_mutex_lock (&mutex);
	
	if (state == S_EXEC) {		
		scheduler -> stop ();
		state = S_INT;
		stateWhenInterrupted = S_EXEC;
		
		// wait for the main scheduler thread to give you control
		pthread_cond_wait (&interruptThreadWait, &mutex);
	}
	
	else if (state == S_PLAN_GEN) {
		stateWhenInterrupted = S_PLAN_GEN;
		state = S_INT;
	}
	
	pthread_mutex_unlock (&mutex);
	return 0;
}

int ServerImpl::resumeExecution ()
{
	int rc;
	
	ASSERT (state == S_INT);
	
	pthread_mutex_lock (&mutex);
	
	// This does not actually run the scheduler, but just resets its state
	// so that the next run() call runs without returning immediately.
	if ((rc = scheduler -> resume ()) != 0)
		return rc;
	state = stateWhenInterrupted;

	ASSERT (state == S_EXEC || state == S_PLAN_GEN);
	
	pthread_cond_signal (&mainThreadWait);
	pthread_mutex_unlock (&mutex);
	
	return 0;
}

static int registerRelation (NODE *parseTree,
							 Metadata::TableManager *tableMgr,
							 unsigned int &relId)
{	
	int rc;
	
	const char   *relName;
	NODE         *attrList;	
	NODE         *attrSpec;
 	const char   *attrName;
	Type          attrType;
	unsigned int  attrLen;
	
	// Relation name
	relName = parseTree -> u.REL_SPEC.rel_name;
	ASSERT(relName);	
	
	if((rc = tableMgr -> registerRelation(relName, relId)) != 0)
		return rc;
	
	attrList = parseTree -> u.REL_SPEC.attr_list;
	ASSERT(attrList);
	
	// Process each attribute in the list
	while (attrList) {		
		ASSERT(attrList -> kind == N_LIST);		
		
		// Attribute specification
		attrSpec = attrList -> u.LIST.curr;
		
		ASSERT(attrSpec);
		ASSERT(attrSpec -> kind == N_ATTR_SPEC);
		
		attrName = attrSpec -> u.ATTR_SPEC.attr_name;
		attrType = attrSpec -> u.ATTR_SPEC.type;

		switch (attrType) {
		case INT:
			attrLen = INT_SIZE; break;			
		case FLOAT:
			attrLen = FLOAT_SIZE; break;
		case CHAR:
			attrLen = attrSpec -> u.ATTR_SPEC.len; break;			
		case BYTE:
			attrLen = BYTE_SIZE; break;
		default:
			ASSERT (0);
			break;
		}
		
		if((rc = tableMgr -> addTableAttribute(relId, attrName, 
											   attrType, attrLen)) != 0) {
			tableMgr -> unregisterTable (relId);
			return rc;
		}
		
		// next attribute
		attrList = attrList -> u.LIST.next;
	}
	
	return 0;
}

static int registerStream (NODE *parseTree,
						   Metadata::TableManager *tableMgr,
						   unsigned int &strId)
{
	int rc;
	
	const char   *strName;
	NODE         *attrList;	
	NODE         *attrSpec;
 	const char   *attrName;
	Type          attrType;
	unsigned int  attrLen;

	// Stream name
	strName = parseTree -> u.STR_SPEC.str_name;
	ASSERT(strName);
	
	if((rc = tableMgr -> registerStream(strName, strId)) != 0)
		return rc;
	
	attrList = parseTree -> u.STR_SPEC.attr_list;
	ASSERT(attrList);
	
	// Process each attribute in the list
	while (attrList) {
		
		ASSERT(attrList -> kind == N_LIST);
		
		// Attribute specification
		attrSpec = attrList -> u.LIST.curr;
		
		ASSERT(attrSpec);
		ASSERT(attrSpec -> kind == N_ATTR_SPEC);
		
		attrName = attrSpec -> u.ATTR_SPEC.attr_name;
		attrType = attrSpec -> u.ATTR_SPEC.type;
		
		switch (attrType) {
		case INT:
			attrLen = INT_SIZE; break;			
		case FLOAT:
			attrLen = FLOAT_SIZE; break;
		case CHAR:
			attrLen = attrSpec -> u.ATTR_SPEC.len; break;			
		case BYTE:
			attrLen = BYTE_SIZE; break;
		default:
			ASSERT (0);
			break;
		}
		
		if((rc = tableMgr -> addTableAttribute(strId, attrName, 
											   attrType, attrLen)) != 0) {
			tableMgr -> unregisterTable (strId);
			return rc;
		}
		
		// next attribute
		attrList = attrList -> u.LIST.next;
	}
	
	return 0;
}

Server *Server::newServer(std::ostream& LOG) 
{
	return new ServerImpl(LOG);
}

int ServerImpl::setConfigFile (const char *configFile)
{
	int rc;
	ConfigFileReader *configFileReader;
	ConfigFileReader::Param param;
	ConfigFileReader::ParamVal val;
	bool bValid;
	
	configFileReader = new ConfigFileReader (LOG);
	
	if ((rc = configFileReader -> setConfigFile (configFile)) != 0)
		return rc;
	
	while (true) {
		
		if ((rc = configFileReader -> getNextParam (param, val, bValid))
			!= 0)
			return rc;
		
		if (!bValid)
			break;
		
		switch (param) {
		case ConfigFileReader::MEMORY_SIZE:
			MEMORY = (unsigned int)val.ival;
			break;
			
		case ConfigFileReader::QUEUE_SIZE:
			QUEUE_SIZE = (unsigned int)val.ival;
			break;
			
		case ConfigFileReader::SHARED_QUEUE_SIZE:
			SHARED_QUEUE_SIZE = (unsigned int)val.ival;
			break;
			
		case ConfigFileReader::INDEX_THRESHOLD:
			INDEX_THRESHOLD = val.dval;
			break;
			
		case ConfigFileReader::RUN_TIME:
			SCHEDULER_TIME = (unsigned int)val.lval;
			break;

		case ConfigFileReader::CPU_SPEED:
			CPU_SPEED = (unsigned int)val.lval;
			break;
			
		default:
			break;
		}
	}

	delete configFileReader;
	
	return 0;
}

