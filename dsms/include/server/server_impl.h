#ifndef _SERVER_IMPL_
#define _SERVER_IMPL_

#ifndef _SERVER_
#include "interface/server.h"
#endif

#ifndef _TABLE_MGR_
#include "metadata/table_mgr.h"
#endif

#ifndef _QUERY_MGR_
#include "metadata/query_mgr.h"
#endif

#ifndef _PLAN_MGR_
#include "metadata/plan_mgr.h"
#endif

#ifndef _SEM_INTERP_
#include "querygen/sem_interp.h"
#endif

#ifndef _LOG_PLAN_GEN_
#include "querygen/log_plan_gen.h"
#endif

#ifndef _SCHEDULER_
#include "execution/scheduler/scheduler.h"
#endif

#ifdef _MULTI_THREADED_
#include <pthread.h>
#endif

/**
 * The main STREAM server.  This class is the implementation of the
 * interface presented in interface/server.h
 */

class ServerImpl : public Server {
 private:
	// System Log
	std::ostream                &LOG;
	
	// Functional units
	SemanticInterpreter         *semInterpreter;
	LogPlanGen                  *logPlanGen;
	
	// Stateful units
	Metadata::TableManager      *tableMgr;
	Metadata::QueryManager      *qryMgr;
	Metadata::PlanManager       *planMgr;

	Execution::Scheduler        *scheduler;
	
	/// Possible states of the Server
	enum State {
		// Initial state
		S_INIT,
		
		// Application specification
		S_APP_SPEC,
		
		// Plan generation
		S_PLAN_GEN,
		
		// Execution
		S_EXEC,
		
		// Server interrupted
		S_INT,
		
		// Server finished execution
		S_END		
	};
	
	/// Current state of the server (thread)
	State state;

	/// State of the server when interrupted (meaningful only when
	/// state == S_INT
	State stateWhenInterrupted;
	
	// Mutex for critical code
	pthread_mutex_t mutex;

	// condition variable to wake up the interrupting thread
	pthread_cond_t interruptThreadWait;
	
	// condition variable to wake up the main thread after interruption
	pthread_cond_t mainThreadWait;
	
 public:
	ServerImpl(std::ostream &LOG);
	~ServerImpl();
	
	//----------------------------------------------------------------------
	// External Interface: inherited from Server
	//----------------------------------------------------------------------
	
	/**
	 * Set the configuration file for the server.
	 *
	 * @param configFile     The configuration file
	 * @return  0 (error), !0 (otherwise)
	 */	
	int setConfigFile (const char *configFile);
	
	/**
	 * Begin the specification of an application.  The actual
	 * specification is done using 
	 *
	 * @return           0 (success), !0 (failure)
	 */
	int beginAppSpecification();
	
	/**
	 * Register a base table.  Table is the generic name for a stream or a 
	 * relation.  A base table is an input table.  There could also be
	 * tables that are produced by subqueries - these are intermediate
	 * tables or views.  These are specified using registerView method.
	 *
	 * The information about  the table, which is essentially  the name of
	 * the  table and  the name  of the  attributes in  the schema  of the
	 * table, is encoded in input tableInfo.
	 *
	 * For each base table, and TableSource object should be provided.
	 * TableSource is the interface that the system uses to get input
	 * streams and relations from the "external" world.
	 * 
	 * [[ Details of the encoding of table name, schema ]]
	 * 
	 * @param tableInfo     encoding of the table name, schema
	 * @param tableInfoLen  Length of the string tableInfo
	 *
	 * @return            0 (success), !0 (otherwise)
	 */
	
	int registerBaseTable(const char *tableInfo,
						  unsigned int tableInfoLen,
						  Interface::TableSource *input);
	
	/**
	 * Register a  continuous query that produces an  external output with
	 * the server.  All the tables  referenced by the query should already
	 * have been registered with the server before.
	 *
	 * The output  of a  query is written  to a QueryOutput  object, which
	 * essentially supports a "push" method for the tuples of the query.
	 *
	 * There could  be queries for  which external output is  not desired.
	 * Such a query exists purely to produce output which is used by other
	 * queries (i.e., it is a subquery for another query).  For such
	 * queries the parameter output can be null (0).  
	 *
	 * The method  returns a queryId, which uniquely  identifies the query
	 * with the  dsms for later references.  QueryId  is used specifically
	 * to "name" the output schema of a query so that it can be referenced
	 * in other queries (i.e., create a named view).
	 * 
	 * @param querySpec     specification of the query in CQL syntax
	 * @param querySpecLen  Length of the string querySpec
	 * @param output        QueryOutput object where the output of the 
	 *                      query should be sent.
	 *
	 * @param queryId       returned identifier for the query
	 *
	 *
	 * @return            0 (success), !0 (otherwise)
	 */
	
	int registerQuery(const char *querySpec, 
					  unsigned int querySpecLen, 
					  Interface::QueryOutput *output,
					  unsigned int &queryId);	

	
	/**
	 * Get the output schema of a given query.  The output schema contains
	 * information about:
	 * 
	 * 1. Whether the output of the query is a stream or relation
	 * 2. The type of each attribute of the output query, where the
	 *    attribute positions are fixed by teh query semantics.
	 * (The schema is therefore unnamed)
	 *
	 * The schema is encoded as an XML string.  For example:
	 *  
	 *    <schema query = "1" type = "stream">
	 *       <column type = "int"/>
	 *       <column type = "float/>
	 *       <column type = "char" len = "10"/>
	 *       <column type = "byte"/>
	 *    </schema>
	 */
	
	int getQuerySchema (unsigned int queryId,
						char *schemaBuf,
						unsigned int schemaBufLen);	
		
	/**
	 * Designate one of the previously registered queries as a named view.
	 * The previously  registered query is  identified using its  id which
	 * was returned at registration time.
	 * 
	 * To create a view we have to provide the name and schema information
	 * for the  view -  this is specified  using the  tableInfor parameter
	 * which has the same encoding scheme as in the registerBaseTable
	 * method.
	 *
	 * It is  required that  the schema for  the table info  be consistent
	 * with the output schema of the registered query.  Otherwise an error
	 * is signalled.
	 * 
	 * @param queryId    Identifier for the query
	 * @param tableInfo  encoding of the name & schema of the view.  
	 * @return           0 (success), !0 (otherwise)
	 */
	
	int registerView(unsigned int queryId,
					 const char *tableInfo, unsigned int tableInfoLen);
	
	/**
	 * End the specification of an application.  At this point the server
	 * can allocate resources (read create operators, queues etc) for the
	 * execution of the query.  
	 * 
	 * @return            0 (success), !0 (otherwise)
	 */	
	int endAppSpecification();

	/**
	 * Get the plan for execution.  Should be called after endAppSpec()
	 * method is called
	 */
	int getXMLPlan (char *planBuf, unsigned int planBufLen);
	
	/**
	 * Start the execution of the query: start reading the tuples from all
	 * the table sources and produce the output tuples.  In true spirit of
	 * continuous queries - this never returns :)
	 */
	int beginExecution();

	/**
	 * Stop the execution of the registered queries.  This method can be
	 * called only after beginExecution() is called.  this method is
	 * useful only in multi-threaded execution (e.g., using netserver)
	 */
	int stopExecution ();
	
	//----------------------------------------------------------------------
	// Non-external interface: used by objects within the system
	//----------------------------------------------------------------------
	
	/**
	 * @return  the system-wide table manager.
	 */
	Metadata::TableManager *getTableManager() const;
	
#ifdef _SYS_STR_
	
	/**
	 * Register a monitor query with the server.  This call is
	 * meaningful only in multi-threaded execution.  registerMonitor()
	 * method can be called only when the server is currently executing
	 * within the beginExecution() method.
	 *
	 * @param monitorSpec      The specification of the monitor query
	 * @param monitorSpecLen   The length of the monitorSpec String
	 * @param output           The output to which the results of the
	 *                         monitor query is written
	 * @param monitorId        an id assigned to refer to this monitor later
	 * @return                 0 (success), !0 (failure)
	 */
	
	virtual int registerMonitor (const char *monitorSpec,
								 unsigned int monitorSpecLen,
								 Interface::QueryOutput *output,
								 unsigned int &monitorId);
#endif
	
 private:
	int interruptExecution ();
	int resumeExecution ();
};

#endif
