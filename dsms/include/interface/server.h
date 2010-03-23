#ifndef _SERVER_
#define _SERVER_

#ifndef _QUERY_OUTPUT_
#include "interface/query_output.h"
#endif

#ifndef  _TABLE_SOURCE_
#include "interface/table_source.h"
#endif

#include <ostream>

/**
 * The STREAM server interface.  The external interface allows one to
 * register streams, relations, and continuous queries, and start
 * execution of the continuous queries.  The server operates in two
 * phases: in the first phase all the registering (of CQ, streams, and
 * relations) is done, and in the second phase the continuous queries
 * registered in the first phase are executed.
 *
 */

class Server {
 public:
	virtual ~Server() {}

	/**
	 * Set the configuration file for the server.
	 *
	 * @param configFile     The configuration file
	 * @return  0 (error), !0 (otherwise)
	 */	
	virtual int setConfigFile (const char *configFile) = 0;
	
	/**
	 * Begin the specification of an application.  The actual
	 * specification is done using 
	 *
	 * @return           0 (success), !0 (failure)
	 */
	virtual int beginAppSpecification() = 0;
	
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
	
	virtual int registerBaseTable(const char *tableInfo, 
								  unsigned int tableInforLen,
								  Interface::TableSource *input) = 0;
	
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
	
	virtual int registerQuery(const char *querySpec, 
							  unsigned int querySpecLen, 
							  Interface::QueryOutput *output,
							  unsigned int &queryId) = 0;

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
	
	virtual int getQuerySchema (unsigned int queryId,
								char *schemaBuf,
								unsigned int schemaBufLen) = 0;	
	
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
	
	virtual int registerView(unsigned int queryId,
							 const char *tableInfo,
							 unsigned int tableInfoLen) = 0;
	
	/**
	 * End the specification of an application.  At this point the server
	 * can allocate resources (read create operators, queues etc) for the
	 * execution of the query.  
	 * 
	 * @return            0 (success), !0 (otherwise)
	 */	
	virtual int endAppSpecification() = 0;

	/**
	 * Get the plan for execution.  Should be called after endAppSpec()
	 * method is called
	 */
	virtual int getXMLPlan (char *planBuf, unsigned int planBufLen) = 0;
	
	/**
	 * Start the execution of the query: start reading the tuples from all
	 * the table sources and produce the output tuples.  In true spirit of
	 * continuous queries - this never returns :)
	 */
	virtual int beginExecution() = 0;

	/**
	 * Stop the execution of the query.  This is useful only in multi
	 * threaded execution (e.g., using net_server) in whcih case
	 * stopExecution is invoked by a thread different than the one that
	 * is doing the actual execution.  The implementation has to be
	 * thread safe.
	 */
	virtual int stopExecution () = 0;
	
	/**
	 *  Factory method to construct a new server.
	 */
	static Server *newServer(std::ostream& LOG);


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
								 unsigned int &monitorId) = 0;
#endif
	
};

#endif
