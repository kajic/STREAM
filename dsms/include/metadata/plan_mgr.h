#ifndef _PLAN_MGR_
#define _PLAN_MGR_

/**
 * @file       plan_mgr.h
 * @date       Aug. 21, 2004
 * @brief      
 */

#include <ostream>

#ifndef _LOGOP_
#include "querygen/logop.h"
#endif

#ifndef _OPERATOR_
#include "execution/operators/operator.h"
#endif

#ifndef _QUERY_OUTPUT_
#include "interface/query_output.h"
#endif

#ifndef _TABLE_SOURCE_
#include "interface/table_source.h"
#endif

#ifndef _TABLE_MGR_
#include "metadata/table_mgr.h"
#endif

#ifndef _SCHEDULER_
#include "execution/scheduler/scheduler.h"
#endif

namespace Metadata {

	/**
	 * Plan manager  manages the physical  level entities in the  system -
	 * the operators, the queues, synopses, the query plans, etc.
	 *	 
	 * Once   the  query  plans   are  finalized   it  also   handles  the
	 * instantiation  of  physical  entities  (creating  the  objects  for
	 * operators, queues etc.)
	 *
	 * Note that by query plan, we mean the global plan involving all
	 * subqueries with one or more inputs and one or more outputs.
	 */
	
	class PlanManager {
	public:
		virtual ~PlanManager () {}
		
		/**
		 * Indicate to  the plan manager that  the table (stream/relation)
		 * represented by  tableId is  a base input  (stream/relation).  A
		 * table could also be  a intermediate stream/relation produced by
		 * a query,  and this is indicated  to the plan  manager using the
		 * map method.
		 *
		 * @param     tableId     system-wide id for the stream/relation
		 *
		 * @param     source      source object for the stream/relation
		 *
		 * @return                0 (success), !0 (failure)		 
		 */ 
		
		virtual int addBaseTable (unsigned int tableId,
								  Interface::TableSource *source) = 0;		
		
		/**
		 * Register a query with a  logical query plan.  The logical query
		 * plan is converted  to a physical query plan  within this method
		 * and stored by the plan  manager.  Every table referenced in the
		 * logical query plan should  have been "registered" with the plan
		 * manager earlier  using addSource()  (for base table)  method or
		 * map method (for intermediate tables).
		 * 
		 * @param   queryId      Identifier for the (sub)query which is
		 *                       registered.  Assigned by QueryManager
		 *
		 * @param   logPlan      logical query plan for the subquery.
		 *
		 * @return               0 (success), !0 (failure)
		 */ 
		
		virtual int addLogicalQueryPlan(unsigned int queryId,
										Logical::Operator *logPlan,
										Interface::QueryOutput *output) = 0;

		/**
		 * Get the schema of a given queryId.  The plan for the query
		 * should have been registered previously.  The schema is encoded
		 * as an XML text in the buffer passed as a parameter.  The
		 * details of the encoding are given in
		 * Interface::Server::getQuerySchema()
		 */
		
		virtual int getQuerySchema (unsigned int queryId,
									char *schemaBuf,
									unsigned int schemaBufLen) = 0;
		
		/**
		 * Indicate that the result of a previously registered query will
		 * be referenced in future queries using tableId.
		 * 
		 * @param  queryId       Identifier for a query
		 * @param  tableId       Identifier for the output of query queryId
		 * @return               0 (success), !0 (failure)
		 */ 
		
		virtual int map(unsigned int queryId, unsigned int tableId) = 0;
		
		/**
		 * This  method  is  usually called   after  all  the  queries
		 * have  been registered.  It performs optimizations across
		 * subqueries.
		 */
		
		virtual int optimize_plan () = 0;

		/**
		 * This method is called after the optimize_plan() method.
		 * This adds all the auxiliary (non-operator) structures lik
		 * synopses, stores, queues to the plan.
		 */ 
		virtual int add_aux_structures () = 0;
		
		/**
		 * Get the xml representation of the plan - called after
		 * add_aux_structures ().  planBuf is the buffer in which
		 * the plan is encoded and is allocated/owned by the user.
		 */
		
		virtual int getXMLPlan (char *planBuf, unsigned int planBufLen) = 0;
		
		/**
		 * Instantiate all the physical level entities in the current
		 * plan.
		 *
		 * Currently  once  this method  is  called,  we  cannot call  the
		 * "register"  methods  (addBaseTable,  addLogicalQueryPlan,  etc)
		 * though  it should  be straightforward  to code  a  more general
		 * version of this method.
		 */
		
		virtual int instantiate () = 0;
		
		/**
		 * Initialize the schedulerr with the instantiated operators. 
		 */
		
		virtual int initScheduler (Execution::Scheduler *sched) = 0;
		
		/**
		 * Factory method to construct a new plan manager. 
		 */		
		static PlanManager *newPlanManager(TableManager *tableMgr,
										   std::ostream& LOG);
		
#ifdef _DM_
		/**
		 * (Debug routine) 
		 */
		virtual void printPlan() = 0;		
#endif

		/// debug
		virtual int printStat () = 0;

#ifdef _SYS_STR_
		
		/**
		 * Add a plan for monitoring some property of the system.
		 * Unlike a logical qeuery plan which has to be added before
		 * the optimize() and instantiate() methods have been called, this
		 * method is called at "run-time" - after the operators have been
		 * instantiated (and possibly running).
		 *
		 * The only input of the logical plan is guaranteed to be the
		 * System property stream.
		 *
		 * This method generates the physical plan, instantiates the new
		 * operators of the physical plan, and updates the scheduler
		 * with the new operator.
		 */
		
		virtual int addMonitorPlan (unsigned int monitorId,
									Logical::Operator *logPlan,
									Interface::QueryOutput *output,
									Execution::Scheduler *sched) = 0;
#endif
		
	};
}

#endif
