#ifndef _PLAN_MGR_IMPL_
#define _PLAN_MGR_IMPL_

/**
 * @file       plan_mgr_impl.h
 * @date       Aug. 21, 2004
 * @brief      
 */

#include <ostream>

#ifndef _PLAN_MGR_
#include "metadata/plan_mgr.h"
#endif

#ifndef _PHY_OP_
#include "metadata/phy_op.h"
#endif

#ifndef _PHY_SYN_
#include "metadata/phy_syn.h"
#endif

#ifndef _PHY_QUEUE_
#include "metadata/phy_queue.h"
#endif

#ifndef _PHY_STORE_
#include "metadata/phy_store.h"
#endif

#ifndef _TUPLE_
#include "execution/internals/tuple.h"
#endif

#ifndef _AEVAL_
#include "execution/internals/aeval.h"
#endif

#ifndef _BEVAL_
#include "execution/internals/beval.h"
#endif

#ifndef _INDEX_
#include "execution/indexes/index.h"
#endif

#ifndef _TUPLE_LAYOUT_
#include "metadata/tuple_layout.h"
#endif

#ifndef _MEMORY_MGR_
#include "execution/memory/memory_mgr.h"
#endif

#ifndef _STATIC_TUPLE_ALLOC_
#include "metadata/static_tuple_alloc.h"
#endif

#ifndef _QUEUE_
#include "execution/queues/queue.h"
#endif

using namespace Physical;

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
	
	class PlanManagerImpl : public PlanManager {
	private:
		
		/// System log
		std::ostream& LOG;
		
		/// System-wide table manager
		TableManager *tableMgr;		
		
		//----------------------------------------------------------------------
		// Constants affecting the system resources allocated for storing
		// various meta information.
		//----------------------------------------------------------------------
		
		/// Maximum number of physical operators permitted in the system
		static const unsigned int MAX_OPS = 2000;

		/// Maximum number of queues permitted in the system
		static const unsigned int MAX_QUEUES = 3000;
		
		/// Maximum number of synopses permitted in the system
		static const unsigned int MAX_SYNS = 2000;
		
		/// Maximum number of stores in the system
		static const unsigned int MAX_STORES = 1000;
		
		/// Maximum number of indexes in the system
		static const unsigned int MAX_INDEXES = 1000;
		
		/// Maximum number of query outputs from the system
		static const unsigned int MAX_OUTPUT = 50;		
		
		/// Maximum number of expression over all the queries
		static const unsigned int MAX_EXPRS = 1000;
		
		//----------------------------------------------------------------------
		// System Execution state: operators, queues, plan ...
		//----------------------------------------------------------------------
		
		/// Physical operators in the system.
		Operator ops [MAX_OPS];
		
		/// Pool of free operators (organized as a linked list)
		Operator *freeOps;
		
		/// Pool of active (used) operators organized as a linked list
		Operator *usedOps;
		
		/// Synopses for the operators
		Synopsis syns [MAX_SYNS];		
		unsigned int numSyns;
		
		/// Pool of queues
		Queue queues [MAX_QUEUES];
		unsigned int numQueues;
		
		/// Stores for the operators
		Store stores [MAX_STORES];
		unsigned int numStores;
		
		/// Indexes
		Execution::Index *indexes [MAX_INDEXES];
		unsigned int numIndexes;
		
		/// Expressions that occur within operators
		Expr exprs [MAX_EXPRS];
		unsigned int numExprs;
		
		/// Boolean expressions that occur within operators
		BExpr bexprs [MAX_EXPRS];
		unsigned int numBExprs;		
		
		/// System-wide memory manager
		Execution::MemoryManager *memMgr;
		
		//----------------------------------------------------------------------
		// System Input/Output
		//----------------------------------------------------------------------
		
		/// Number of base (input) stream / relations
		unsigned int numBaseTables;
		
		/// Source objects to get tuples from base tables.
		Interface::TableSource *baseTableSources [MAX_TABLES];		
		
		/// Number of output points in the system.
		unsigned int numOutputs;
		
		/// QueryOutput's are interfaces dsms uses to produce query results
		Interface::QueryOutput *queryOutputs [MAX_OUTPUT];
		
		//----------------------------------------------------------------------
		// "Naming" related:
		// 
		// The output of some subqueries form the input of other queries.
		// A tableId that occurs in a plan could refer to a base
		// stream/relation or to the result of another subquery.  We
		// handle these two cases uniformly by storing a "source" operator
		// for each tableId that produces the tuples of the
		// stream/relation corresponding to the table.
		//----------------------------------------------------------------------
		
		/// Number of base tables + intermediate (named) tables
		unsigned int numTables;
		
		/// Structure that stores tableId -> opId mapping
		struct {
			unsigned int tableId;
			unsigned int opId;
		} sourceOps [MAX_TABLES];
		
		/// Number of queries registered so far.
		unsigned int numQueries;
		
		/// For each query, we store the operator that produces the outptu
		/// of the query
		struct {
			unsigned int queryId;
			unsigned int opId;
		} queryOutOps [MAX_QUERIES];
		
	public:
		PlanManagerImpl(TableManager *tableMgr, std::ostream& LOG);
		virtual ~PlanManagerImpl();		
		
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
		
		int addBaseTable (unsigned int tableId,
						  Interface::TableSource *source);		
		
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
		
		int addLogicalQueryPlan (unsigned int queryId,
								 Logical::Operator *logPlan,
								 Interface::QueryOutput *output);

		/**
		 * Get the schema of a given queryId.  The plan for the query
		 * should have been registered previously.  The schema is encoded
		 * as an XML text in the buffer passed as a parameter.  The
		 * details of the encoding are given in
		 * Interface::Server::getQuerySchema()
		 */
		int getQuerySchema (unsigned int queryId,
							char *schemaBuf,
							unsigned int schemaBufLen);
		
		/**
		 * Indicate that the result of a previously registered query will
		 * be referenced in future queries using tableId.
		 * 
		 * @param  queryId       Identifier for a query
		 * @param  tableId       Identifier for the output of query queryId
		 * @return               0 (success), !0 (failure)
		 */ 
		
		int map(unsigned int queryId, unsigned int tableId);
		
		/**
		 * This  method  is  usually called   after  all  the  queries
		 * have  been registered.  It performs optimizations across
		 * subqueries.
		 */
		
		int optimize_plan ();

		/**
		 * This method is called after the optimize_plan() method.
		 * This adds all the auxiliary (non-operator) structures lik
		 * synopses, stores, queues to the plan.
		 */ 
		int add_aux_structures ();
		
		/**
		 * Get the xml representation of the plan - called after
		 * add_aux_structures ().  planBuf is the buffer in which
		 * the plan is encoded and is allocated/owned by the user.
		 */
		
		int getXMLPlan (char *planBuf, unsigned int planBufLen);
		
		/**
		 * Instantiate all the physical level entities in the current
		 * plan.
		 *
		 * Currently  once  this method  is  called,  we  cannot call  the
		 * "register"  methods  (addBaseTable,  addLogicalQueryPlan,  etc)
		 * though  it should  be straightforward  to code  a  more general
		 * version of this method.
		 */
		
		int instantiate ();
		

		int initScheduler (Execution::Scheduler *sched);
		
		
#ifdef _DM_
		/**
		 * (Debug routine) 
		 */
		void printPlan();
#endif

		int printStat ();

#ifdef _SYS_STR_
		
		/**
		 * Add a plan for monitoring a system property.
		 */
		
		int addMonitorPlan (unsigned int monitorId,
							Logical::Operator *logPlan,
							Interface::QueryOutput *output,
							Execution::Scheduler *sched);
#endif
		
	private:
		//---------------------------------------------------------------------
		// Operators management routines:
		//
		// Routines to keep track of the list of available operators
		// (ops), which operators are free, which are not,  allocate
		// operators during plan generation
		//---------------------------------------------------------------------
		
		/**
		 * Organize the set of all operators into a linked list of "free"
		 * operators.  this linked list is used 
		 */
		void init_ops ();
		
		/**
		 * Allocate a new Physical::Operator from the pool of available unused
		 * operators (ops).  Return 0 if we do not have a spare operator.
		 */
		Operator *new_op(OperatorKind kind);
		
		/**
		 * Free up one of the operators currently in use so that it can
		 * be used later.
		 */
		void free_op (Physical::Operator *op);
		
		//----------------------------------------------------------------------
		// Other resource management routines: expressions, boolean
		// expressions, strings, etc.
		//----------------------------------------------------------------------
		
		/**
		 * Allocate a new boolean expression from the pool of available
		 * boolean expressions (bexprs).  Return 0 if we do not have a
		 * spare one.
		 */
		BExpr *new_bexpr();

		/**
		 * Allocate a new arithmetic expression from the pool of available
		 * arithmetic expressions (exprs).  Return 0 if we do not have a
		 * spare one.
		 */		
		Expr *new_expr();
		
		/**
		 * Make a local copy of the string.  This is used to make local
		 * copy of string constants occurring in queries.  The memory for
		 * these constants was allocated within parser, which will be used
		 * to future queries ...
		 */ 
		
		char *copyString(const char *string);
		
		//----------------------------------------------------------------------
		
		/**
		 * Generate a physical plan from a logical plan.  This is called
		 * from within addLogicalQueryPlan		 
		 */ 
		int genPhysicalPlan(Logical::Operator *logPlan,
							Physical::Operator *&phyPlan);
		
		
		//----------------------------------------------------------------------
		//
		// Routines to construct various kinds of physical operators.
		// These routines are called from within genPhysicalPlan method.
		// All these routines take in two parameters:
		//
		// 1. A logical plan (logPlan)
		// 2. An array of physical plans (phyChildPlans)
		// 
		// The phyChildPlans[i] is the physical plan for the i-th subtree
		// below the root node of the logPlan.  The root node of the
		// logPlan is SELECT for mk_select () routine, PROJECT for
		// mk_project() routine, and so on.
		//
		// Each routine returns the physical plan for the entire logPlan -
		// it constructs the physical operator for the root and attaches
		// the phyChildPlans as child nodes of the root.
		//
		// All these routines are implemented in gen_phy_plan.cc
		//----------------------------------------------------------------------
		
		// PO_STREAM_SOURCE
		int mk_stream_source (Logical::Operator *logPlan,
							  Physical::Operator **phyChildPlans,
							  Physical::Operator *&phyPlan);
		
		// PO_RELN_SOURCE
		int mk_reln_source (Logical::Operator *logPlan,
							Physical::Operator **phyChildPlans,
							Physical::Operator *&phyPlan);
		
		// PO_ROW_WIN
		int mk_row_win (Logical::Operator *logPlan,
						Physical::Operator **phyChildPlans,
						Physical::Operator *&phyPlan);
		
		// PO_RANGE_WIN
		int mk_range_win (Logical::Operator *logPlan,
						  Physical::Operator **phyChildPlans,
						  Physical::Operator *&phyPlan);

		// PO_NOW_WIN
		int mk_now_win (Logical::Operator *logPlan,
						Physical::Operator **phyChildPlans,
						Physical::Operator *&phyPlan);

		// PO_PARTN_WIN
		int mk_partn_win (Logical::Operator *logPlan,
						  Physical::Operator **phyChildPlans,
						  Physical::Operator *&phyPlan);
		
		// PO_SELECT
		int mk_select (Logical::Operator *logPlan,
					   Physical::Operator **phyChildPlans,
					   Physical::Operator *&phyPlan);
		
		// PO_PROJECT
		int mk_project (Logical::Operator *logPlan,
						Physical::Operator **phyChildPlans,
						Physical::Operator *&phyPlan);

		// PO_JOIN
		int mk_join (Logical::Operator *logPlan,
					 Physical::Operator **phyChildPlans,
					 Physical::Operator *&phyPlan);
		
		// PO_STR_JOIN
		int mk_str_join (Logical::Operator *logPlan,
						 Physical::Operator **phyChildPlans,
						 Physical::Operator *&phyPlan);
		
		// PO_GROUP_AGGR
		int mk_group_aggr (Logical::Operator *logPlan,
						   Physical::Operator **phyChildPlans,
						   Physical::Operator *&phyPlan);

		// PO_DISTINCT
		int mk_distinct (Logical::Operator *logPlan,
						 Physical::Operator **phyChildPlans,
						 Physical::Operator *&phyPlan);

		// PO_ISTREAM
		int mk_istream (Logical::Operator *logPlan,
						Physical::Operator **phyChildPlans,
						Physical::Operator *&phyPlan);
		
		// PO_DSTREAM
		int mk_dstream (Logical::Operator *logPlan,
						Physical::Operator **phyChildPlans,
						Physical::Operator *&phyPlan);
		
		// PO_RSTREAM
		int mk_rstream (Logical::Operator *logPlan,
						Physical::Operator **phyChildPlans,
						Physical::Operator *&phyPlan);

		// PO_UNION
		int mk_union (Logical::Operator *logPlan,
					  Physical::Operator **phyChildPlans,
					  Physical::Operator *&phyPlan);
		
		// PO_EXCEPT
		int mk_except (Logical::Operator *logPlan,
					   Physical::Operator **phyChildPlans,
					   Physical::Operator *&phyPlan);
		
		// PO_QUERY_SOURCE
		int mk_qry_src (Physical::Operator *phyPlan,
						Physical::Operator *& qrySrc);
		
		// PO_OUTPUT
		int mk_output (Physical::Operator *phyPlan,
					   Physical::Operator *& output);

		// PO_SINK
		int mk_sink (Operator *op, Operator *&sink);
		
		//---------------------------------------------------------------------
		// Help routines used during the physical plan generation
		//---------------------------------------------------------------------
		
		/// Update child to reflect that parent reads from the child 
		int addOutput (Operator *child, Operator *parent);		
		
		//----------------------------------------------------------------------
		//
		// Routines to transform objects from Logical:: namespace to
		// Physical:: namespace.  These are used within the Physiclal
		// operator construction routines above.
		//
		// Implemented in gen_phy_plan.cc
		//----------------------------------------------------------------------
		
		/// Logical::Attr -> Physical::Attr
		Physical::Attr transformAttr (Logical::Attr, Logical::Operator *);
		
		/// Logical::BExpr -> Physical::BExpr
		Physical::BExpr *transformBExpr (Logical::BExpr, Logical::Operator *);
		
		/// Logical::Expr -> Physical::Expr
		Physical::Expr *transformExpr (Logical::Expr *, Logical::Operator *);
		
		//----------------------------------------------------------------------
		// Plan transformation routines:
		//
		// These routines are used to perform transformations over the
		// global plan (cutting across all the queries).  These are called
		// from within 
		//----------------------------------------------------------------------
		
		/**
		 * Remove all the query source operators from the overall query
		 * plan.  See comment within addLogicalQueryPlan() definition to
		 * understand the rationale for having query sources.  
		 */
		
		int removeQuerySources ();

		/**
		 * Add sink operators to drain away output from operators with
		 * no other operator reading their inputs
		 */
		int addSinks ();
		
		/**
		 * Wherever possible, merge two select operators that apply
		 * different predicates into one select operator that applies all
		 * the predicates of both the select operators.  Also a select
		 * above a join is merged into the join.
		 */ 
		int mergeSelects ();
		
		/**
		 * Whenever possible merge a project above a join into the join.
		 */
		int mergeProjectsToJoin ();
		
		/**
		 * Each group by aggr. function requires some "internal"
		 * aggregations either to compute other aggregation functions
		 * (e.g., avg requires count) or for its implementation.
		 */
		
		int addIntAggrs ();
		
		//----------------------------------------------------------------------
		// Routines to manage synopses:
		//----------------------------------------------------------------------
		
		/**
		 * Allocate a new synopsis from the pool of available synopses
		 */		
		Synopsis *new_syn (SynopsisKind kind);
		
		// Routines to add synopses to operators
		int add_syn_rel_source (Operator *op);
		int add_syn_proj (Operator *op);
		int add_syn_join (Operator *op);
		int add_syn_str_join (Operator *op);
		int add_syn_join_proj (Operator *op);
		int add_syn_str_join_proj (Operator *op);
		int add_syn_gby_aggr (Operator *op);
		int add_syn_distinct (Operator *op);
		int add_syn_row_win (Operator *op);
		int add_syn_range_win (Operator *op);
		int add_syn_now_win (Operator *op);
		int add_syn_partn_win (Operator *op);
		int add_syn_istream (Operator *op);
		int add_syn_dstream (Operator *op);
		int add_syn_rstream (Operator *op);
		int add_syn_union (Operator *op);
		int add_syn_except (Operator *op);
		
		/// add synopsis to one operator: a big switch statement that
		/// calls each of the above routines...
		int add_syn (Operator *op);
		
		/**
		 * Add synopses to all the operators in the plan
		 */		
		int add_syn ();

		//----------------------------------------------------------------------
		// Routines to manage queues
		//----------------------------------------------------------------------
		Queue *new_queue (QueueKind kind);
		
		//----------------------------------------------------------------------
		// Routines that help in synopsis sharing
		//----------------------------------------------------------------------
		
		/**
		 * Determine the kind of synopsis that an operator requires on one
		 * of its inputs.  This information is used to sharing state among
		 * different synopses.
		 *
		 * For example, a join requires a REL_SYN on both its inputs.  A
		 * stream join requires  REL_SYN on its outer but no synopsis on
		 * its inner.
		 *
		 * @param op        The operator for which we are doing this
		 *                  exercise
		 *
		 * @param inputIdx  The input of the operator in question
		 *
		 * @param synReq    Output: true if a synopsis is required
		 *                  by the operator on this input
		 *
		 * @param synKind   The kind of synopsis that the operator
		 *                  requires.
		 *
		 * @return          0 (success), !0 (failure)
		 */ 
		int getSharedSynType (Operator *op, unsigned int inputIdx,
							  bool &synReq, SynopsisKind &synKind);
		
		/**
		 * Set the store for the shared synopses. [[ Explanation ]]
		 */ 
		int setSharedStore (Operator *op, unsigned int inputIdx,
							Store *store);
		
		//----------------------------------------------------------------------
		// Routines that add "stores" which are entities that allocate
		// space for tuples.
		//----------------------------------------------------------------------
		
		Store *new_store(StoreKind kind);
		
		int add_store ();
		int add_store (Operator *op);
		int add_store_select (Operator *op);

		// Set the input stores for operators
		int set_in_stores ();
		
		// defined in gen_phy_plan.cc
		int mk_dummy_project (Operator *child, Operator *&project);
		
		//----------------------------------------------------------------------
		// Instantiation routines
		//----------------------------------------------------------------------
		
		struct EvalContextInfo {
			TupleLayout *st_layout;
			ConstTupleLayout *ct_layout;
		};
		
		static const unsigned int SCRATCH_ROLE = 0;
		static const unsigned int CONST_ROLE = 1;
		
		int inst_expr (Physical::Expr *expr,
					   unsigned int *roleMap,
					   Physical::Operator *op,					   
					   Execution::AEval *&eval,
					   unsigned int& role,
					   unsigned int& col,
					   EvalContextInfo &evalCxt);
		
		int inst_expr_dest (Physical::Expr *expr,
							unsigned int *roleMap,
							Physical::Operator *op,
							unsigned int role,
							unsigned int col,							
							Execution::AEval *&eval,
							EvalContextInfo &evalCxt);
		
		int inst_bexpr (Physical::BExpr *expr,
						unsigned int *roleMap,
						Physical::Operator *op,
						Execution::BEval *&beval,
						EvalContextInfo &evalCxt);
		
		//------------------------------------------------------------
		// Operators
		//------------------------------------------------------------
		int inst_op (Physical::Operator *op);
		int inst_ops ();
		int inst_select (Physical::Operator *op);		
		int inst_project (Physical::Operator *op);
		int inst_join (Physical::Operator *op);
		int inst_str_join (Physical::Operator *op);
		int inst_aggr (Physical::Operator *op);
		int inst_distinct (Physical::Operator *op);
		int inst_row_win (Physical::Operator *op);
		int inst_range_win (Physical::Operator *op);
		int inst_now_win (Physical::Operator *op);
		int inst_pwin (Physical::Operator *op);
		int inst_istream (Physical::Operator *op);
		int inst_dstream (Physical::Operator *op);
		int inst_rstream (Physical::Operator *op);
		int inst_str_source (Physical::Operator *op);
		int inst_rel_source (Physical::Operator *op);
		int inst_output (Physical::Operator *op);
		int inst_union (Physical::Operator *op);
		int inst_except (Physical::Operator *op);
		int inst_sink (Physical::Operator *op);
		
		//------------------------------------------------------------
		// Stores
		//------------------------------------------------------------
		int inst_simple_store (Physical::Store *store,
							   TupleLayout *dataLayout); 
		
		int inst_lin_store (Physical::Store *store,
							TupleLayout *dataLayout);
		
		int inst_rel_store (Physical::Store *store,
							TupleLayout *dataLayout);
		
		int inst_win_store (Physical::Store *store,
							TupleLayout *dataLayout);
		
		int inst_pwin_store (Physical::Store *store);		
		
		//----------------------------------------------------------------------
		// Queues
		//----------------------------------------------------------------------
		int inst_writer_queue (Queue *queue);
		int inst_reader_queue (Queue *queue);
		int inst_simple_queue (Queue *queue);
		int inst_queues ();
		int link_reader_writer (Queue *reader);
		
		//-----------------------------------------------------------
		// Memory manager
		//------------------------------------------------------------
		int inst_mem_mgr ();
		
		/// Allocator for static tuples space.  Static tuples contain the
		/// constants that are used in operator computations
		StaticTupleAlloc *staticTupleAlloc;
		
		int inst_static_tuple_alloc ();
		
		int getStaticTuple (Execution::Tuple &tuple,
							unsigned int tupleSize);
		
		int add_queues ();
		
		// Link the synopses to their stores [[ Explanation ]]
		int link_syn_store ();
		
		// Link the ops & their input stores
		int link_in_stores ();

		//------------------------------------------------------------
		// Monitor related routines
		//------------------------------------------------------------
		
		void createSSGen ();
		
		/**
		 * Convert the logical plan for the monitor query to a physical
		 * plan representation.  We do not do any fancy optimizations -
		 * just a one-one conversion from a logical operator to a physical
		 * operator.
		 */
		int genPhysicalPlan_mon (unsigned int monitorId,
								 Logical::Operator *logPlan,
								 Interface::QueryOutput *output,
								 Physical::Operator *&phyPlan);

		int optimize_plan_mon (Physical::Operator *&plan,
							   Physical::Operator **opList,
							   unsigned int &numOps);

		int add_aux_structures_mon (Physical::Operator *&plan,
									Physical::Operator **opList,
									unsigned int &numOps);

		int instantiate_mon (Physical::Operator *&plan,
							 Physical::Operator **opList,
							 unsigned int &numOps);
		
		int mergeSelects_mon (Physical::Operator *&plan,
							  Physical::Operator **opList,
							  unsigned int &numOps);
		
		int mergeProjectsToJoin_mon (Physical::Operator *&plan,
									 Physical::Operator **opList,
									 unsigned int &numOps);

		int addIntAggrs_mon (Physical::Operator *&plan,
							 Physical::Operator **opList,
							 unsigned int &numOps);

		int add_queues_mon (Physical::Operator *&plan,
							Physical::Operator **opList,
							unsigned int &numOps);

		int add_syn_mon (Physical::Operator *&plan,
						 Physical::Operator **opList,
						 unsigned int &numOps);

		int add_store_mon (Physical::Operator *&plan,
						   Physical::Operator **opList,
						   unsigned int &numOps);

		int set_in_stores_mon (Physical::Operator *&plan,
							   Physical::Operator **opList,
							   unsigned int &numOps);
		
		int inst_ops_mon (Physical::Operator *&plan,
						  Physical::Operator **opList,
						  unsigned int &numOps);
		
		int link_syn_store_mon (Physical::Store **storeList,
								unsigned int numStores);

		int link_in_stores_mon (Physical::Operator **opList,
								unsigned int numOps);
		
		int inst_ss_gen (Physical::Operator *op);
		int inst_ss_gen_store (Physical::Operator *op);
		int update_ss_gen (Physical::Operator *p_op,
						   Physical::Operator **opList, unsigned int,
						   Physical::Queue **queueList, unsigned int,
						   Physical::Store **storeList, unsigned int,
						   Physical::Synopsis **synList, unsigned int);
	};
}

#endif
