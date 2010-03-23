#ifndef _LOG_PLAN_GEN_
#define _LOG_PLAN_GEN_

#ifndef _TABLE_MGR_
#include "metadata/table_mgr.h"
#endif

#ifndef _QUERY_
#include "querygen/query.h"
#endif

#ifndef _LOGOP_
#include "querygen/logop.h"
#endif

/**
 * @file      log_plan_gen.h
 * @date      May 25, 2004
 * @brief     Generate logical query plan from a declarative
 *            (internal) representation of a CQL query.
 */

class LogPlanGen {
 private:
	Metadata::TableManager *_tableMgr;
	
 public:
	
	/**
	 * @param    tableMgr   System-wide table manager.
	 */	
	LogPlanGen (Metadata::TableManager *tableMgr);
	
	/**
	 * @param  query   (Input) parsed, internal representation of CQL
	 *                  query
	 */	
	int genLogPlan (const Semantic::Query &query,
					Logical::Operator *& queryPlan);
	
 private:
	
    /**
	 * Generate a naive query plan from a query:
	 * 
	 * @param    query       Input query
	 * @param    plan        Output plan
	 * 
	 * @return               [[ usual convention ]]
	 */ 
	int genPlan_n (const Semantic::Query &query, 
				   Logical::Operator *&plan); 
	
	//----------------------------------------------------------------------
	//
	// Bunch of transformation rules for query plans: all prefixed with "t_"
	//
	//----------------------------------------------------------------------
	
	/**
	 * Transform: (Stream -> [Now] -> filter -> project -> rstream) to the form
	 *            (Stream -> filter -> project)
	 */ 
	
	int t_rstreamNow (Logical::Operator *&plan);
	
	/**
	 * Remove redundant projects: (useful in Select * queries)
	 */
	int t_removeProject (Logical::Operator *&plan);
	
	/**
	 * Transform: (Stream -> (implicit unbounded) -> filter -> istream) to
	 *            (Stream -> filter )
	 */
	int t_removeIstream (Logical::Operator *&plan);
	
	/**
	 * Identify and use "Stream Cross Product" operator instead of the
	 * normal cross product: 
	 *
	 * Transform:
	 *  
	 *     Rstream ( Cross Product (S [Now], <rels> )) to
	 *  
	 *     (Stream Cross Product (S, <rels> ))
	 *   
	 *  A stream cross product operation probes the relations for every
	 *  stream tuple of S and produces a concated stream tuple.  [[
	 *  More Explanation ]]
	 */ 
	int t_streamCross (Logical::Operator *&plan);
	
	/**
	 * Transform: Identify stream joins: Stream join is a special join
	 * which has a stream one its "outer" and a stream or relation on its
	 * "inner"  [[ more explanation ]]
	 *
	 * Rstream (Stream [Now] Join Rel) -> (Stream StreamJoin Rel)
	 */
	int t_useStreamJoins (Logical::Operator *&plan);
	
	/**
	 * Trasform: replace a multiway cross with a sequence of binary
	 * crosses (Useful to identify binary joins later)
	 */ 
	int t_makeCrossBinary (Logical::Operator *&plan);

	/**
	 * ditto as above for stream crosses
	 */
	int t_makeStreamCrossBinary (Logical::Operator *&plan);	
	
	/**
	 * Transform: push down selects below CROSS operator
	 */
	int t_pushSelect (Logical::Operator *&plan);
};

#endif
