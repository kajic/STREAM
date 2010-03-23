/**
 * @file     log_naive_plan_gen.cc
 * @date     May 25, 2004
 * @brief    Generate naive logical query plan from a declarative query 
 *           representation.
 */

/// debug
#include <iostream>
using namespace std;
#include "querygen/logop_debug.h"

#ifndef _LOG_PLAN_GEN_
#include "querygen/log_plan_gen.h"
#endif

#ifndef _DEBUG_
#include "common/debug.h"
#endif

using namespace Logical;

static const Semantic::Query  *l_Query;
static Metadata::TableManager *l_tableMgr;

//----------------------------------------------------------------------
// Misc. help routines
//----------------------------------------------------------------------
static bool operator== (const Attr& attr1, const Attr& attr2);

//----------------------------------------------------------------------
// Functions to Transform from Semantic::<> to Logical::<>
//
// [[ Explanation ]]
// ----------------------------------------------------------------------

static Logical::Attr transformAttr(Semantic::Attr semAttr);
static Logical::Attr transformAggrAttr(const Semantic::Expr *expr);
static Logical::Expr *transformConstVal(const Semantic::Expr *constVal);
static Logical::Expr *transformExpr(const Semantic::Expr *semExpr);
static Logical::BExpr transformBExpr(Semantic::BExpr semBExpr);

//----------------------------------------------------------------------
// Functions used to generate a naive query plan from a declarative CQL
// query (internal representation of).  The suffix "_n" stands for
// "naive". 
//----------------------------------------------------------------------

/**
 * Generate a naive plan for a select from where query
 *
 * @param   query         Input query
 * @param   plan          Output plan for the query
 *
 * @return                [[ Usual convention ]]
 */
static int genSFWPlan_n (const Semantic::Query &query,
						 Operator *&plan);

/**
 * Generate a naive plan for a binary operator query
 *
 * @param   query         Input query
 * @param   plan          Output plan for the query
 *
 * @return                [[ Usual convention ]]
 */
static int genBinOpPlan_n (const Semantic::Query &query,
						   Operator *&plan);

/**
 * Generate a naive plan that joins all the tables in the from clause.
 * Some tables can be streams with windows: it generates the window
 * operators as required.
 *
 * @param    query        Input query
 * @param    outputPlan   Output plan that joins the tables
 *
 * @return                [[ usual convention ]]
 */ 
static int joinTables_n (const Semantic::Query &query, 
						 Operator *&outputPlan);

/**
 * Apply predicates that are present in the WHERE clause of a query to an
 * input subplan.
 *
 * @param   query         Query for which we are generating plan
 * @param   inputPlan     Partial query plan (input)
 * @param   outputPlan    Query plan which applies WHERE clause predicates
 *
 * @return                [[ usual convention ]]
 */ 

static int applyPreds_n (const Semantic::Query &query,
						 Operator *inputPlan, 
						 Operator *&outputPlan);

/**
 * Apply an aggregation operator if necessary.
 * 
 * @param  query          Query for which we are generating plan
 * @param  inputPlan      Partial query plan (input)
 * @param  outputPlan     Query plan with aggregation operator if needed.
 *
 * @return                [[ usual convention ]]
 */  

static int applyAggr_n (const Semantic::Query &query,
						Operator *inputPlan,
						Operator *&outputPlan);

/**
 * Perform the projections required by the query:
 *
 * @param  query          Query for which we are generating plan
 * @param  inputPlan      Partial query plan (input)
 * @param  outputPlan     Query plan with the projections in SELECT clause
 *
 * @return                [[ usual convention ]]
 */ 

static int doProjs_n (const Semantic::Query &query,
					  Operator *inputPlan,
					  Operator *&outputPlan);

/**
 * Apply distinct operator: remove duplicates
 *
 * @param query           Query for which we are generating plan
 * @param inputPlan       Partial query plan (input)
 * @param outputPlan      Query plan that applies distinct
 * 
 * @return                [[ usual convention ]]
 */
static int applyDistinct_n (const Semantic::Query &query,
							Operator *inputPlan,
							Operator *&outputPlan);

/**
 * Apply a R2S operator if required by the query:
 *
 * @param query          Query for which we are generating plan
 * @param inputPlan      Partial uqery plan (input)
 * @param outputPlan     Query plan with the R2S operator
 *
 * @return               [[ usual convention ]]
 */

static int applyR2SOps_n (const Semantic::Query &query,
						  Operator *inputPlan,
						  Operator *&outputPlan);

/**
 * Apply a window operation over a specified stream
 *
 * @param win             Window Specification
 * @param inputStr        Plan producing a stream
 * @param outputPlan      Plan that applies window operator over the
 *                        stream 
 *
 * @return                [[ usual convention ]]
 */ 

static int applyWindow_n (Semantic::WindowSpec win,						  
						  Operator *inputStr,
						  Operator *&outputPlan);

/**
 * Get a source operator for a table occuring in a query.
 *
 * @param  varId          Variable Id of the table
 * @param  source         Output source operator.
 * 
 * @return                [[ usual convention ]]
 */
static int getSource_n (unsigned int varId, 
						unsigned int tableId,
						Operator *&source);


LogPlanGen::LogPlanGen (Metadata::TableManager *tableMgr)
{
	this -> _tableMgr = tableMgr;
}

/**
 * Generate naive query plan:
 */
int LogPlanGen::genPlan_n (const Semantic::Query &query, Operator *&plan)
{
	int rc;
	
	// Initialize global variables
	l_Query = &query;
	l_tableMgr = _tableMgr;	
	set_table_mgr (_tableMgr);
	
	reset_op_pool();
	reset_expr_pool();
	
	if (query.kind == Semantic::SFW_QUERY) {
		if ((rc = genSFWPlan_n (query, plan)) != 0)
			return rc;
	}
	
	else {
		if ((rc = genBinOpPlan_n (query, plan)) != 0)
			return rc;
	}
	
	return 0;
}

static int genBinOpPlan_n (const Semantic::Query &query, Operator *&plan)
{
	int rc;
	Operator     *leftSource;
	Operator     *rightSource;
	Operator     *binOp;
	Operator     *r2s;
	
	// Source operator for the left input
	if ((rc = getSource_n (0, query.refTables [0], leftSource)) != 0)
		return rc;
	
	// Source operator for the right input
	if ((rc = getSource_n (0, query.refTables [1], rightSource)) != 0)
		return rc;
	
	// Binary operator
	if (query.binOp == Semantic::UNION) {
		binOp = mk_union (leftSource, rightSource);		
	}
	
	else {
		binOp = mk_except (leftSource, rightSource);
	}
	
	// Optional r2s operator
	if ((rc = applyR2SOps_n (query, binOp, r2s)) != 0)
		return rc;
	
	// The final plan
	plan = r2s;
	
	return 0;
}

static int genSFWPlan_n (const Semantic::Query &query, Operator *&plan)
{
	Operator   *join;
	Operator   *select;
	Operator   *aggr;
	Operator   *project;
	Operator   *distinct;
	Operator   *r2s;
	int rc;
	
	
	// Generate a plan that joins the FROM clause tables
	if ((rc = joinTables_n (query, join)) != 0)
		return rc;
	
	// Apply WHERE clause predicates over the join
	if ((rc = applyPreds_n (query, join, select)) != 0)
		return rc;
	
	// Apply Aggregations & perform group by if necessary
	if ((rc = applyAggr_n (query, select, aggr)) != 0)
		return rc;
	
	// Perform projections specified in SELECT clause
	if ((rc = doProjs_n (query, aggr, project)) != 0)
		return rc;
	
	// Apply Distinct operator if needed
	if ((rc = applyDistinct_n (query, project, distinct)) != 0)
		return rc;
	
	// Apply R2S operators if present ...
	if ((rc = applyR2SOps_n (query, distinct, r2s)) != 0)
		return rc;
	
	// ... which is the final plan
	plan = r2s;
	
	return 0;
}

/**
 * Produce a plan that joins all the FROM clause tables.  If there is just
 * one table, no join is necessary.
 */ 
static int joinTables_n (const Semantic::Query &query, 
						 Operator *&outputPlan) 
{
	unsigned int  numFromClauseTables;	
	Operator     *cross;
	Operator     *tableSource;
	Operator     *win;	
	Operator     *tablePlan;
	unsigned int  varId;
	unsigned int  tableId;
	int rc;
	
	numFromClauseTables = query.numFromClauseTables;
	
	ASSERT (numFromClauseTables > 0);
	
	// More than one table: join needed
	if (numFromClauseTables > 1) {
		
		// n-way cross product
		cross = mk_cross ();
		if (!cross) return -1;
		
		// For each table ...
		for (unsigned int t = 0 ; t < numFromClauseTables ; t++) {
			
			varId = query.fromClauseTables [t];
			tableId = query.refTables[varId];
			
			// ... get the source operator.
			if ((rc = getSource_n (varId, tableId, tableSource)) != 0)
				return rc;
			
			// This table is a stream: apply the specified window
			if (l_tableMgr -> isStream (tableId)) {				
				if ((rc = applyWindow_n (query.winSpec [t],
										 tableSource, win)) != 0)
					return rc;
				tablePlan = win;
			}
			
			// Table is a relation:
			else {
				tablePlan = tableSource;
			}
			
			// Input the plan for the table to the cross operator
			cross = cross_add_input (cross, tablePlan);
			if (!cross) return -1;
		}
		
		outputPlan = cross;		
	}
	
	// Just one table: no join needed
	else {
		varId = query.fromClauseTables [0];
		tableId = query.refTables[varId];
			
		// Get the source operator for the table
		if ((rc = getSource_n (varId, tableId, tableSource)) != 0)
			return rc;
		
		// The table is a stream: apply the specified window
		if (l_tableMgr -> isStream (tableId)) {				
			if ((rc = applyWindow_n (query.winSpec [0],
									 tableSource, win)) != 0)
				return rc;
			tablePlan = win;
		}
		
		// Table is a relation:
		else {
			tablePlan = tableSource;
		}
		
		outputPlan = tablePlan;
	}
	
	return 0;
}

/**
 * Apply WHERE clause predicates to a partial query plan
 */
static int applyPreds_n (const Semantic::Query &query,
						 Operator *inputPlan, 
						 Operator *&outputPlan) 
{	
	Logical::BExpr bExpr;
	outputPlan = inputPlan;
	
	// Include one select operator per predicate
	for (unsigned int p = 0 ; p < query.numPreds ; p++) {
		bExpr = transformBExpr(query.preds [p]);
		outputPlan = mk_select (outputPlan, bExpr);
		
		if(!outputPlan)
			return -1;
	}
	
	return 0;
}

/**
 * Collect the "aggregations" (e.g. MAX(S.A)) that occur in an expression
 * and add them to an input list of aggregations, if they do not already
 * occur in the list.  (note: input list could be nonempty)
 *
 * @param  expr       Expression from which we want to collect aggregations
 * @param  aggrAttrs  Aggregated attributes (list of) [input/ouptut]
 * @param  aggrFns    Aggregation function  (list of) [input/output]
 * @param  numAggrs   Number of aggregations in the list (aggrAttrs, aggrFns)
 * @param  maxAttrs   Maximum number of attributes (size of list)
 * 
 * @return            [[ explanation ]]
 */
static int collectAggrs (const Semantic::Expr  *expr,
						 Logical::Attr         *aggrAttrs,
						 AggrFn                *aggrFns,
						 unsigned int          &numAggrs,
						 unsigned int           maxAttrs)						  
{
	int rc;
	Attr   newAggrAttr;
	AggrFn newAggrfn;	
	bool   bNew;
	
	switch (expr -> exprType) {
		
	case Semantic::E_CONST_VAL:
		break;
		
	case Semantic::E_ATTR_REF:  
		break;
		
	case Semantic::E_AGGR_EXPR:
		
		// An aggregation 
		newAggrAttr = transformAttr (expr -> u.AGGR_EXPR.attr);
		newAggrfn   = expr -> u.AGGR_EXPR.fn;
		
		// Is this aggregation already in the (input) list?
		bNew = true;
		for (unsigned int a = 0 ; a < numAggrs && bNew ; a++) 
			if ((aggrAttrs [a] == newAggrAttr) && (aggrFns [a] == newAggrfn))
				bNew = false;
		
		// New aggregation: add it to the input list
		if(bNew) {
			// Do we have space in the list?
			if (numAggrs == maxAttrs)
				return -1;
			
			aggrAttrs [ numAggrs ] = newAggrAttr;
			aggrFns   [ numAggrs ] = newAggrfn;
			numAggrs ++;
		}
		
		break;
		
	case Semantic::E_COMP_EXPR:
		
		// Recursively collect aggregations from the left & right
		// expressions: 
		
		if ((rc = collectAggrs (expr -> u.COMP_EXPR.left,
								aggrAttrs, aggrFns, 
								numAggrs, maxAttrs)) != 0)
			return rc;
		
		if ((rc = collectAggrs (expr -> u.COMP_EXPR.right,
								aggrAttrs, aggrFns, 
								numAggrs, maxAttrs)) != 0)
			return rc;
		
		break;
		
	default:
		
		return -1;
	}
	
	return 0;
}

/**
 * Apply an aggregation operator if necessary to a partial query plan.
 */  

static int applyAggr_n (const Semantic::Query &query,
						Operator *inputPlan,
						Operator *&outputPlan) 
{
	int rc;
	unsigned int  numGroupAttrs;
	unsigned int  numProjExprs;	
	Logical::Attr aggrAttrs [MAX_AGGR_ATTRS];
	AggrFn        aggrFns [MAX_AGGR_ATTRS];
	unsigned int  numAggrs;	
	Operator     *aggrOp;
	Attr          groupAttr;
	
	numGroupAttrs = query.numGbyAttrs;	
	numProjExprs = query.numProjExprs;
	
	// Compute the aggregations occurring the SELECT clause: We need to
	// this information to initialize the aggregation operator.
	numAggrs = 0;
	for (unsigned int e = 0 ; e < numProjExprs ; e++) {
		if((rc = collectAggrs (query.projExprs [e], aggrAttrs, aggrFns,
							   numAggrs, MAX_AGGR_ATTRS)) != 0) {
			return rc;
		}
	}
		
	// We do not need an aggregation operator if the group by clause is
	// empty and there are no aggregations in the SELECT clause
	if (numAggrs == 0 && numGroupAttrs == 0) {
		outputPlan = inputPlan;		
		return 0;
	}
	
	// Create the aggregation operator:
	aggrOp = mk_group_aggr (inputPlan);
	if (!aggrOp)
		return -1;
	
	// specify the grouping attributes to the operator:
	for (unsigned int a = 0 ; a < numGroupAttrs ; a++) {
		groupAttr = transformAttr(query.gbyAttrs [a]);		
		aggrOp = add_group_attr (aggrOp, groupAttr);
	}
	
	// Specify the aggregations to the operator:
	for (unsigned int a = 0 ; a < numAggrs ; a++) {
		aggrOp = add_aggr (aggrOp, aggrFns [a], aggrAttrs [a]);
	}
		
	outputPlan = aggrOp;
	
	return 0;
}

/**
 * Perform the projections required by the query:
 *
 * @param  query          Query for which we are generating plan
 * @param  inputPlan      Partial query plan (input)
 * @param  outputPlan     Query plan with the projections in SELECT clause
 *
 * @return                [[ usual convention ]]
 */ 

static int doProjs_n (const Semantic::Query &query,
					  Operator *inputPlan,
					  Operator *&outputPlan) {
	Operator      *project;
	unsigned int   numProjExprs;
	Logical::Expr *projExpr;
	
	project = mk_project (inputPlan);
	if (!project)
		return -1;
	
	numProjExprs = query.numProjExprs;		
	for (unsigned int e = 0 ; e < numProjExprs ; e++) {
		projExpr = transformExpr (query.projExprs [e]);
		project = add_project_expr (project, projExpr);
	}
	
	outputPlan = project;
	
	return 0;
}

/**
 * Apply distinct operator if query requires it.
 */
static int applyDistinct_n (const Semantic::Query &query,
							Operator *inputPlan,
							Operator *&outputPlan) 
{
	if (query.bDistinct)
		outputPlan = mk_distinct (inputPlan);
	else 
		outputPlan = inputPlan;
	
	return (outputPlan)? 0 : -1;
}


/**
 * Apply a R2S operator if required by the query:
 */

static int applyR2SOps_n (const Semantic::Query &query,
						  Operator *inputPlan,
						  Operator *&outputPlan) 
{
	// Query does not have an R2S operator
	if (!query.bRelToStr) {
		outputPlan = inputPlan;
		return 0;
	}
	
	switch (query.r2sOp) {
	case Semantic::ISTREAM:
		outputPlan = mk_istream(inputPlan);
		break;

	case Semantic::DSTREAM:
		outputPlan = mk_dstream(inputPlan);
		break;
		
	case Semantic::RSTREAM:
		outputPlan = mk_rstream(inputPlan);
		break;
		
	default:
		return -1;
	}
	
	return (outputPlan)? 0 : -1;
}


/**
 * Apply a window operation over a specified stream
 */ 

static int applyWindow_n (Semantic::WindowSpec win,						  
						  Operator *inputStr,
						  Operator *&outputPlan) 
{
	unsigned int   numPartnAttrs;
	Logical::Attr  partnAttr;
	Operator      *winOp;
	
	switch (win.type) {
		
	case RANGE:
		
		winOp = mk_range_window (inputStr, win.u.RANGE.timeUnits, 
          win.u.RANGE.strideUnits);
		break;
		
	case ROW:
		
		winOp = mk_row_window (inputStr, win.u.ROW.numRows);
		break;
		
	case PARTITION:
		
		winOp = mk_partn_window (inputStr, win.u.PARTITION.numRows);		
		numPartnAttrs = win.u.PARTITION.numPartnAttrs;
		
		for (unsigned int a = 0 ; a < numPartnAttrs ; a++) {
			partnAttr = transformAttr(win.u.PARTITION.attrs [a]);
			partn_window_add_attr (winOp, partnAttr);
		}
		
		break;		
		
	case NOW:
		
		winOp = mk_now_window (inputStr);
		break;
		
	case UNBOUNDED:
		// Unbounded window is a no-op [[ Explain ]]
		
		winOp = inputStr;
		break;
		
	default:
		
		return -1;		
	}
	
	outputPlan = winOp;
	
	return 0;
}

/**
 * Get a source operator for a table occuring in a query.
 */
static int getSource_n (unsigned int varId, 
						unsigned int tableId,
						Operator *&source)
{
	if (l_tableMgr -> isStream (tableId)) 
		source = mk_stream_source (varId, tableId);
	else
		source = mk_reln_source (varId, tableId);
	
	return (source)? 0 : -1;
}

static Logical::Attr transformAttr (Semantic::Attr semAttr)
{	
	Logical::Attr logAttr;
	
	logAttr.kind             = NAMED;
	logAttr.u.NAMED.varId    = semAttr.varId;
	logAttr.u.NAMED.tableId  = l_Query -> refTables [ semAttr.varId ];
	logAttr.u.NAMED.attrId   = semAttr.attrId;
	
	return logAttr;
}

static Logical::Attr transformAggrAttr (const Semantic::Expr *aggrExpr) 
{
	ASSERT(aggrExpr -> exprType == Semantic::E_AGGR_EXPR);
	
	Logical::Attr  logAttr;
	Semantic::Attr semAttr;
	
	semAttr = aggrExpr -> u.AGGR_EXPR.attr;
	
	logAttr.kind             = AGGR;
	logAttr.u.AGGR.varId     = semAttr.varId;
	logAttr.u.AGGR.tableId   = l_Query -> refTables [semAttr.varId ];
	logAttr.u.AGGR.attrId    = semAttr.attrId;
	logAttr.u.AGGR.fn        = aggrExpr -> u.AGGR_EXPR.fn;
	
	return logAttr;
}

static Logical::Expr *transformConstVal (const Semantic::Expr *semConstVal)
{
	ASSERT(semConstVal -> exprType == Semantic::E_CONST_VAL);
	
	Logical::Expr *logConstVal;
	
	switch (semConstVal -> type) {
	case INT:
		
		logConstVal = mk_const_int_expr (semConstVal -> u.ival);
		break;
		
	case FLOAT:
		
		logConstVal = mk_const_float_expr (semConstVal -> u.fval);
		break;
		
	case BYTE:
		
		logConstVal = mk_const_byte_expr (semConstVal -> u.bval);
		break;
		
	case CHAR:
		
		logConstVal = mk_const_char_expr (semConstVal -> u.sval);
		break;
		
	default:
		
		logConstVal = 0;
		break;
		
	}
	
	return logConstVal;
}

static Logical::Expr *transformExpr (const Semantic::Expr *semExpr) 

{	
	Semantic::Attr   semAttr;
	Logical::Expr   *logExpr;
	Logical::Expr   *logLeftExpr, *logRightExpr;
	
	switch (semExpr -> exprType) {
	case Semantic::E_CONST_VAL:
		
		logExpr = transformConstVal (semExpr);
		break;
		
	case Semantic::E_ATTR_REF:
		
		semAttr = semExpr -> u.attr;
		logExpr = mk_attr_expr (transformAttr(semAttr),
								semExpr -> type);
		break;
		
	case Semantic::E_AGGR_EXPR:
		
		logExpr = mk_attr_expr (transformAggrAttr(semExpr),
								semExpr -> type);
		break;
		
	case Semantic::E_COMP_EXPR:
		
		logLeftExpr = transformExpr (semExpr -> u.COMP_EXPR.left);
		logRightExpr = transformExpr (semExpr -> u.COMP_EXPR.right);
		
		logExpr = mk_comp_expr (semExpr -> u.COMP_EXPR.op,
								logLeftExpr, 
								logRightExpr);
		break;
		
	default:
		
		logExpr = 0;
		break;
	}
	
	return logExpr;
}

static Logical::BExpr transformBExpr (Semantic::BExpr semBExpr)
{	
	Logical::BExpr logBExpr;
	
	logBExpr.op     = semBExpr.op;
	logBExpr.left   = transformExpr (semBExpr.left);
	logBExpr.right  = transformExpr (semBExpr.right);
	
	return logBExpr;
}

static bool operator == (const Attr& attr1, const Attr& attr2) 
{
	if (attr1.kind != attr2.kind)
		return false;
	
	switch (attr1.kind) {
	case NAMED:
		
		return ((attr1.u.NAMED.varId == attr2.u.NAMED.varId)     &&
				(attr1.u.NAMED.tableId == attr2.u.NAMED.tableId) &&
				(attr1.u.NAMED.attrId == attr2.u.NAMED.attrId));
		
	case AGGR:
		
		return ((attr1.u.AGGR.varId == attr2.u.AGGR.varId)     &&
				(attr1.u.AGGR.tableId == attr2.u.AGGR.tableId) &&
				(attr1.u.AGGR.attrId == attr2.u.AGGR.attrId)   &&
				(attr1.u.AGGR.fn == attr2.u.AGGR.fn));		
		
	case UNNAMED:
		return false;
		
	default:
		return false;
	}
	
	// never comes
	return false;
}
