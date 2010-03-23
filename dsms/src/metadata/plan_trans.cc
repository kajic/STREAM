/**
 * @file    plan_trans.cc
 * @date    Aug. 24, 2004
 * @brief   Routines to transform plans into (hopefully) better plans.
 */

#ifndef _PLAN_MGR_IMPL_
#include "metadata/plan_mgr_impl.h"
#endif

#ifndef _DEGUG_
#include "common/debug.h"
#endif

using namespace Metadata;
using namespace Physical;
using namespace std;

/**
 * Remove this operator from a plan, and connect all its inputs to its
 * outputs directly.
 */

static int shortCircuitInputOutput (Operator *op);
static int addOutput (Operator *child, Operator *parent);
static bool operator == (Attr a1, Attr a2);

int PlanManagerImpl::removeQuerySources ()
{
	int rc;
	Operator *op, *nextOp;
	
	// Iterate through all the used ops and remove query sources
	op = usedOps;
	while (op) {
		
		if (op -> kind == PO_QUERY_SOURCE) {
			rc = shortCircuitInputOutput (op);
			
			if (rc != 0)
				return rc;
			
			// we remember the next used op before freeing op
			nextOp = op -> next;
			free_op (op);
			
			op = nextOp;
		}
		
		else {
			op = op -> next;
		}
	}
	
	return 0;		
}

int PlanManagerImpl::addSinks ()
{
	int rc;
	
	Operator *op;
	Operator *sink;
	
	op = usedOps;
	while (op) {
		if ((op -> numOutputs == 0) && (op -> kind != PO_OUTPUT) &&
			(op -> kind != PO_SS_GEN)) {
			// create a new sink operator to read off the output of op
			if ((rc = mk_sink (op, sink)) != 0)
				return rc;
		}
		
		op = op -> next;
	}

	return 0;
}

static void append (BExpr *&dest, BExpr *src)
{
	BExpr *p;
	
	// destination predicate list empty
	if (!dest) {
		dest = src;
		return;
	}

	p = dest;
	while (p -> next)
		p = p -> next;	
	p -> next = src;
	
	return;
}

/**
 * Transform an attribute in a operator above join to an equivalent 
 * attribute within the join operator.
 */
static void transToJoinAttr (Attr &attr, Operator *join)
{
	ASSERT (join);
	ASSERT (attr.input == 0);
	ASSERT (attr.pos < join -> numAttrs);
	
	// We only deal with binary joins
	ASSERT (join -> numInputs == 2);
	ASSERT (join -> inputs [0]);
	ASSERT (join -> inputs [1]);
	ASSERT (join -> kind == PO_JOIN ||
			join -> kind == PO_STR_JOIN);

	if (join -> kind == PO_JOIN) {
		
		if (join -> u.JOIN.numOuterAttrs <= attr.pos) {
			attr.input = 1;
			attr.pos -= join -> u.JOIN.numOuterAttrs;
		}
		
	}
	
	else {
		
		if (join -> u.STR_JOIN.numOuterAttrs <= attr.pos) {
			attr.input = 1;
			attr.pos -= join -> u.STR_JOIN.numOuterAttrs;
		}
		
	}
	
}

/**
 * Transform an arithmetic expression in an operator above a join to an 
 * equiv. expression within the join.  The transformation is "in-place". 
 *
 * @param expr cannot be null.
 */

static void transToJoinExpr (Expr *expr, Operator *join)
{
	ASSERT (expr);
	
	switch (expr -> kind) {
	case CONST_VAL: break; // do nothing
	case COMP_EXPR:
		transToJoinExpr (expr -> u.COMP_EXPR.left, join);
		transToJoinExpr (expr -> u.COMP_EXPR.right, join);
		break;
	case ATTR_REF:
		transToJoinAttr (expr -> u.attr, join);
		break;
	default:
		break;
	}
}

/**
 * Transform a predicate in an operator above a join to an
 * equiv. predicate within the join. The transformation is "in-place".
 *
 * @param pred   predicate to be transformed. Could be null
 */

static void transToJoinPred (BExpr *pred, Operator *join)
{
	if (pred) {
		ASSERT (pred -> left);
		ASSERT (pred -> right);
		
		transToJoinExpr (pred -> left, join);
		transToJoinExpr (pred -> right, join);
		
		// pred -> next == 0 handled properly
		transToJoinPred (pred -> next, join);
	} 
}

static bool mergeSelect (Operator *op)
{
	Operator *inOp;
	
	ASSERT (op -> numInputs == 1);
	ASSERT (op -> inputs [0]);
	ASSERT (op -> inputs [0] -> numOutputs >= 1);
	
	inOp = op -> inputs [0];

	// We cannot merge if someone else is reading from the child operator
	if (inOp -> numOutputs != 1)
		return false;
	
	if (inOp -> kind == PO_SELECT) {
		append (inOp -> u.SELECT.pred, op -> u.SELECT.pred);
		return true;		
	}
	
	if (inOp -> kind == PO_JOIN) {		
		// Transform the predicate in the select to an equivalent
		// predicate in the join.
		transToJoinPred (op -> u.SELECT.pred, inOp);
		
		append (inOp -> u.JOIN.pred, op -> u.SELECT.pred);
		return true;
	}
	
	if (inOp -> kind == PO_STR_JOIN) {
		// Transform the predicate in the select to an equivalent
		// predicate in the join.
		transToJoinPred (op -> u.SELECT.pred, inOp);
		
		append (inOp -> u.STR_JOIN.pred, op -> u.SELECT.pred);
		return true;
	}
	
	return false;
}

int PlanManagerImpl::mergeSelects ()
{
	int rc;
	Operator *op, *nextOp;
	
	// Iterate through all the active ops and try merging selects
	op = usedOps;
	while (op) {
		
		// If we manage to successfully merge a select with its child
		// (another select or a join), we short circuit it
		if (op -> kind == PO_SELECT && mergeSelect (op)) {
			
			rc = shortCircuitInputOutput (op);
			if (rc != 0)
				return rc;
			
			nextOp = op -> next;
			free_op (op);
			
			op = nextOp;			
		}
		
		else {
			op = op -> next;
		}
	}
	
	return 0;	
}

static bool mergeProject (Operator *project)
{
	Operator *inOp;
	BExpr *pred;
	
	ASSERT (project);
	ASSERT (project -> numInputs == 1);
	ASSERT (project -> inputs [0]);   
	
	inOp = project -> inputs [0];

	// We cannot merge if someone is reading from the child operator.
	if (inOp -> numOutputs > 1)
		return false;
	
	if (inOp -> kind == PO_JOIN) {

		pred = inOp -> u.JOIN.pred;
		
		// Transform and copy the projections ...
		for (unsigned int a = 0 ;a < project -> numAttrs ; a++) {
			transToJoinExpr (project -> u.PROJECT.projs [a], inOp);
		}
		
		// transform the input operator to a join-project
		inOp -> kind = PO_JOIN_PROJECT;

		// The output schema of the new operator is the same as the project
		inOp -> numAttrs = project -> numAttrs;
		for (unsigned int a = 0 ; a < inOp -> numAttrs ; a++) {
			inOp -> attrTypes [a] = project -> attrTypes [a];
			inOp -> attrLen [a] = project -> attrLen [a];
			inOp -> u.JOIN_PROJECT.projs [a] =
				project -> u.PROJECT.projs [a];
		}
		
		ASSERT (inOp -> bStream == project -> bStream);
		ASSERT (inOp -> outputs [0] == project);
		
		inOp -> u.JOIN_PROJECT.pred = pred;				
		
		return true;
	}

	if (inOp -> kind == PO_STR_JOIN) {
		
		pred = inOp -> u.STR_JOIN.pred;
		
		// Transform and copy the projections
		for (unsigned int a = 0 ; a < project -> numAttrs ; a++) {
			transToJoinExpr (project -> u.PROJECT.projs [a], inOp);
		}			
		
		// transform the input to the str-join-project
		inOp -> kind = PO_STR_JOIN_PROJECT;

		// The output schema of the new op is the same as the project
		inOp -> numAttrs = project -> numAttrs;
		for (unsigned int a = 0 ; a < inOp -> numAttrs ; a++) {
			inOp -> attrTypes [a] = project -> attrTypes [a];
			inOp -> attrLen [a] = project -> attrLen [a];
			inOp -> u.STR_JOIN_PROJECT.projs [a] =
				project -> u.PROJECT.projs [a];
		}
		
		ASSERT (inOp -> bStream == project -> bStream);
		ASSERT (inOp -> outputs [0] == project);

		inOp -> u.STR_JOIN_PROJECT.pred = pred;		
		
		return true;
	}
	
	return false;
}

int PlanManagerImpl::mergeProjectsToJoin()
{
	int rc;
	Operator *op, *nextOp;

	// Iterate through all the active ops and try merging projects
	op = usedOps;
	
	while (op) {
		
		if (op -> kind == PO_PROJECT && mergeProject (op)) {
			
			rc = shortCircuitInputOutput (op);
			if (rc != 0)
				return rc;
			
			nextOp = op -> next;
			free_op(op);
			
			op = nextOp;
		}
		
		else {
			op = op -> next;
		}
	}
	
	return 0;
}

static bool existsCount (Operator *op)
{
	unsigned int numAggrAttrs;

	numAggrAttrs = op -> u.GROUP_AGGR.numAggrAttrs;

	for (unsigned int a = 0 ; a < numAggrAttrs ; a++)
		if (op -> u.GROUP_AGGR.fn [a] == COUNT)
			return true;
	
	return false;
}

static bool existsSum (Operator *op, Attr attr)
{
	unsigned int numAggrAttrs;

	numAggrAttrs = op -> u.GROUP_AGGR.numAggrAttrs;

	for (unsigned int a = 0 ; a < numAggrAttrs ; a++)
		if ((op -> u.GROUP_AGGR.aggrAttrs [a] == attr) &&
			(op -> u.GROUP_AGGR.fn [a] == SUM))
			return true;

	return false;
}

static int addCount (Operator *op)
{
	unsigned int numAggrAttrs;
	
	numAggrAttrs = op -> u.GROUP_AGGR.numAggrAttrs;
	
	if (numAggrAttrs >= MAX_AGGR_ATTRS)
		return -1;
	
	op -> u.GROUP_AGGR.aggrAttrs [numAggrAttrs].input = 0;
	op -> u.GROUP_AGGR.aggrAttrs [numAggrAttrs].pos = 0;
	op -> u.GROUP_AGGR.fn [numAggrAttrs] = COUNT;
	op -> u.GROUP_AGGR.numAggrAttrs ++;

	if (op -> numAttrs >= MAX_ATTRS)
		return -1;
	
	op -> attrTypes [ op -> numAttrs ] = INT;
	op -> attrLen [ op -> numAttrs ] = INT_SIZE;
	op -> numAttrs ++;
	
	return 0;
}

static int addSum (Operator *op, Attr attr)
{
	unsigned int numAggrAttrs;
	
	numAggrAttrs = op -> u.GROUP_AGGR.numAggrAttrs;
	
	if (numAggrAttrs >= MAX_AGGR_ATTRS)
		return -1;

	op -> u.GROUP_AGGR.aggrAttrs [numAggrAttrs] = attr;
	op -> u.GROUP_AGGR.fn [numAggrAttrs] = SUM;
	op -> u.GROUP_AGGR.numAggrAttrs ++;

	if (op -> numAttrs >= MAX_ATTRS)
		return -1;
	
	op -> attrTypes [ op -> numAttrs ] =
		op -> inputs [0] -> attrTypes [attr.pos];
	op -> attrLen [ op -> numAttrs ] =
		op -> inputs [0] -> attrLen [attr.pos];
	op -> numAttrs ++;
	
	return 0;
}

static int addIntAggrsGby (Operator *op)
{
	int rc;
	unsigned int numAggrAttrs;

	numAggrAttrs = op -> u.GROUP_AGGR.numAggrAttrs;
	
	ASSERT (op -> inputs [0]);
	
	// If the input to a gby aggr. operator is not a stream we need a
	// maintain a count aggr. within the operator to determine when a
	// group no longer exists.
	if (!op -> inputs [0] -> bStream && !existsCount (op)) { 
		rc = addCount (op);
		if (rc != 0) return rc;
	}
	
	for (unsigned int a = 0 ; a < numAggrAttrs ; a++) {
		if (op -> u.GROUP_AGGR.fn [a] != AVG)
			continue;
		
		if (!existsCount (op)) {
			rc = addCount (op);
			if (rc != 0) return rc;
		}
		
		if (!existsSum (op, op -> u.GROUP_AGGR.aggrAttrs [a])) {
			rc = addSum (op, op -> u.GROUP_AGGR.aggrAttrs [a]);
			if (rc != 0) return rc;
		}
	}
	
	return 0;	
}

static int addIntAggrsDistinct (Operator *op)
{
	int rc;
	
	ASSERT (op -> inputs [0]);
	
	if (!op -> inputs [0] -> bStream) {
		rc = addCount (op);
		if (rc != 0) return rc;
	}
	
	return 0;
}

int PlanManagerImpl::addIntAggrs ()
{
	int rc;
	Operator *op;

	// Iterate through all the active ops
	op = usedOps;
	while (op) {

		if (op -> kind == PO_GROUP_AGGR) {
			if ((rc = addIntAggrsGby (op)) != 0) {
				return rc;
			}
		}
		
		else if (op -> kind == PO_DISTINCT) {
			if ((rc = addIntAggrsDistinct (op)) != 0) {
				return rc;
			}
		}
		
		op = op -> next;		
	}
	
	return 0;
}

int PlanManagerImpl::mergeSelects_mon (Operator *&plan,
									   Operator **opList,
									   unsigned int &numOps)
{
	int rc;
	Operator *op;
	
	for (unsigned int o = 0 ; o < numOps ; ) {
		op = opList [o];
		
		// If we manage to successfully merge a select with its child
		// (another select or a join), we short circuit it
		if (op -> kind == PO_SELECT && mergeSelect (op)) {

			// Root operator is never select, it is output
			ASSERT (op != plan);
			
			rc = shortCircuitInputOutput (op);
			if (rc != 0)
				return rc;
			
			// Move the last operator to the current position
			numOps--;
			opList [o] = opList [numOps];
			
			free_op (op);
		}
		
		else {
			o++;
		}
	}

	return 0;
}

int PlanManagerImpl::mergeProjectsToJoin_mon (Operator *&plan,
											  Operator **opList,
											  unsigned int &numOps)
{
	int rc;
	Operator *op;
	
	for (unsigned int o = 0 ; o < numOps ; ) {
		op = opList [o];
		
		// If we manage to successfully merge a project with its child
		// (a join), we short circuit it
		if (op -> kind == PO_PROJECT && mergeProject (op)) {
			
			// Root operator is never project, it is output
			ASSERT (op != plan);
			
			rc = shortCircuitInputOutput (op);
			if (rc != 0)
				return rc;
			
			// Move the last operator to the current position
			numOps--;
			opList [o] = opList [numOps];
			
			free_op (op);
		}
		
		else {
			o++;
		}
	}
	
	return 0;	
}

int PlanManagerImpl::addIntAggrs_mon (Operator *&plan,
									  Operator **opList,
									  unsigned int &numOps)
{
	int rc;
	Operator *op;

	for (unsigned int o = 0 ; o < numOps ; o++) {
		op = opList [o];
			
		if (op -> kind == PO_GROUP_AGGR) {
			if ((rc = addIntAggrsGby (op)) != 0) {
				return rc;
			}
		}
		
		else if (op -> kind == PO_DISTINCT) {
			if ((rc = addIntAggrsDistinct (op)) != 0) {
				return rc;
			}
		}		
	}
	
	return 0;
}

static unsigned int getOutputIndex (Operator *child, Operator *parent)
{
	ASSERT (child);
	ASSERT (parent);
	
	for (unsigned int o = 0 ; o < child -> numOutputs ; o++)
		if (child -> outputs [o] == parent)
			return o;
	
	// should never come here
	ASSERT (0);
	return 0;
}

static unsigned int getInputIndex (Operator *parent, Operator *child)
{
	ASSERT (child);
	ASSERT (parent);
	
	for (unsigned int i = 0 ; i < parent -> numInputs ; i++)
		if (parent -> inputs [i] == child)
			return i;
	
	// should never come here
	ASSERT (0);
	return 0;
}

/**
 * Remove this operator from a plan, and directly connect its input to all
 * its outputs.
 */

static int shortCircuitInputOutput (Operator *op)
{
	int rc;
	Operator *inOp;
	unsigned int inputIdx;
	unsigned int outputIdx;
	
	ASSERT (op);
	ASSERT (op -> numInputs == 1);
	ASSERT (op -> inputs [0]);
	ASSERT (op -> inputs [0] -> numOutputs >= 1);
	
	// input operator
	inOp = op -> inputs [0];
	
	// position of op among inOp's outputs
	outputIdx = getOutputIndex (inOp, op);
	
	// update all the output operators
	for (unsigned int o = 0 ; o < op -> numOutputs ; o++) {
		
		// position of op among output operators inputs
		inputIdx = getInputIndex (op -> outputs [o], op);
		
		// short-circuit
		op -> outputs [o] -> inputs [inputIdx] = inOp;
		
		if ((rc = addOutput (inOp, op -> outputs [o])) != 0)
			return rc;						
	}
	
	// move the last output of inOp to outputIdx position [This works even
	// if op had no outputs - if you can't understand this never mind :) ]
	inOp -> outputs [outputIdx] =
		inOp -> outputs [-- (inOp -> numOutputs) ];
	
	return 0;
}

static int addOutput (Operator *child, Operator *parent)
{
	ASSERT (child);

	if (child -> numOutputs >= MAX_OUT_BRANCHING) {
		return -1;
	}

	child -> outputs [child -> numOutputs ++] = parent;
	return 0;
}

static bool operator == (Attr a1, Attr a2)
{
	return ((a1.input == a2.input) && (a1.pos == a2.pos));
}

