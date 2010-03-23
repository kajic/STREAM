#ifndef _LOG_PLAN_GEN_
#include "querygen/log_plan_gen.h"
#endif

#ifndef _DEBUG_
#include "common/debug.h"
#endif

/// debug
#ifdef _DM_
#include "querygen/logop_debug.h"
#endif

#include <iostream>
using namespace std;

using namespace Logical;

int LogPlanGen::genLogPlan (const Semantic::Query &query,
							Logical::Operator *& queryPlan) 
{
	int rc;
	
	// Generate naive plan
	if ((rc = genPlan_n (query, queryPlan)) != 0)
		return rc;
	
	ASSERT (check_plan (queryPlan));	
	
	// Transformation 1: remove 
	if((rc = t_rstreamNow (queryPlan)) != 0)
		return rc;

	ASSERT (check_plan (queryPlan));
	
	if((rc = t_removeIstream(queryPlan)) != 0)
		return rc;
	
	ASSERT (check_plan (queryPlan));
	
	if((rc = t_streamCross (queryPlan)) != 0)
		return rc;
	
	ASSERT (check_plan (queryPlan));
	
	if((rc = t_removeProject (queryPlan)) != 0)
		return rc;
	
	ASSERT (check_plan (queryPlan));
	
	if((rc = t_makeCrossBinary (queryPlan)) != 0)
		return rc;
	
	ASSERT (check_plan (queryPlan));
	
	if((rc = t_makeStreamCrossBinary (queryPlan)) != 0)
		return rc;
	
	ASSERT (check_plan (queryPlan));
	
	if((rc = t_pushSelect(queryPlan)) != 0)
		return rc;
	
	ASSERT (check_plan (queryPlan));
	
	return 0;
}

/**
 * Transform:
 *  
 * Plan pattern:
 *
 *   Stream Source -> [Now] -> (Select|Project|Distinct) * -> Rstream
 *
 * Output:
 * 
 *   Same plan with Now & Rstream removed.
 */ 
int LogPlanGen::t_rstreamNow (Operator *&plan)
{	
	Operator    *op;
	Operator    *rstream;
	Operator    *now;
	
	ASSERT (plan);
	ASSERT (plan -> output == 0); 	// topmost operator
	
	// Pattern failure: top most operator not rstream
	if (plan -> kind != LO_RSTREAM) {
		return 0;
	}
	
	rstream = plan;
	
	// Traverse down the plan upto [Now] operator
	op = rstream -> inputs [0];
	
	now = 0;
	while (true) {
		ASSERT (op);
		
		// Found a [Now] operator
		if (op -> kind == LO_NOW_WIN) {
			now = op;
			break;
		}
		
		// Select | Project | Distinct
		if ((op -> kind == LO_SELECT) ||
			(op -> kind == LO_PROJECT) ||
			(op -> kind == LO_DISTINCT)) {			
			op = op -> inputs [0];		 
		}
		
		// Pattern failure:
		else {
			return 0;
		}
	}
	
	// The pattern is satisfied: 
	// (1) remove rstream
	plan = rstream -> inputs [0];
	plan -> output = 0;
	
	// (2) remove now.
	now -> inputs [0] -> output = now -> output;
	
	if (!now -> output) {
		ASSERT (plan == now);
		plan = now -> inputs [0];
	}
	
	else {
		ASSERT (now -> output -> numInputs == 1);
		now -> output -> inputs [0] = now -> inputs [0];
		
		// Update all intermediate operators to reflect that they all produce
		// streams
		op = now -> output;
		while (op) {
			
			ASSERT ((op -> kind == LO_SELECT) ||
					(op -> kind == LO_PROJECT) ||
					(op -> kind == LO_DISTINCT));			
			ASSERT (op -> inputs [0] -> bStream);
			
			op -> bStream = true;
			op = op -> output;
		}
	}	
	
	return 0;
}

//
// Equality of attributes: used in isUselessProject
//
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
		
		return ((attr1.u.AGGR.varId == attr2.u.AGGR.varId)       &&
				(attr1.u.AGGR.tableId == attr2.u.AGGR.tableId)   &&
				(attr1.u.AGGR.attrId == attr2.u.AGGR.attrId)     &&
				(attr1.u.AGGR.fn == attr2.u.AGGR.fn));
		
	case UNNAMED:
	default:
		return false;		
	}
	// never comes
	return false;
}

static bool isUselessProject (const Operator *project)
{
	ASSERT (project -> kind == LO_PROJECT);
	
	Operator *input;
	
	input = project -> inputs [0];
	ASSERT (input);
	
	if (project -> numOutAttrs != input -> numOutAttrs)
		return false;
	
	for (unsigned int a = 0 ; a < project -> numOutAttrs ; a++)
		if (!(project -> outAttrs [a] == input -> outAttrs [a]))
			return false;
	
	return true;	
}


/**
 * Transform:
 *
 * Input plan pattern:
 *
 *    (something) --> Project --> (Select|Distinct|XStream)*
 *
 * Output:
 *
 *    If the project is redundant, remove the project operator.
 */

int LogPlanGen::t_removeProject (Operator *&plan)

{
	Operator *project;
	Operator *op;
	
	ASSERT (plan);
	ASSERT (plan -> output == 0);
		
	// Walk down the plan tree to locate a project operator
	project = 0;
	op = plan;
	while (true) {
		ASSERT (op);
		
		// project operator.
		if (op -> kind == LO_PROJECT) {
			project = op;
			break;
		}
		
		if ((op -> kind == LO_SELECT) ||
			(op -> kind == LO_DISTINCT) ||
			(op -> kind == LO_RSTREAM) ||
			(op -> kind == LO_DSTREAM) ||
			(op -> kind == LO_ISTREAM)) {
			op = op -> inputs [0];
		}
		
		// Pattern failure: no transformation
		else {
			return 0;
		}
	}
	
	// Remove the project if it is useless
	if (isUselessProject (project)) {
		project -> inputs [0] -> output = project -> output;
		
		// Project is the root of the query plan
		if (!project -> output) {
			plan = project -> inputs [0];
		}
		
		// Project is some intermediate operator
		else {
			ASSERT (project -> output -> numInputs == 1);
			project -> output -> inputs [0] = project -> inputs [0];
		}
	}
	
	return 0;
}

int LogPlanGen::t_removeIstream (Operator *&plan)
{
	ASSERT (plan);
	ASSERT (plan -> output == 0);
	
	if (plan -> kind == LO_ISTREAM && plan -> inputs [0] -> bStream) {
		plan = plan -> inputs [0];
		plan -> output = 0;
	}
	
	return 0;
}

int LogPlanGen::t_streamCross (Operator *&plan)
{
	int rc;
	Operator *rstream;
	Operator *cross;
	Operator *now;
	Operator *op;
	unsigned int nowIndex;
	
	ASSERT (plan);
	ASSERT (plan -> output == 0);
	
	// Root is not rstream: pattern failure
	if (plan -> kind != LO_RSTREAM) 		
		return 0;
	rstream = plan;
	
	// Travel down the plan tree to find the CROSS operator
	op = plan -> inputs [0];
	
	cross = 0;
	while (true) {
		ASSERT (op);
		
		if (op -> kind == LO_CROSS) {
			cross = op;
			break;
		}
		
		if ((op -> kind == LO_PROJECT) ||
			(op -> kind == LO_DISTINCT) ||
			(op -> kind == LO_GROUP_AGGR) || 
			(op -> kind == LO_SELECT)) {
			op = op -> inputs [0];
		}
		
		// Pattern failure
		else {
			return 0;
		}
	}
	
	now = 0;
	nowIndex = 0;
	for (unsigned int i = 0 ; i < cross -> numInputs ; i++) {
		if (cross -> inputs [i] -> kind == LO_NOW_WIN) {
			now = cross -> inputs [i];
			nowIndex = i;
			break;
		}
	}
	
	// No now window below the cross: pattern failure
	if (!now) 
		return 0;
	
	// Replace the CROSS by a STREAM CROSS operator
	Operator *streamCross;
	Operator *streamSource;
	
	streamSource = now -> inputs [0];
	ASSERT (streamSource);
	
	streamCross = mk_stream_cross (streamSource);
	if (!streamCross)
		return -1;
	
	for (unsigned int i = 0 ; i < cross -> numInputs ; i++) {
		if (i == nowIndex)
			continue;
		streamCross = stream_cross_add_input (streamCross,
											  cross -> inputs [i]);
		if (!streamCross)
			return -1;
	}
	
	// Cross cannot be the toppmost operator: we know rstream is above it.
	ASSERT(cross -> output);
	
	streamCross -> output = cross -> output;
	
	// We only jumped over operators with single stream input
	ASSERT(cross -> output -> numInputs == 1);
	
	cross -> output -> inputs [0] = streamCross;	   
	
	// Since the order of inputs for STREAM CROSS is (possibly) not the
	// same as that of CROSS, we need to update teh schema of the above
	// operators .
	if ((rc = update_schema_recursive (cross -> output)) != 0)
		return rc;
	
	if ((rc = update_stream_property_recursive (cross -> output)) != 0)
		return rc;
	
	// Finally remove the rstream
	ASSERT (rstream -> inputs [0]);
	plan = rstream -> inputs [0];
	plan -> output = 0;
	
	return 0;	
}

/**
 * Transform:
 *
 *   Input plan pattern: 
 * Cross (inp1, inp2, ,...inpn) --> (select|project|distinct|aggr|xstream)
 *
 *   Output:
 * Cross(.. Cross(Cross(inp1,inp2), inp3) ..) --> (select .... xstream)
 */ 

int LogPlanGen::t_makeCrossBinary(Operator *&plan)
{
	Operator *cross;
	Operator *op;
	
	ASSERT (plan);
	ASSERT (plan -> output == 0);
	
	// Walk down the tree to identify the CROSS operator
	cross = 0;
	op = plan;
	
	while (true) {
		ASSERT (op);
		
		if (op -> kind == LO_CROSS) {
			cross = op;
			break;
		}
		
		if ((op -> kind == LO_SELECT) ||
			(op -> kind == LO_PROJECT) ||
			(op -> kind == LO_GROUP_AGGR) ||
			(op -> kind == LO_DISTINCT) ||
			(op -> kind == LO_ISTREAM) ||
			(op -> kind == LO_DSTREAM) ||
			(op -> kind == LO_RSTREAM)) {
			
			op = op -> inputs [0];
		}
		
		// Unrecognized pattern
		else {
			return 0;
		}
	}
	
	unsigned int numInputs = cross -> numInputs;
	
	ASSERT (numInputs > 1);
	
	// Already binary: no work to do.
	if(numInputs == 2) 
		return 0;
	
	Operator *binCross, *binCrossParent;
	
	// Join the first two inputs to get the first of the sequence of
	// binary crosses.
	if (!(binCross = mk_cross()))
		return -1;		
	if (!(binCross = cross_add_input (binCross, cross -> inputs [0])))
		return -1;	
	if (!(binCross = cross_add_input (binCross, cross -> inputs [1])))
		return -1;

	for (unsigned int i = 2 ; i < cross -> numInputs ; i++) {
		if (!(binCrossParent = mk_cross()))
			return -1;		
		if (!(binCrossParent = cross_add_input (binCrossParent,
												binCross)))
			return -1;
		if (!(binCrossParent = cross_add_input (binCrossParent,
												cross -> inputs [i])))
			return -1;
		
		binCross = binCrossParent;
	}
	
	binCross -> output = cross -> output;			
	
	if (binCross -> output) {
		ASSERT (binCross -> output -> numInputs == 1);
		binCross -> output -> inputs [0] = binCross;
	}
	
	// cross was the root of the plan, now binCross is
	else {
		plan = binCross;
	}
	
	return 0;
}


/**
 * Transform:
 *
 *   Input plan pattern: 
 * Stream Cross (inp1, inp2, ,...inpn) --> (select|project|distinct|aggr|xstream)
 *
 *   Output:
 * StreamCross(.. StreamCross(Cross(inp1,inp2), inp3) ..) --> (select .... xstream)
 */ 

int LogPlanGen::t_makeStreamCrossBinary(Operator *&plan)
{
	Operator *streamCross;
	Operator *op;
	
	ASSERT (plan);
	ASSERT (plan -> output == 0);
	
	// Walk down the tree to identify the CROSS operator
	streamCross = 0;
	op = plan;
	
	while (true) {
		ASSERT (op);
		
		if (op -> kind == LO_STREAM_CROSS) {
			streamCross = op;
			break;
		}
		
		if ((op -> kind == LO_SELECT) ||
			(op -> kind == LO_PROJECT) ||
			(op -> kind == LO_GROUP_AGGR) ||
			(op -> kind == LO_DISTINCT) ||
			(op -> kind == LO_ISTREAM) ||
			(op -> kind == LO_DSTREAM) ||
			(op -> kind == LO_RSTREAM)) {
			
			op = op -> inputs [0];
		}
		
		// Unrecognized pattern
		else {
			return 0;
		}
	}
	
	unsigned int numInputs = streamCross -> numInputs;
	
	ASSERT (numInputs > 1);
	
	// Already binary: no work to do.
	if(numInputs == 2) 
		return 0;
	
	Operator *binStreamCross, *binStreamCrossParent;
	
	// Join the first two inputs to get the first of the sequence of
	// binary crosses.
	if (!(binStreamCross = mk_stream_cross(streamCross -> inputs [0])))
		return -1;		
	if (!(binStreamCross = stream_cross_add_input 
		  (binStreamCross, streamCross -> inputs [1])))
		return -1;
	
	for (unsigned int i = 2 ; i < numInputs ; i++) {
		if (!(binStreamCrossParent = mk_stream_cross(binStreamCross)))
			return -1;		
		if (!(binStreamCrossParent = 
			  stream_cross_add_input (binStreamCrossParent,
									  streamCross -> inputs [i])))
			return -1;
		
		binStreamCross = binStreamCrossParent;
	}
	
	binStreamCross -> output = streamCross -> output; 
	
	if (binStreamCross -> output) {
		ASSERT (binStreamCross -> output -> numInputs == 1);
		binStreamCross -> output -> inputs [0] = binStreamCross;
	}
	
	// cross was the root of the plan, now binCross is
	else {
		plan = binStreamCross;
	}
	
	return 0;
}

/**
 * Can a select be pushed below the given operator 
 */ 

static bool canBePushed(Operator *select, Operator *op,
						unsigned int &childPos)
{
	Expr    *leftExpr;       // left expr of the selection predicate 
	Expr    *rightExpr;      // right expr of the selection predicate
	
	// A select can always be pushed below another select
	if (op -> kind == LO_SELECT) {
		childPos = 0;
		return true;
	}
	
	// A select can be pushed below a cross/stream-cross if all the
	// attributes referenced in the select come from a single child
	// of the cross/stream-cross
	if ((op -> kind == LO_CROSS) ||
		(op -> kind == LO_STREAM_CROSS)) {
		
		leftExpr = select -> u.SELECT.bexpr.left;
		rightExpr = select -> u.SELECT.bexpr.right;
		
		for (childPos = 0 ; childPos < op -> numInputs ; childPos++) {
			if (check_reference (leftExpr, op -> inputs [childPos]) &&
				check_reference (rightExpr, op -> inputs [childPos]))
				break;			
		}
		
		return (childPos < op -> numInputs);
	}
	
	return false;
}

/**
 * Push select below a cross.  Does not delete the original select - just
 * makes a copy below the cross if possible [[ Explanation ]]
 */ 

static int pushSelect (Operator *select, bool &bPushed)
{
	
	Operator *op;
	bool bUsefulPush;
	unsigned int siblingPos, childPos;
	
	// Sanity check
	ASSERT (select -> kind == LO_SELECT);
	
	// We haven't pushed the select yet ...
	bPushed = false;

	// op is used to iterate down from the select operator (see loop
	// invariant below)
	op = select -> inputs [0];

	// siblingPos: position of op among its siblings
	siblingPos = 0;

	// bUsefulPush = true <--> there is non-select operator (cross)
	// between select and op.
	bUsefulPush = false;	
	
	// loop invariant: select can always be pushed from its present
	// position to just above op.
	while (op) {

		// Selects  can  be  pushed  below  certain  operators.   This  is
		// determined by the canBePushed  function, which also returns the
		// path along which the select can be pushed (siblingPos)
		
		if (canBePushed(select, op, childPos)) {
			
			// We are pushing below a non-select, so the overall pushing
			// of the select is "useful"
			if (op -> kind != LO_SELECT)
				bUsefulPush = true;

			// We should not be able to push below source operators.
			ASSERT (op -> numInputs > 0);
			
			op = op -> inputs [childPos];
			siblingPos = childPos;
		}
		
		else {
			break;
		}

	}
	
	// If the push is useful, make a copy of the select operator above
	// 'op'. 
	if (bUsefulPush) {
		Operator *pushedSelect;
		Operator *op_parent;

		
		ASSERT (op -> output);
		ASSERT (op -> output != select);		
		ASSERT (op -> output -> numInputs > siblingPos);
		
		// insert a copy of 'select' between op and its parent
		op_parent = op -> output;
		
		pushedSelect = mk_select (op, select -> u.SELECT.bexpr);
		op_parent -> inputs [siblingPos] = pushedSelect;
		pushedSelect -> output = op_parent;
		
		bPushed = true;
	}
		
	return 0;
}

/**
 * Transform:
 * 
 * Input plan:
 *
 *     --> Cross (  ) --> (select) --> (select) --> ... -->
 *         --> (project|distinct|aggr|xstream)
 *
 * Pushes the selects below the Cross / Stream Cross
 *
 */ 

int LogPlanGen::t_pushSelect (Operator *&plan)
{
	int rc;
	Operator *select;
	Operator *op;
	
	ASSERT (plan);
	ASSERT (plan -> output == 0);
	
	// Get the top most select operator
	op = plan;
	select = 0;
	while (true) {
		ASSERT (op);
		
		if (op -> kind == LO_SELECT) {
			select = op;
			break;
		}
		
		if ((op -> kind == LO_ISTREAM) || 
			(op -> kind == LO_DSTREAM) || 
			(op -> kind == LO_RSTREAM) ||
			(op -> kind == LO_PROJECT) ||
			(op -> kind == LO_DISTINCT) ||
			(op -> kind == LO_GROUP_AGGR)) {
			
			ASSERT (op -> numInputs == 1);
			op = op -> inputs [0];
		}
		
		// Pattern does not match the required pattern
		else {
			return 0;
		}
	}
	
	// Try to push each select operator as much as possible.
	while (true) {
		
		bool bPushed;

		ASSERT (check_plan(plan));
		
		// Try to push the select below the cross.  For coding convenience
		// the pushSelect function pushes a copy of the select operator,
		// while retaining the original select operator in its initial
		// position. 
		if ((rc = pushSelect (select, bPushed)) != 0)
			return rc;

		ASSERT (check_plan(plan));
		
		// We manage to push this one: delete the existing select
		// operation.
		if (bPushed) {
			ASSERT (select -> inputs [0]);
			
			select -> inputs [0] -> output = select -> output;
			
			if (select -> output) {
				ASSERT (select -> output -> numInputs == 1);
				select -> output -> inputs [0] = select -> inputs [0];
			}
			
			else {
				ASSERT (plan == select);
				plan = select -> inputs [0];				
			}
		}
		
		select = select -> inputs [0];
		
		if (select -> kind != LO_SELECT)
			break;		
	}
	
	return 0;
}
