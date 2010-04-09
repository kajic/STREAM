#include <string.h>

#ifndef _PLAN_MGR_IMPL_
#include "metadata/plan_mgr_impl.h"
#endif

#ifndef _DEBUG_
#include "common/debug.h"
#endif

using namespace Metadata;
using namespace Physical;
using namespace std;

//----------------------------------------------------------------------
// Schema manipulation routines
//----------------------------------------------------------------------

/// Append schema of src to the schema of dest
static int append_schema (Operator *dest, Operator *src);

/// Copy schema of src to the schema of dest
static int copy_schema (Operator *dest, Operator *src);

//----------------------------------------------------------------------

static Type getAttrType (Physical::Attr, Physical::Operator *);
static unsigned int getAttrLen (Physical::Attr, Physical::Operator *);	   

int PlanManagerImpl::genPhysicalPlan(Logical::Operator *logPlan,
									 Physical::Operator *&phyPlan)
{
	int rc;
	
	// root operator in the logical plan
	Logical::Operator *l_rootOp;
	
	// physical plans for each child of the root
	Physical::Operator *p_childPlans [Logical::MAX_INPUT_OPS];
	
	ASSERT (logPlan);
	
	l_rootOp = logPlan;
	
	// recursively produce the physical plan for each child logical plan
	for (unsigned int i = 0 ; i < l_rootOp -> numInputs ; i++) {
		
		if ((rc = genPhysicalPlan (l_rootOp -> inputs [i],
								   p_childPlans [i])) != 0) {
			return rc;
		}
	}
	
	switch (l_rootOp -> kind) {		
	case Logical::LO_STREAM_SOURCE:		
		return mk_stream_source (logPlan, p_childPlans, phyPlan);
		
	case Logical::LO_RELN_SOURCE:
		return mk_reln_source (logPlan, p_childPlans, phyPlan);
		
	case Logical::LO_ROW_WIN:
		return mk_row_win (logPlan, p_childPlans, phyPlan);
		
	case Logical::LO_RANGE_WIN:
		return mk_range_win (logPlan, p_childPlans, phyPlan);
		
	case Logical::LO_NOW_WIN:
		return mk_now_win (logPlan, p_childPlans, phyPlan);
		
	case Logical::LO_PARTN_WIN:
		return mk_partn_win (logPlan, p_childPlans, phyPlan);
		
	case Logical::LO_SELECT:
		return mk_select (logPlan, p_childPlans, phyPlan);
		
	case Logical::LO_PROJECT:
		return mk_project (logPlan, p_childPlans, phyPlan);
		
	case Logical::LO_CROSS:
		
		// Currently we expect all joins in logical plans to be binary
		if (l_rootOp -> numInputs != 2) {
			LOG << "PlanMgr: invalid logical plan with multiway joins"
				<< endl;
			return -1;
		}			
		
		return mk_join (logPlan, p_childPlans, phyPlan);

	case Logical::LO_STREAM_CROSS:

		// Currently we expect all joins in logical plans to be binary
		if (l_rootOp -> numInputs != 2) {
			LOG << "PlanMgr: invalid logical plan with multiway joins"
				<< endl;
			return -1;
		}			
		
		return mk_str_join (logPlan, p_childPlans, phyPlan);
		
	case Logical::LO_GROUP_AGGR:
		return mk_group_aggr (logPlan, p_childPlans, phyPlan);		
		
	case Logical::LO_DISTINCT:
		return mk_distinct (logPlan, p_childPlans, phyPlan);
		
	case Logical::LO_ISTREAM:
		return mk_istream (logPlan, p_childPlans, phyPlan);
		
	case Logical::LO_DSTREAM:
		return mk_dstream (logPlan, p_childPlans, phyPlan);
		
	case Logical::LO_RSTREAM:
		return mk_rstream (logPlan, p_childPlans, phyPlan);
		
	case Logical::LO_UNION:
		return mk_union (logPlan, p_childPlans, phyPlan);
		
	case Logical::LO_EXCEPT:
		return mk_except (logPlan, p_childPlans, phyPlan);
		
	default:
		// unknown kind
		ASSERT (0);
		return -1;
	}
	
	// never comes
	return -1;
}

int PlanManagerImpl::mk_stream_source (Logical::Operator *logPlan,
									   Physical::Operator **phyChildPlans,
									   Physical::Operator *&phyPlan)
{
	unsigned int strId;
	unsigned int srcOpId;
	bool bSrcOpFound;
	
	ASSERT (logPlan);
	ASSERT (logPlan -> numInputs == 0);
	
	// Id for the stream
	strId = logPlan -> u.STREAM_SOURCE.strId;
	
	// Find the operator that is the source for this stream
	bSrcOpFound = false;
	srcOpId = 0;
	for (unsigned int t = 0 ; t < numTables && !bSrcOpFound ; t++) {
		if (sourceOps [t].tableId == strId) {
			bSrcOpFound = true;
			srcOpId = sourceOps [t].opId;
		}
	}
	
	// If the system executes correctly, we should always have a src op
	ASSERT (bSrcOpFound);	
	
	// The physical plan is just this source operator
	phyPlan = ops + srcOpId;	
	
	return 0;
}
	
int PlanManagerImpl::mk_reln_source (Logical::Operator *logPlan,
									 Physical::Operator **phyChildPlans,
									 Physical::Operator *&phyPlan)
{
	unsigned int relId;
	unsigned int srcOpId;
	bool bSrcOpFound;
	
	ASSERT (logPlan);
	ASSERT (logPlan -> numInputs == 0);
	
	// Id for the relation
	relId = logPlan -> u.RELN_SOURCE.relId;
	
	// Find the operator that is the source for this relation
	bSrcOpFound = false;
	srcOpId = 0;
	for (unsigned int t = 0 ; t < numTables && !bSrcOpFound ; t++) {
		if (sourceOps [t].tableId == relId) {
			bSrcOpFound = true;
			srcOpId = sourceOps [t].opId;
		}
	}
	
	// If the system executes correctly, we should always have a src op
	ASSERT (bSrcOpFound);
	
	// The physical plan is just this source operator
	phyPlan = ops + srcOpId;
	
	return 0;
}

int PlanManagerImpl::mk_row_win (Logical::Operator *logPlan,
								 Physical::Operator **phyChildPlans,
								 Physical::Operator *&phyPlan)
{
	int rc;
	Operator *row_win;
	
	ASSERT (logPlan);
	ASSERT (logPlan -> kind == Logical::LO_ROW_WIN);
	ASSERT (logPlan -> numInputs == 1);
	
	// create a new operator
	row_win = new_op (PO_ROW_WIN);
	if (!row_win) {
		LOG << "PlanManager: no space for operators" << endl;
		return -1;
	}

	// Initializations.
	row_win -> store = 0;
	row_win -> instOp = 0;
	row_win -> u.ROW_WIN.winSyn = 0;
	
	// output schema = input schema
	if ((rc = copy_schema (row_win, phyChildPlans [0])) != 0)
		return rc;
	
	// output is a relation, not stream
	row_win -> bStream = false;
	
	// currently noone is reading from this operator
	row_win -> numOutputs = 0;
	
	// input: 
	row_win -> numInputs = 1;
	row_win -> inputs [0] = phyChildPlans [0];

	if ((rc = addOutput (phyChildPlans [0], row_win)) != 0)
		return rc;
	
	// window specification
	row_win -> u.ROW_WIN.numRows = logPlan -> u.ROW_WIN.numRows;
		
	phyPlan = row_win;
	
	return 0;
}

int PlanManagerImpl::mk_range_win (Logical::Operator *logPlan,
								   Physical::Operator **phyChildPlans,
								   Physical::Operator *&phyPlan)
{
	int rc;
	Operator *range_win;
	
	ASSERT (logPlan);
	ASSERT (logPlan -> kind == Logical::LO_RANGE_WIN);
	ASSERT (logPlan -> numInputs == 1);
	
	// new operator
	range_win = new_op (PO_RANGE_WIN);
	if (!range_win) {
		LOG << "PlanManager: no space for operators" << endl;
		return -1;
	}
	
	// Initializations
	range_win -> store = 0;
	range_win -> instOp = 0;
	range_win -> u.RANGE_WIN.winSyn = 0;
	
	// output schema = input schema
	if ((rc = copy_schema (range_win, phyChildPlans [0])) != 0)
		return rc;
	
	// output is a relation, not a stream
	range_win -> bStream = false;

	// currently no one is reading from this operator
	range_win -> numOutputs = 0;

	// input:
	range_win -> numInputs = 1;
	range_win -> inputs [0] = phyChildPlans [0];

	if ((rc = addOutput (phyChildPlans [0], range_win)) != 0)
		return rc;
	
	// window specification:
	range_win -> u.RANGE_WIN.timeUnits = logPlan -> u.RANGE_WIN.timeUnits;
    range_win -> u.RANGE_WIN.slideUnits = logPlan -> u.RANGE_WIN.slideUnits;
	
	phyPlan = range_win;

	return 0;
}

int PlanManagerImpl::mk_now_win (Logical::Operator *logPlan,
								 Physical::Operator **phyChildPlans,
								 Physical::Operator *&phyPlan)
{
	int rc;
	Operator *now_win;
	
	ASSERT (logPlan);
	ASSERT (logPlan -> kind == Logical::LO_NOW_WIN);
	ASSERT (logPlan -> numInputs == 1);
	
	// new operator
	now_win = new_op (PO_RANGE_WIN);
	if (!now_win) {
		LOG << "PlanManager: no space for operators" << endl;
		return -1;
	}

	// Initializations
	now_win -> store = 0;
	now_win -> instOp = 0;
	now_win -> u.RANGE_WIN.winSyn = 0;
	
	// output schema = input schema
	if ((rc = copy_schema (now_win, phyChildPlans [0])) != 0)
		return rc;
	
	// output is a relation, not a stream
	now_win -> bStream = false;
	
	// currently no one is reading from this operator
	now_win -> numOutputs = 0;
	
	// input:
	now_win -> numInputs = 1;
	now_win -> inputs [0] = phyChildPlans [0];
	
	if ((rc = addOutput (phyChildPlans [0], now_win)) != 0)
		return rc;
	
	// window specification:
	now_win -> u.RANGE_WIN.timeUnits = 1;
	
	phyPlan = now_win;
	
	return 0;
}

int PlanManagerImpl::mk_partn_win (Logical::Operator *logPlan,
								   Physical::Operator **phyChildPlans,
								   Physical::Operator *&phyPlan)
{
	int rc;
	Operator *partn_win;

	ASSERT (logPlan);
	ASSERT (logPlan -> kind == Logical::LO_PARTN_WIN);
	ASSERT (logPlan -> numInputs == 1);
	
	// new operator
	partn_win = new_op (PO_PARTN_WIN);
	if (!partn_win) {
		LOG << "PlanManager: no space for operators" << endl;
		return -1;
	}

	partn_win -> store = 0;
	partn_win -> instOp = 0;
	
	// output schema = input schema
	if ((rc = copy_schema (partn_win, phyChildPlans [0])) != 0)
		return rc;
	
	// output is a relation not a stream
	partn_win -> bStream = false;
	
	// input:
	partn_win -> numInputs = 1;
	partn_win -> inputs [0] = phyChildPlans [0];

	if ((rc = addOutput (phyChildPlans [0], partn_win)) != 0)
		return rc;
	
	// window specification:
	partn_win -> u.PARTN_WIN.numPartnAttrs =
		logPlan -> u.PARTN_WIN.numPartnAttrs;
	partn_win -> u.PARTN_WIN.numRows =
		logPlan -> u.PARTN_WIN.numRows;
	
	ASSERT (partn_win -> u.PARTN_WIN.numPartnAttrs < MAX_GROUP_ATTRS);	
	ASSERT (logPlan -> numInputs == 1);
	
	// window specification: partition attributes
	for (unsigned int a = 0 ; a < partn_win -> u.PARTN_WIN.numPartnAttrs ;
		 a++) {		
		partn_win -> u.PARTN_WIN.partnAttrs [a] =
			transformAttr (logPlan -> u.PARTN_WIN.partnAttrs [a], logPlan);		
	}
	
	partn_win -> u.PARTN_WIN.winSyn = 0;
	
	phyPlan = partn_win;
	
	return 0;
}

int PlanManagerImpl::mk_select (Logical::Operator *logPlan,
								Physical::Operator **phyChildPlans,
								Physical::Operator *&phyPlan)
{
	int rc;
	Operator *select;
	BExpr *pred;
	
	ASSERT (logPlan);
	ASSERT (logPlan -> kind == Logical::LO_SELECT);
	ASSERT (logPlan -> numInputs == 1);
	
	// Transform the boolean expression from "logical" to physical "form"
	pred = transformBExpr (logPlan -> u.SELECT.bexpr, logPlan);
	if (!pred)
		return -1;
	
	// If the physical plan corresponding to child of this select operator
	// (S1)  is  a  (physical)  select  operator (S2),  we  just  add  the
	// predicate  of logical  select operator  (S1) to  (S2).   In logical
	// plans the  select operators only contain atomic  predicates to help
	// us push  down select predicates  as much as possible.   In physical
	// plans  select  operators  can   contain  a  conjunction  of  atomic
	// predicates.   We   get  better   efficiency  by  packing   as  much
	// functionality into a single physical select operator.
	
	if (phyChildPlans [0] -> kind == Physical::PO_SELECT) {
		phyPlan = phyChildPlans [0];
		pred -> next = phyPlan -> u.SELECT.pred;
		phyPlan -> u.SELECT.pred = pred;
	}
	
	// Otherwise, create a new selection operator ...
	else {
		select = new_op(Physical::PO_SELECT);
		if (!select) {
			LOG << "PlanManager: no space for operators" << endl;
			return -1;
		}

		select -> store = 0;
		select -> instOp = 0;
		
		// output schema = input schema
		if ((rc = copy_schema (select, phyChildPlans [0])) != 0)
			return -1;
		
		// output is a stream iff input is a stream
		select -> bStream = phyChildPlans [0] -> bStream;
		
		select -> numOutputs = 0;
		
		select -> numInputs = 1;
		select -> inputs [0] = phyChildPlans [0];
		if ((rc = addOutput (phyChildPlans [0], select)) != 0)
			return rc;
		
		select -> u.SELECT.pred = pred;
		
		phyPlan = select;
	}
	
	return 0;	
}

int PlanManagerImpl::mk_dummy_project (Physical::Operator *child,
									   Physical::Operator *&project)
{
	int rc;
	Expr *projExpr;
	
	project = new_op (Physical::PO_PROJECT);
	if (!project) {
		LOG << "PlanManager: no space for operators" << endl;
		return -1;
	}

	project -> store = 0;
	project -> instOp = 0;
	
	if ((rc = copy_schema (project, child)) != 0)
		return rc;
	
	project -> bStream = child -> bStream;	
	project -> numOutputs = 0;	
	project -> numInputs = 1;
	project -> inputs [0] = child;

	if ((rc = addOutput (child, project)) != 0)
		return rc;
	
	project -> store = 0;
	project -> u.PROJECT.outSyn = 0;
	
	for (unsigned int a = 0 ; a < project -> numAttrs ; a++) {
		projExpr = new_expr ();		
		if (!projExpr) {
			LOG << "PlanManager: no space for expressions" << endl;
			return -1;
		}
		
		projExpr -> kind = ATTR_REF;
		projExpr -> type = project -> attrTypes [a];
		projExpr -> u.attr.input = 0;
		projExpr -> u.attr.pos = a;
		
		project -> u.PROJECT.projs [a] = projExpr;
	}
	
	return 0;
}

int PlanManagerImpl::mk_project (Logical::Operator *logPlan,
								 Physical::Operator **phyChildPlans,
								 Physical::Operator *&phyPlan)
{	
	int rc;
	unsigned int numProjExprs;
	Physical::Expr *projExpr;
	Physical::Operator *project;
	
	ASSERT (logPlan);
	ASSERT (logPlan -> numInputs == 1);
	ASSERT (logPlan -> kind == Logical::LO_PROJECT);
	
	// new project operator
	project = new_op (Physical::PO_PROJECT);
	if (!project) {
		LOG << "PlanManager: no space for operators" << endl;
		return -1;
	}
	
	project -> store = 0;
	project -> instOp = 0;
	project -> u.PROJECT.outSyn = 0;
	
	// project expressions
	numProjExprs = logPlan -> numOutAttrs;
	ASSERT (numProjExprs < MAX_ATTRS);
	for (unsigned int p = 0 ; p < numProjExprs ; p++) {
		projExpr =  transformExpr (logPlan -> u.PROJECT.projExprs[p],								   
								   logPlan);
		if (!projExpr)
			return -1;
		
		project -> u.PROJECT.projs [p] = projExpr;		
	}
	
	// output schema
	project -> numAttrs = numProjExprs;
	for (unsigned int a = 0 ; a < numProjExprs ; a++) {		
		project -> attrTypes [a] = project -> u.PROJECT.projs[a] -> type;		
		
		if (project -> attrTypes [a] == INT) {
			project -> attrLen [a] = INT_SIZE;
		}
		else if (project -> attrTypes [a] == FLOAT) {
			project -> attrLen [a] = FLOAT_SIZE;
		}
		else if (project -> attrTypes [a] == BYTE) {
			project -> attrLen [a] = BYTE_SIZE;
		}
		else {
			// We don't allow expression over char types.  So the
			// project should just be an attribute reference.
			ASSERT (project -> attrTypes [a] == CHAR);
			ASSERT (project -> u.PROJECT.projs[a]->kind == ATTR_REF);
			ASSERT (project -> u.PROJECT.projs[a]->u.attr.input == 0);			
			ASSERT (project -> u.PROJECT.projs[a]->u.attr.pos <
					phyChildPlans [0] -> numAttrs);
			
			project -> attrLen [a] =
				phyChildPlans [0] ->
				attrLen [ project -> u.PROJECT.projs[a]->u.attr.pos ];			
		}
	}

	// output is a stream iff input is a stream
	project -> bStream = phyChildPlans [0] -> bStream;

	// output
	project -> numOutputs = 0;
	
	// input
	project -> numInputs = 1;
	project -> inputs [0] = phyChildPlans [0];
	if ((rc = addOutput (phyChildPlans [0], project)) != 0)
		return rc;

	phyPlan = project;
	
	return 0;	
}

int PlanManagerImpl::mk_join (Logical::Operator *logPlan,
							  Physical::Operator **phyChildPlans,
							  Physical::Operator *&phyPlan)
{
	int rc;
	Physical::Operator *join;
	
	ASSERT (logPlan);
	ASSERT (logPlan -> numInputs == 2);
	ASSERT (logPlan -> kind == Logical::LO_CROSS);
	
	// new operator
	join = new_op (Physical::PO_JOIN);
	if (!join) {
		LOG << "PlanManager: no space for operators" << endl;
		return -1;
	}
	
	join -> store = 0;
	join -> instOp = 0;
	join -> u.JOIN.innerSyn = 0;
	join -> u.JOIN.outerSyn = 0;
	join -> u.JOIN.joinSyn = 0;
	
	// schema = concatenation of schemas of the inputs
	join -> numAttrs = 0;
	
	if ((rc = append_schema (join, phyChildPlans [0])) != 0)
		return rc;
	if ((rc = append_schema (join, phyChildPlans [1])) != 0)
		return rc;
	
	join -> u.JOIN.numOuterAttrs = phyChildPlans [0] -> numAttrs;
	join -> u.JOIN.numInnerAttrs = phyChildPlans [1] -> numAttrs;
	ASSERT (join -> u.JOIN.numOuterAttrs +
			join -> u.JOIN.numInnerAttrs ==
			join -> numAttrs);
	
	// output is a stream iff both inputs are streams
	join -> bStream = (phyChildPlans [0] -> bStream &&
					   phyChildPlans [1] -> bStream);
	
	// outputs:
	join -> numOutputs = 0;

	// inputs;
	join -> numInputs = 2;
	join -> inputs [0] = phyChildPlans [0];
	join -> inputs [1] = phyChildPlans [1];
	
	if ((rc = addOutput (phyChildPlans [0], join)) != 0)
		return rc;
	if ((rc = addOutput (phyChildPlans [1], join)) != 0)
		return rc;
	
	// join predicate is set to (null).  It will be later [[ Explain ]]
	join -> u.JOIN.pred = 0;

	phyPlan = join;
	
	return 0;
}

int PlanManagerImpl::mk_str_join (Logical::Operator *logPlan,
								  Physical::Operator **phyChildPlans,
								  Physical::Operator *&phyPlan)
{
	int rc;
	Physical::Operator *strJoin;

	ASSERT (logPlan);
	ASSERT (logPlan -> numInputs == 2);
	ASSERT (logPlan -> kind == Logical::LO_STREAM_CROSS);
	
	// new operator
	strJoin = new_op(Physical::PO_STR_JOIN);
	if (!strJoin) {
		LOG << "PlanManager: no space for operators" << endl;
		return -1;
	}
	
	strJoin -> store = 0;
	strJoin -> instOp = 0;
	strJoin -> u.STR_JOIN.innerSyn = 0;
	
	// schema = concatenation of schemas of inputs and outputs
	strJoin -> numAttrs = 0;

	if ((rc = append_schema (strJoin, phyChildPlans [0])) != 0)
		return rc;
	if ((rc = append_schema (strJoin, phyChildPlans [1])) != 0)
		return rc;
	
	strJoin -> u.STR_JOIN.numOuterAttrs = phyChildPlans [0] -> numAttrs;	
	strJoin -> u.STR_JOIN.numInnerAttrs = phyChildPlans [1] -> numAttrs;
	ASSERT (strJoin -> u.STR_JOIN.numOuterAttrs +
			strJoin -> u.STR_JOIN.numInnerAttrs ==
			strJoin -> numAttrs);	
	
	// output of a stream join is always a stream
	strJoin -> bStream = true;

	// outputs :
	strJoin -> numOutputs = 0;

	// inputs:
	strJoin -> numInputs = 2;
	strJoin -> inputs [0] = phyChildPlans [0];
	strJoin -> inputs [1] = phyChildPlans [1];

	if ((rc = addOutput (phyChildPlans [0], strJoin)) != 0)
		return rc;
	if ((rc = addOutput (phyChildPlans [1], strJoin)) != 0)
		return rc;
	
	// join predicate is currently null [[Explain ]]
	strJoin -> u.STR_JOIN.pred = 0;

	phyPlan = strJoin;
	return 0;
}

int PlanManagerImpl::mk_group_aggr (Logical::Operator *logPlan,
									Physical::Operator **phyChildPlans,
									Physical::Operator *&phyPlan)
{
	int rc;
	unsigned int numGroupAttrs;
	unsigned int numAggrAttrs;
	Physical::Operator *gaOp;
	
	ASSERT (logPlan);
	ASSERT (logPlan -> numInputs == 1);
	ASSERT (logPlan -> kind == Logical::LO_GROUP_AGGR);
	
	// new operator
	gaOp = new_op (Physical::PO_GROUP_AGGR);
	if (!gaOp) {
		LOG << "PlanManager: no space for operators" << endl;
		return -1;
	}

	gaOp -> store = 0;
	gaOp -> instOp = 0;
	
	// grouping attributes
	numGroupAttrs = gaOp -> u.GROUP_AGGR.numGroupAttrs =
		logPlan -> u.GROUP_AGGR.numGroupAttrs;
	
	for (unsigned int g = 0 ; g < numGroupAttrs ; g++) {
		gaOp -> u.GROUP_AGGR.groupAttrs [g] =
			transformAttr (logPlan -> u.GROUP_AGGR.groupAttrs [g],
						   logPlan);
	}
	
	// aggr. attributes
	numAggrAttrs = gaOp -> u.GROUP_AGGR.numAggrAttrs =
		logPlan -> u.GROUP_AGGR.numAggrAttrs;
	
	for (unsigned int a = 0 ; a < numAggrAttrs ; a++) {
		gaOp -> u.GROUP_AGGR.fn [a] =
			logPlan -> u.GROUP_AGGR.fn [a];
		
		if (gaOp -> u.GROUP_AGGR.fn [a] != COUNT) {
			gaOp -> u.GROUP_AGGR.aggrAttrs [a] =
				transformAttr (logPlan -> u.GROUP_AGGR.aggrAttrs [a],
							   logPlan);
		}
		
		// COUNT is attr. independent
		else {
			gaOp -> u.GROUP_AGGR.aggrAttrs [a].input = 0;
			gaOp -> u.GROUP_AGGR.aggrAttrs [a].pos = 0;
		}
	}
	
	// output schema = concatenation of grouping and aggr. attributes
	gaOp -> numAttrs = numGroupAttrs + numAggrAttrs;
	
	// The type information for grouping attributes can be copied over
	// from the type information of the child operator.
	for (unsigned int a = 0 ; a < numGroupAttrs ; a++) {
		gaOp -> attrTypes [a] =
			getAttrType (gaOp -> u.GROUP_AGGR.groupAttrs [a],
						 phyChildPlans [0]);
		
		gaOp -> attrLen [a] =
			getAttrLen (gaOp -> u.GROUP_AGGR.groupAttrs [a],
						phyChildPlans [0]);
	}
	
	// The type information of an aggregated attribute can be derived from
	// the aggregating function and the type information of the aggregated
	// attribute
	for (unsigned int a = numGroupAttrs ; a < gaOp -> numAttrs ; a++) {
		Physical::Attr aggrAttr;
		AggrFn aggrFn;
		Type aggrAttrType;
		
		aggrAttr = gaOp -> u.GROUP_AGGR.aggrAttrs [a - numGroupAttrs];		
		aggrAttrType = getAttrType (aggrAttr, phyChildPlans [0]);
		aggrFn = gaOp -> u.GROUP_AGGR.fn [a - numGroupAttrs];

		gaOp -> attrTypes [a] = getOutputType (aggrFn, aggrAttrType);
		
		if (gaOp -> attrTypes [a] == INT) {
			gaOp -> attrLen [a] = INT_SIZE;
		}
		
		else if (gaOp -> attrTypes [a] == FLOAT) {
			gaOp -> attrLen [a] = FLOAT_SIZE;
		}
		
		else if(gaOp -> attrTypes [a] == BYTE) {
			gaOp -> attrLen [a] = BYTE_SIZE;
		}
		
		else {
			// We can't ever produce a char out of aggregation
			ASSERT (0);
		}
	}

	// Output of gby aggregation is never a stream (well, almost)
	gaOp -> bStream = false;

	// outputs:
	gaOp -> numOutputs = 0;

	// input:
	gaOp -> numInputs = 1;
	gaOp -> inputs [0] = phyChildPlans [0];

	if ((rc = addOutput (phyChildPlans [0], gaOp)) != 0)
		return rc;

	phyPlan = gaOp;

	return 0;
}

int PlanManagerImpl::mk_distinct (Logical::Operator *logPlan,
								  Physical::Operator **phyChildPlans,
								  Physical::Operator *&phyPlan)
{
	int rc;
	Physical::Operator *distinct;
	
	ASSERT (logPlan);
	ASSERT (logPlan -> numInputs == 1);
	ASSERT (logPlan -> kind == Logical::LO_DISTINCT);
	
	// new operator
	distinct = new_op(Physical::PO_DISTINCT);
	if (!distinct) {
		LOG << "PlanManagerImpl: no space for operators" << endl;
		return -1;
	}

	distinct -> store = 0;
	distinct -> instOp = 0;
	
	// output schema = input schema
	if ((rc = copy_schema (distinct, phyChildPlans [0])) != 0)
		return rc;
	
	// output is a stream iff input is a stream
	distinct -> bStream = phyChildPlans [0] -> bStream;
	
	// output:
	distinct -> numOutputs = 0;
	
	// input:
	distinct -> numInputs = 1;
	distinct -> inputs [0] = phyChildPlans [0];
	
	if ((rc = addOutput (phyChildPlans [0], distinct)) != 0)
		return rc;
	
	phyPlan = distinct;
	
	return 0;
}

int PlanManagerImpl::mk_istream (Logical::Operator *logPlan,
						Physical::Operator **phyChildPlans,
						Physical::Operator *&phyPlan)
{
	int rc;
	Physical::Operator *istream;
	
	ASSERT (logPlan);
	ASSERT (logPlan -> numInputs == 1);
	ASSERT (logPlan -> kind == Logical::LO_ISTREAM);

	// new operator
	istream = new_op (Physical::PO_ISTREAM);
	if (!istream) {
		LOG << "PlanManagerImpl: no space for operators" << endl;
		return -1;
	}

	istream -> store = 0;
	istream -> instOp = 0;
	
	// output schema = input schema
	if ((rc = copy_schema (istream, phyChildPlans [0])) != 0)
		return rc;

	// output is always a stream
	istream -> bStream = true;

	istream -> numOutputs = 0;

	// input:
	istream -> numInputs = 1;
	istream -> inputs [0] = phyChildPlans [0];
	
	if ((rc = addOutput (phyChildPlans [0], istream)) != 0)
		return rc;
	
	phyPlan = istream;
	
	return 0;
}

int PlanManagerImpl::mk_dstream (Logical::Operator *logPlan,
						Physical::Operator **phyChildPlans,
						Physical::Operator *&phyPlan)
{
	int rc;
	Physical::Operator *dstream;
	
	ASSERT (logPlan);
	ASSERT (logPlan -> numInputs == 1);
	ASSERT (logPlan -> kind == Logical::LO_DSTREAM);

	// new operator
	dstream = new_op (Physical::PO_DSTREAM);
	if (!dstream) {
		LOG << "PlanManagerImpl: no space for operators" << endl;
		return -1;
	}

	dstream -> store = 0;
	dstream -> instOp = 0;
	
	// output schema = input schema
	if ((rc = copy_schema (dstream, phyChildPlans [0])) != 0)
		return rc;

	// output is always a stream
	dstream -> bStream = true;

	dstream -> numOutputs = 0;

	// input:
	dstream -> numInputs = 1;
	dstream -> inputs [0] = phyChildPlans [0];
	
	if ((rc = addOutput (phyChildPlans [0], dstream)) != 0)
		return rc;
	
	phyPlan = dstream;
	
	return 0;
}

int PlanManagerImpl::mk_rstream (Logical::Operator *logPlan,
						Physical::Operator **phyChildPlans,
						Physical::Operator *&phyPlan)
{
	int rc;
	Physical::Operator *rstream;
	
	ASSERT (logPlan);
	ASSERT (logPlan -> numInputs == 1);
	ASSERT (logPlan -> kind == Logical::LO_RSTREAM);

	// new operator
	rstream = new_op (Physical::PO_RSTREAM);
	if (!rstream) {
		LOG << "PlanManagerImpl: no space for operators" << endl;
		return -1;
	}

	rstream -> store = 0;
	rstream -> instOp = 0;
	
	// output schema = input schema
	if ((rc = copy_schema (rstream, phyChildPlans [0])) != 0)
		return rc;

	// output is always a stream
	rstream -> bStream = true;

	rstream -> numOutputs = 0;

	// input:
	rstream -> numInputs = 1;
	rstream -> inputs [0] = phyChildPlans [0];
	
	if ((rc = addOutput (phyChildPlans [0], rstream)) != 0)
		return rc;
	
	phyPlan = rstream;
	
	return 0;
}

int PlanManagerImpl::mk_union (Logical::Operator *logPlan,
							   Physical::Operator **phyChildPlans,
							   Physical::Operator *&phyPlan)
{
	int rc;
	Physical::Operator *unionOp;
	Physical::Operator *leftPlan, *rightPlan;
	
	ASSERT (logPlan);
	ASSERT (logPlan -> numInputs == 2);
	ASSERT (logPlan -> kind == Logical::LO_UNION);
	
	unionOp = new_op (Physical::PO_UNION);
	if (!unionOp) {
		LOG << "PlanManagerImpl: no space for operators" << endl;
		return -1;
	}

	leftPlan = phyChildPlans [0];
	rightPlan = phyChildPlans [1];
	
	// output schema = one of the input schema
	ASSERT (leftPlan -> numAttrs == rightPlan -> numAttrs);		
	unionOp -> numAttrs = leftPlan -> numAttrs;
	
	for (unsigned int a = 0 ; a < unionOp -> numAttrs ; a++) {
		
		ASSERT (leftPlan -> attrTypes [a] == rightPlan -> attrTypes [a]);		
		unionOp -> attrTypes [a] = leftPlan -> attrTypes [a];
		
		if (unionOp -> attrTypes [a] == CHAR) {
			unionOp -> attrLen [a] =
				(leftPlan -> attrLen [a] > rightPlan -> attrLen [a])?
				leftPlan -> attrLen [a] :
				rightPlan -> attrLen [a];			
		}
		
		else {
			ASSERT (leftPlan -> attrLen [a] == rightPlan -> attrLen[a]);
			unionOp -> attrLen [a] = leftPlan -> attrLen [a];
		}
	}
	
	// output is a bstream iff both inputs are streams
	unionOp -> bStream = (leftPlan -> bStream &&
						  rightPlan -> bStream);	

	unionOp -> numOutputs = 0;
	unionOp -> numInputs = 2;
	unionOp -> inputs [0] = leftPlan;
	unionOp -> inputs [1] = rightPlan;
	
	if ((rc = addOutput (leftPlan, unionOp)) != 0)
		return rc;
	
	if ((rc = addOutput (rightPlan, unionOp)) != 0)
		return rc;

	unionOp -> u.UNION.outSyn = 0;
	
	phyPlan = unionOp;
	
	return 0;
}

int PlanManagerImpl::mk_except (Logical::Operator *logPlan,
								Physical::Operator **phyChildPlans,
								Physical::Operator *&phyPlan)
{
	int rc;
	Physical::Operator *except;
	Physical::Operator *leftPlan, *rightPlan;
	
	ASSERT (logPlan);
	ASSERT (logPlan -> numInputs == 2);
	ASSERT (logPlan -> kind == Logical::LO_EXCEPT);
	
	except = new_op (Physical::PO_EXCEPT);
	if (!except) {
		LOG << "PlanManagerImpl: no space for operators" << endl;
		return -1;
	}
	
	leftPlan = phyChildPlans [0];
	rightPlan = phyChildPlans [1];
	
	// output schema = one of the input schema
	ASSERT (leftPlan -> numAttrs == rightPlan -> numAttrs);		
	except -> numAttrs = leftPlan -> numAttrs;
	
	for (unsigned int a = 0 ; a < except -> numAttrs ; a++) {
		
		ASSERT (leftPlan -> attrTypes [a] == rightPlan -> attrTypes [a]);		
		except -> attrTypes [a] = leftPlan -> attrTypes [a];
		
		if (except -> attrTypes [a] == CHAR) {
			except -> attrLen [a] =
				(leftPlan -> attrLen [a] > rightPlan -> attrLen [a])?
				leftPlan -> attrLen [a] :
				rightPlan -> attrLen [a];			
		}
		
		else {
			ASSERT (leftPlan -> attrLen [a] == rightPlan -> attrLen[a]);
			except -> attrLen [a] = leftPlan -> attrLen [a];
		}
	}

	
	except -> bStream = false;

	except -> numOutputs = 0;
	except -> numInputs = 2;
	except -> inputs [0] = leftPlan;
	except -> inputs [1] = rightPlan;

	if ((rc = addOutput (leftPlan, except)) != 0)
		return rc;

	if ((rc = addOutput (rightPlan, except)) != 0)
		return rc;
	
	except -> u.EXCEPT.countSyn = 0;
	except -> u.EXCEPT.countStore = 0;

	phyPlan = except;

	return 0;
}

int PlanManagerImpl::mk_qry_src (Physical::Operator *phyPlan,
								 Physical::Operator *&qrySrc)
{
	int rc;
	ASSERT (phyPlan);

	qrySrc = new_op (Physical::PO_QUERY_SOURCE);
	if (!qrySrc) {
		LOG << "PlanManagerImpl:: no space for operators" << endl;
		return -1;
	}

	qrySrc -> store = 0;
	qrySrc -> instOp = 0;
	
	// schema = input schema
	if ((rc = copy_schema (qrySrc, phyPlan)) != 0)
		return rc;
	
	qrySrc -> bStream = phyPlan -> bStream;
	qrySrc -> numOutputs = 0;
	qrySrc -> numInputs = 1;
	qrySrc -> inputs [0] = phyPlan;
	
	if ((rc = addOutput (phyPlan, qrySrc)) != 0)
		return rc;

	return 0;
}

int PlanManagerImpl::mk_output (Physical::Operator *phyPlan,
								Physical::Operator *&output)
{
	int rc;
	
	ASSERT (phyPlan);
	
	output = new_op (Physical::PO_OUTPUT);
	if (!output) {
		LOG << "PlanManagerImpl:: no space for operators" << endl;
		return -1;
	}
	
	output -> store = 0;
	output -> instOp = 0;
	
	// schema = input schema
	if ((rc = copy_schema (output, phyPlan)) != 0)
		return rc;
	
	output -> bStream = phyPlan -> bStream;
	output -> numOutputs = 0;
	output -> numInputs = 1;
	output -> inputs [0] = phyPlan;

	
	if ((rc = addOutput (phyPlan, output)) != 0)
		return rc;
	
	return 0;
}
	
int PlanManagerImpl::mk_sink (Operator *op, Operator *&sink)
{
	int rc;
	
	sink = new_op (PO_SINK);
	if (!sink) {
		LOG << "PlanManagerImpl: no space for operators" << endl;
		return -1;
	}

	sink -> store = 0;
	sink -> instOp = 0;

	if ((rc = copy_schema (sink, op)) != 0)
		return rc;

	sink -> bStream = op -> bStream;
	sink -> numOutputs = 0;
	sink -> numInputs = 1;
	sink -> inputs [0] = op;

	if ((rc = addOutput (op, sink)) != 0)
		return rc;
	
	return 0;
}

static int copy_schema (Physical::Operator *dest, Physical::Operator *src)
{
	
	ASSERT (src -> numAttrs > 0);	
	dest -> numAttrs = src -> numAttrs;
	
	for (unsigned int a = 0 ; a < src -> numAttrs ; a++) {
		dest -> attrTypes [a] = src -> attrTypes [a];
		dest -> attrLen [a] = src -> attrLen [a];
	}
	
	return 0;
}

int PlanManagerImpl::addOutput (Operator *child, Operator *parent)
{
	ASSERT (child);
	
	if (child -> numOutputs >= MAX_OUT_BRANCHING) {
		LOG << "PlanManagerImpl: too high branching" << endl;
		return -1;
	}

	child -> outputs [child -> numOutputs ++] = parent;
	return 0;
}

static int append_schema (Operator *dest, Operator *src)
{
	ASSERT (src -> numAttrs > 0);	
	ASSERT (src -> numAttrs + dest -> numAttrs < MAX_ATTRS);
	
	for (unsigned int a = 0 ; a < src -> numAttrs ; a++) {
		dest -> attrTypes [ dest -> numAttrs ] =
			src -> attrTypes [a];
		dest -> attrLen [ dest -> numAttrs ] =
			src -> attrLen [a];
		dest -> numAttrs ++;
	}
	
	return 0;
}

static bool operator == (const Logical::Attr& attr1,
						 const Logical::Attr& attr2)
{
	if (attr1.kind != attr2.kind)
		return false;
	
	switch (attr1.kind) {
	case Logical::NAMED:
		
		return ((attr1.u.NAMED.varId == attr2.u.NAMED.varId)     &&
				(attr1.u.NAMED.tableId == attr2.u.NAMED.tableId) &&
				(attr1.u.NAMED.attrId == attr2.u.NAMED.attrId));
		
	case Logical::AGGR:
		
		return ((attr1.u.AGGR.varId == attr2.u.AGGR.varId)       &&
				(attr1.u.AGGR.tableId == attr2.u.AGGR.tableId)   &&
				(attr1.u.AGGR.attrId == attr2.u.AGGR.attrId)     &&
				(attr1.u.AGGR.fn == attr2.u.AGGR.fn));
		
	case Logical::UNNAMED:
	default:
		return false;		
	}
	
	// never comes
	return false;
}

Physical::Attr PlanManagerImpl::transformAttr (Logical::Attr l_attr,
											   Logical::Operator *op)
{
	Logical::Operator *childOp;
	Physical::Attr p_attr;
	
	for (unsigned int i = 0 ; i < op -> numInputs ; i++) {
		
		childOp = op -> inputs [i];
		ASSERT (childOp);
		
		for (unsigned int a = 0 ; a < childOp -> numOutAttrs ; a++) {
			if (l_attr == childOp -> outAttrs [a]) {
				p_attr.input = i;
				p_attr.pos = a;
				
				return p_attr;
			}
		}
	}
	
	// should never come here
	ASSERT (0);
	return p_attr;
}

Physical::BExpr *PlanManagerImpl::transformBExpr (Logical::BExpr l_bexpr,
												  Logical::Operator *l_op)
{
	Physical::Expr *left;
	Physical::Expr *right;
	BExpr *p_bexpr;
	
	// transform the left and right (arithmetic) expressions
	left = transformExpr (l_bexpr.left, l_op);
	right = transformExpr (l_bexpr.right, l_op);	
	if (!left || !right)
		return 0;
	
	// allocate space.
	p_bexpr = new_bexpr();
	if (!p_bexpr)
		return 0;
	
	p_bexpr -> left = left;
	p_bexpr -> right = right;
	p_bexpr -> op = l_bexpr.op;
	p_bexpr -> next = 0;

	return p_bexpr;
}

Physical::Expr *PlanManagerImpl::transformExpr (Logical::Expr *l_expr,
												Logical::Operator *l_op)
{
	Physical::Expr *p_expr;

	p_expr = new_expr();
	if (!p_expr)
		return 0;

	p_expr -> type = l_expr -> type;
	
	if (l_expr -> kind == Logical::CONST_VAL) {
		p_expr -> kind = Physical::CONST_VAL;
		
		if (l_expr -> type == INT)
			p_expr -> u.ival = l_expr -> u.ival;
		
		else if (l_expr -> type == FLOAT)
			p_expr -> u.fval = l_expr -> u.fval;

		else if (l_expr -> type == BYTE)
			p_expr -> u.bval = l_expr -> u.bval;

		else {
			ASSERT (l_expr -> type == CHAR);
			p_expr -> u.sval = copyString (l_expr -> u.sval);
		}
	}
	
	else if (l_expr -> kind == Logical::ATTR_REF) {
		p_expr -> kind = Physical::ATTR_REF;
		p_expr -> u.attr = transformAttr (l_expr ->u.attr, l_op);
	}
	
	else {
		ASSERT (l_expr -> kind == Logical::COMP_EXPR);
		p_expr -> kind =  Physical::COMP_EXPR;
		p_expr -> u.COMP_EXPR.op = l_expr -> u.COMP_EXPR.op;
		p_expr -> u.COMP_EXPR.left =
			transformExpr (l_expr->u.COMP_EXPR.left, l_op);
		p_expr -> u.COMP_EXPR.right =
			transformExpr (l_expr->u.COMP_EXPR.right, l_op);			
		
		if (!p_expr -> u.COMP_EXPR.left || !p_expr -> u.COMP_EXPR.right)
			return 0;
	}
	
	return p_expr;
}

static Type getAttrType (Physical::Attr attr, Physical::Operator *child)
{
	unsigned int pos;
	
	pos = attr.pos;

	ASSERT (child);
	ASSERT (pos < child -> numAttrs);

	return child -> attrTypes [pos];	
}

static unsigned int getAttrLen (Physical::Attr attr,
								Physical::Operator *child)
{
	unsigned int pos;
	
	pos = attr.pos;
	
	ASSERT (child);
	ASSERT (pos < child -> numAttrs);
	
	return child -> attrLen [pos];
}

void PlanManagerImpl::init_ops ()
{
	usedOps = 0;
	freeOps = ops;
	
	for (unsigned int o = 1 ; o < MAX_OPS ; o++) {
		ops [o].prev = (ops + o - 1);
		ops [o-1].next = (ops + o);
		ops [o].id = o;
	}
	
	ops [MAX_OPS - 1].next = 0;
	ops [0].prev = 0;
	ops [0].id = 0;
	
	return;
}

/**
 * Pick the first operator in the list of free operators (freeOps) and
 * move it to the beginning of the list of used operators (usedOps).
 * Return this operator as the output.
 */ 
Operator *PlanManagerImpl::new_op (OperatorKind kind)
{
	Operator *op;	

	// We don't have any free operators
	if (!freeOps)
		return 0;	
	
	// Get the free operator from the beginning of the pool. 
	op = freeOps;
	
	// update the list of free ops
	freeOps = freeOps -> next;
	ASSERT (freeOps -> prev == op);
	freeOps -> prev = 0;
	
	// update the list of used ops
	ASSERT (!op -> prev);
	op -> next = usedOps;
	if (usedOps)
		usedOps -> prev = op;
	usedOps = op;	
	
	op -> kind = kind;
	op -> numAttrs = 0;
	op -> numOutputs = 0;
	op -> numInputs = 0;	
	op -> instOp = 0;
	
	return op;
}

/**
 * Remove the operator from usedOps list and add to to freeOps list.
 */
void PlanManagerImpl::free_op (Operator *op)
{
	
	// Oops! we are trying to free an null operator
	ASSERT (op);

	// op should be currently in use - so usedOps can't be empty
	ASSERT (usedOps);	
	
#ifdef _DM_
	Operator *t_op = usedOps;
	while (t_op)
		if (t_op == op)
			break;
		else
			t_op = t_op -> next;	
	ASSERT (t_op);
#endif

	// Update usedOps linked list
	if (op -> prev) {
		op -> prev -> next = op -> next;
	}

	// otherwise op should be in the beginning of usedOps
	else {
		ASSERT (usedOps == op);
		usedOps = op -> next;
	}
	
	
	if (op -> next)
		op -> next -> prev = op -> prev;
	
	// Add op to beginning of freeOps
	op -> next = freeOps;
	op -> prev = 0;

	if (freeOps) {	
		ASSERT (!freeOps -> prev);				
		freeOps -> prev = op;
	}
	
	freeOps = op;
	
	return;
}


Expr *PlanManagerImpl::new_expr ()
{
	Expr *e;

	if (numExprs >= MAX_EXPRS)
		return 0;

	e = exprs + numExprs;
	numExprs++;
	
	return e;
}

BExpr *PlanManagerImpl::new_bexpr()
{
	BExpr *b;
	
	if (numBExprs >= MAX_EXPRS)
		return 0;

	b = bexprs + numBExprs;
	numBExprs ++;

	return b;
}

char *PlanManagerImpl::copyString (const char *s)
{
	return strdup(s);
}

