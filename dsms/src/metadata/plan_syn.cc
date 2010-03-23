/**
 * @file      plan_syn.cc
 * @date      Aug. 25, 2004
 * @brief     Definition of PlanManagerImpl routines which deal with
 *            synopses
 */

#ifndef _PLAN_MGR_IMPL_
#include "metadata/plan_mgr_impl.h"
#endif

#ifndef _DEBUG_
#include "common/debug.h"
#endif

using namespace Physical;
using namespace Metadata;


/**
 * Allocate a new synopsis from the pool of available synopses
 */

Synopsis *PlanManagerImpl::new_syn (SynopsisKind kind)
{
	Synopsis *syn;
	
	if (numSyns >= MAX_SYNS)
		return 0;

	syn = syns + numSyns;
	
	syn -> id = numSyns;
	syn -> kind = kind;
	
	numSyns ++;
	
	return syn;
}


int PlanManagerImpl::add_syn_rel_source (Operator *op)
{
	Synopsis *outSyn;
	
	ASSERT (op);
	ASSERT (op -> kind == PO_RELN_SOURCE);
	
	outSyn = new_syn (REL_SYN);
	outSyn -> ownOp = op;
	
	op -> u.RELN_SOURCE.outSyn = outSyn;
	
	return 0;
}

/**
 * Project requires a synopsis if can produce MINUS elements.  The
 * synopsis is required to produce the MINUS elements (we maintain the
 * constraint that the PLUS and the corresponding MINUS elements point to
 * the same data tuple)
 */

int PlanManagerImpl::add_syn_proj (Operator *op)
{
	Synopsis *outSyn;

	ASSERT (op);
	ASSERT (op -> kind == PO_PROJECT);
	
	if (op -> bStream) {		
		op -> u.PROJECT.outSyn = 0;		
	}
	
	else {
		outSyn = new_syn (LIN_SYN);
		if (!outSyn)
			return -1;
		
		op -> u.PROJECT.outSyn = outSyn;
		outSyn -> ownOp = op;
	}
	
	return 0;
}
		

/**
 * A join needs a relation synopsis for its outer and inner inputs.  If
 * the join produces a relation (i.e., its output can contain MINUS
 * tuples) then it also needs a join synopsis for its output.  The join
 * synopsis is required to produce MINUS tuples.
 */ 

int PlanManagerImpl::add_syn_join (Operator *op)
{
	Synopsis *inner, *outer, *join;

	ASSERT (op);
	ASSERT (op -> kind == PO_JOIN);
	
	inner = new_syn (REL_SYN);	
	if (!inner)
		return -1;	
	
	outer = new_syn (REL_SYN);
	if (!outer)
		return -1;

	op -> u.JOIN.innerSyn = inner;
	inner -> ownOp = op;
	
	op -> u.JOIN.outerSyn = outer;
	outer -> ownOp = op;
	
	if (!op -> bStream) {
		join = new_syn (LIN_SYN);		
		
		if (!join)
			return -1;
		
		op -> u.JOIN.joinSyn = join;
		join -> ownOp = op;
	}
	
	else {
		op -> u.JOIN.joinSyn = 0;
	}
	
	return 0;
}

/**
 * A stream join requires a synopsis for its outer input.  No synopsis is
 * required for its inner & its join output.
 */

int PlanManagerImpl::add_syn_str_join (Operator *op)
{
	Synopsis *innerSyn;

	ASSERT (op);
	ASSERT (op -> kind == PO_STR_JOIN);

	innerSyn = new_syn (REL_SYN);
	if (!innerSyn)
		return -1;

	op -> u.STR_JOIN.innerSyn = innerSyn;
	innerSyn -> ownOp = op;
	
	return 0;
}

/**
 * A join needs a relation synopsis for its outer and inner inputs.  If
 * the join produces a relation (i.e., its output can contain MINUS
 * tuples) then it also needs a join synopsis for its output.  The join
 * synopsis is required to produce MINUS tuples.
 */ 

int PlanManagerImpl::add_syn_join_proj (Operator *op)
{
	Synopsis *inner, *outer, *join;

	ASSERT (op);
	ASSERT (op -> kind == PO_JOIN_PROJECT);
	
	inner = new_syn (REL_SYN);	
	if (!inner)
		return -1;	
	
	outer = new_syn (REL_SYN);
	if (!outer)
		return -1;

	op -> u.JOIN_PROJECT.innerSyn = inner;
	inner -> ownOp = op;
	
	op -> u.JOIN_PROJECT.outerSyn = outer;
	outer -> ownOp = op;
	
	if (!op -> bStream) {
		join = new_syn (LIN_SYN);		
		
		if (!join)
			return -1;
		
		op -> u.JOIN_PROJECT.joinSyn = join;
		join -> ownOp = op;
	}
	
	else {
		op -> u.JOIN_PROJECT.joinSyn = 0;
	}
	
	return 0;
}

/**
 * A stream join requires a synopsis for its outer input.  No synopsis is
 * required for its inner & its join output.
 */

int PlanManagerImpl::add_syn_str_join_proj (Operator *op)
{
	Synopsis *innerSyn;

	ASSERT (op);
	ASSERT (op -> kind == PO_STR_JOIN_PROJECT);

	innerSyn = new_syn (REL_SYN);
	if (!innerSyn)
		return -1;

	op -> u.STR_JOIN_PROJECT.innerSyn = innerSyn;
	innerSyn -> ownOp = op;

	return 0;
}

int PlanManagerImpl::add_syn_gby_aggr (Operator *op)
{
	Synopsis *inSyn, *outSyn;
	bool bInSynNeeded;
	
	ASSERT (op);
	ASSERT (op -> kind == PO_GROUP_AGGR);
	
	outSyn = new_syn (REL_SYN);
	if (!outSyn)
		return -1;

	op -> u.GROUP_AGGR.outSyn = outSyn;
	outSyn -> ownOp = op;
	
	// We need the inner synopsis iff one of the aggr. functions is a max
	// or min and the input is not a stream
	bInSynNeeded = false;
	if (!op -> inputs [0] -> bStream) {
		for (unsigned int f = 0 ; f < op -> u.GROUP_AGGR.numAggrAttrs &&
				 !bInSynNeeded ; f++) {
			if ((op -> u.GROUP_AGGR.fn [f] == MAX) ||
				(op -> u.GROUP_AGGR.fn [f] == MIN)) {
				bInSynNeeded = true;
			}
		}
	}
	
	if (bInSynNeeded) {		
		inSyn = new_syn (REL_SYN);
		if (!inSyn)
			return -1;
		
		op -> u.GROUP_AGGR.inSyn = inSyn;
		inSyn -> ownOp = op;
	}
	else {
		op -> u.GROUP_AGGR.inSyn = 0;
	}
	
	return 0;
}

int PlanManagerImpl::add_syn_distinct (Operator *op)
{
	Synopsis *outSyn;

	ASSERT (op);
	ASSERT (op -> kind == PO_DISTINCT);

	outSyn = new_syn (REL_SYN);
	if (!outSyn)
		return -1;

	op -> u.DISTINCT.outSyn = outSyn;
	outSyn -> ownOp = op;
	
	return 0;
}

int PlanManagerImpl::add_syn_row_win (Operator *op)
{
	Synopsis *winSyn;

	ASSERT (op);
	ASSERT (op -> kind == PO_ROW_WIN);

	winSyn = new_syn (WIN_SYN);
	if (!winSyn)
		return -1;
	
	op -> u.ROW_WIN.winSyn = winSyn;
	winSyn -> ownOp = op;

	return 0;
}

int PlanManagerImpl::add_syn_range_win (Operator *op)
{
	Synopsis *winSyn;

	ASSERT (op);
	ASSERT (op -> kind == PO_RANGE_WIN);

	winSyn = new_syn (WIN_SYN);
	if (!winSyn)
		return -1;
	
	op -> u.RANGE_WIN.winSyn = winSyn;
	winSyn -> ownOp = op;

	return 0;
}

int PlanManagerImpl::add_syn_partn_win (Operator *op)
{
	Synopsis *winSyn;
	
	ASSERT (op);
	ASSERT (op -> kind == PO_PARTN_WIN);
	
	winSyn = new_syn (PARTN_WIN_SYN);
	if (!winSyn)
		return -1;
		
	op -> u.PARTN_WIN.winSyn = winSyn;
	winSyn -> ownOp = op;
	
	return 0;
}

int PlanManagerImpl::add_syn_istream (Operator *op)
{
	Synopsis *nowSyn;
	
	ASSERT (op);
	ASSERT (op -> kind == PO_ISTREAM);
	
	nowSyn = new_syn (REL_SYN);
	if (!nowSyn)
		return -1;
	
	op -> u.ISTREAM.nowSyn = nowSyn;
	nowSyn -> ownOp = op;

	return 0;
}
		
int PlanManagerImpl::add_syn_dstream (Operator *op)
{
	Synopsis *nowSyn;
	
	ASSERT (op);
	ASSERT (op -> kind == PO_DSTREAM);
	
	nowSyn = new_syn (REL_SYN);
	if (!nowSyn)
		return -1;
	
	op -> u.DSTREAM.nowSyn = nowSyn;
	nowSyn -> ownOp = op;
	
	return 0;
}

int PlanManagerImpl::add_syn_rstream (Operator *op)
{
	Synopsis *inSyn;
	
	ASSERT (op);
	ASSERT (op -> kind == PO_RSTREAM);
	
	inSyn = new_syn (REL_SYN);
	if (!inSyn)
		return -1;
	
	op -> u.RSTREAM.inSyn = inSyn;
	inSyn -> ownOp = op;
	
	return 0;
}

int PlanManagerImpl::add_syn_union (Operator *op)
{
	Synopsis *outSyn;

	ASSERT (op);
	ASSERT (op -> kind == PO_UNION);
	
	outSyn = 0;
	if (!op -> bStream) {
		outSyn = new_syn (LIN_SYN);

		if (!outSyn)
			return -1;
		
		outSyn -> ownOp = op;
	}
	
	op -> u.UNION.outSyn = outSyn;
	
	return 0;
}

int PlanManagerImpl::add_syn_except (Operator *op)
{
	Synopsis *countSyn;
	Synopsis *outSyn;
	
	ASSERT (op);
	ASSERT (op -> kind == PO_EXCEPT);
	
	countSyn = new_syn (REL_SYN);
	if (!countSyn)
		return -1;
	countSyn -> ownOp = op;
	
	outSyn = new_syn (REL_SYN);
	if (!outSyn)
		return -1;
	outSyn -> ownOp = op;
	
	op -> u.EXCEPT.countSyn = countSyn;
	op -> u.EXCEPT.outSyn = outSyn;
	
	return 0;
}

int PlanManagerImpl::add_syn (Operator *op)
{
	ASSERT (op);

	switch (op -> kind) {

	case PO_SELECT: return 0;
		
	case PO_PROJECT: return add_syn_proj (op);
		
	case PO_JOIN: return add_syn_join (op);
		
	case PO_STR_JOIN: return add_syn_str_join (op);
		
	case PO_JOIN_PROJECT: return add_syn_join_proj (op);
		
	case PO_STR_JOIN_PROJECT: return add_syn_str_join_proj (op);
		
	case PO_GROUP_AGGR: return add_syn_gby_aggr (op);
		
	case PO_DISTINCT: return add_syn_distinct (op);
		
	case PO_ROW_WIN: return add_syn_row_win (op);
		
	case PO_RANGE_WIN: return add_syn_range_win (op);		
		
	case PO_PARTN_WIN: return add_syn_partn_win (op);
		
	case PO_ISTREAM: return add_syn_istream (op);
		
	case PO_DSTREAM: return add_syn_dstream (op);
		
	case PO_RSTREAM: return add_syn_rstream (op);
		
	case PO_STREAM_SOURCE: return 0;
		
	case PO_RELN_SOURCE: return add_syn_rel_source (op);
		
	case PO_UNION: return add_syn_union (op);
		
	case PO_EXCEPT: return add_syn_except (op);
		
	case PO_QUERY_SOURCE: return 0;
		
	case PO_OUTPUT: return 0;
		
	case PO_SINK: return 0;

	case PO_SS_GEN: return 0;
	default:
		break;
	};
	
	// should never come
	ASSERT (0);
	return -1;
}

int PlanManagerImpl::add_syn ()
{
	int rc;
	Operator *op;

	// Iterate through all ops and add synopses
	op = usedOps;
	while (op) {
		if ((rc = add_syn (op)) != 0)
			return rc;
		
		op = op -> next;
	}
	
	return 0;	
}

int PlanManagerImpl::add_syn_mon (Operator *&plan,
								  Operator **opList,
								  unsigned int &numOps)
{
	int rc;
	Operator *op;
	
	for (unsigned int o = 0 ; o < numOps ; o++) {
		op = opList [o];

		if ((rc = add_syn (op)) != 0)
			return rc;
	}

	return 0;
}
