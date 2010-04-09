#ifndef _DEBUG_
#include "common/debug.h"
#endif

#ifndef _PHY_OP_DEBUG_
#include "metadata/phy_op_debug.h"
#endif

#ifndef _PHY_STORE_
#include "metadata/phy_store.h"
#endif

#ifndef _PHY_SYN_
#include "metadata/phy_syn.h"
#endif


using namespace Physical;
using namespace std;

static ostream& operator << (ostream& out, AggrFn fn)
{
	switch (fn) {
	case SUM:   out << "SUM"; break;
	case MAX:   out << "MAX"; break;
	case MIN:   out << "MIN"; break;
	case AVG:   out << "AVG"; break;
	case COUNT: out << "COUNT"; break;
	default:    out << "ERR"; break;
	}
	
	return out;
}

static ostream& operator << (ostream& out, ArithOp op)
{
	switch(op) {
	case ADD: out << " + "; break;
	case SUB: out << " - "; break;
	case MUL: out << " * "; break;
	case DIV: out << " / "; break;
	default:
		break;
	}
	
	return out;
}

static ostream& operator << (ostream& out, CompOp op)
{
	switch (op) {
	case LT: out << " < "; break;
	case LE: out << " <= "; break;
	case GT: out << " > "; break;
	case GE: out << " >= "; break;
	case EQ: out << " == "; break;
	case NE: out << " != "; break;
	default:
		break;
	}
	
	return out;
}

static ostream& operator << (ostream& out, Type type)
{
	switch (type) {
	case INT: out << "Int"; break;
	case FLOAT: out << "Float"; break;
	case BYTE: out << "Byte"; break;
	case CHAR: out << "Char"; break;
	default:
		break;
	}
	return out;
}

ostream& Physical::operator << (ostream &out, Attr attr)
{
	out << "[" << attr.input << "," << attr.pos << "]";
	return out;
}

ostream& Physical::operator << (ostream &out, Expr *expr)
{
	switch (expr -> kind) {
	case CONST_VAL:		
		switch (expr -> type) {
		case INT:
			out << expr -> u.ival; break;
		case FLOAT:
			out << expr -> u.fval; break;
		case BYTE:
			out << expr -> u.bval; break;
		case CHAR:
			out << expr -> u.sval; break;
		default:
			break;
		}
		
		break;
		
	case ATTR_REF:
		
		out << expr -> u.attr;
		break;
		
	case COMP_EXPR:
		
		out << "(" 
			<< expr -> u.COMP_EXPR.left 
			<< expr -> u.COMP_EXPR.op
			<< expr -> u.COMP_EXPR.right
			<< ")";
		break;
		
	default:
		break;
	}
	
	return out;
}		

ostream& Physical::operator << (ostream& out, BExpr *bexpr)
{
	out << "("
		<< bexpr -> left
		<< bexpr -> op
		<< bexpr -> right
		<< ")";
	
	if (bexpr -> next)
		out << " and " << bexpr -> next;
	
	return out;
}

ostream& Physical::operator << (ostream& out, Operator *op)
{
	// Operator type:
	out << op -> id << ". ";
	
	switch (op -> kind) {
	case PO_SELECT:
		out << "Select";
		break;
	case PO_PROJECT:
		out << "Project";
		break;		
	case PO_JOIN:
		out << "Join";
		break;
	case PO_JOIN_PROJECT:
		out << "Join (+Project)";
		break;
		
	case PO_STR_JOIN:
		out << "StreamJoin";
		break;
		
	case PO_STR_JOIN_PROJECT:
		out << "StreamJoin (+Project)";
		break;
		
	case PO_ISTREAM:
		out << "Istream";
		break;
		
	case PO_DSTREAM:
		out << "Dstream";
		break;
		
	case PO_RSTREAM:
		out << "Rstream";
		break;
		
	case PO_STREAM_SOURCE:
		out << "StreamSource";
		break;
		
	case PO_ROW_WIN:
		out << "RowWin";
		break;
		
	case PO_RANGE_WIN:
		out << "RangeWin";
		break;
				
	case PO_GROUP_AGGR:
		out << "Groupby Aggr";
		break;
		
	case PO_DISTINCT:
		out << "Distinct";
		break;
		
	case PO_RELN_SOURCE:
		out << "RelationSource";
		break;
		
	case PO_PARTN_WIN:
		out << "PartitionWindow";
		break;

	case PO_QUERY_SOURCE:
		out << "QuerySource";
		break;

	case PO_OUTPUT:
		out << "Output";
		break;

	case PO_SINK:
		out << "Sink";
		break;
		
	case PO_UNION:
		out << "Union";
		break;
		
	case PO_EXCEPT:
		out << "Except";
		break;

	case PO_SS_GEN:
		out << "SSGen";
		break;
		
	default:
		out << "(Unknown)";
		break;
	}
	
	out << ": " << endl;
	
	// Stream/Reln
	out << "Output: ";	
	if (op -> bStream)
		out << "Stream";
	else
		out << "Relation";	
	out << endl;	
	
	// Schema information
	out << "Schema: <";
	
	for (unsigned int a = 0 ; a < op -> numAttrs ; a++) {
		
		out << op -> attrTypes [a];
		
		if (op -> attrTypes [a] == CHAR) {
			out << "("
				<< op -> attrLen [a]
				<< ")";
		}
		
		if (a < op -> numAttrs - 1)
			out << ", ";
	}
	out << ">" << endl;
	
	// Input information:
	out << "Input: ";
	
	for (unsigned int i = 0 ; i < op -> numInputs ; i++) {
		out << op -> inputs [i] -> id;
		
		if (i < op -> numInputs - 1)
			out << ", ";
	}
	out << endl;
	
	// Store information:
	out << "Store: ";
	if (op -> store)
		out << op -> store -> id << endl;
	else
		out << "(null)" << endl;
	
	switch (op -> kind) {
	case PO_SELECT:
		
		out << "Predicate: " << op -> u.SELECT.pred << endl;		
		break;
		
	case PO_PROJECT:
		
		out << "Projections: ";
		for (unsigned int p = 0 ; p < op -> numAttrs ; p++) {
			out << op -> u.PROJECT.projs [p];
			
			if (p < op -> numAttrs - 1)
				out << ", ";
		}
		out << endl;

		out << "Output Synopsis: ";
		if (op -> u.PROJECT.outSyn)
			out << op -> u.PROJECT.outSyn -> id;
		out << endl;
		
		break;
		
	case PO_JOIN:
		
		out << "Predicate: ";

		if (op -> u.JOIN.pred)
			out << op -> u.JOIN.pred << endl;
		else
			out << "(null)" << endl;

		out << "Inner Synopsis: ";
		if (op -> u.JOIN.innerSyn)
			out << op -> u.JOIN.innerSyn -> id;
		out << endl;
		
		out << "Outer Synopsis: ";
		if (op -> u.JOIN.outerSyn)
			out << op -> u.JOIN.outerSyn -> id;
		out << endl;

		out << "Join Synopsis: ";
		if (op -> u.JOIN.joinSyn)
			out << op -> u.JOIN.joinSyn -> id;
		out << endl;		
		
		break;

	case PO_STR_JOIN:
		
		out << "Predicate: ";
		if (op -> u.STR_JOIN.pred)
			out << op -> u.STR_JOIN.pred << endl;
		else
			out << "(null)" << endl;
		
		out << "Inner Synopsis: ";
		if (op -> u.STR_JOIN.innerSyn)
			out << op -> u.STR_JOIN.innerSyn -> id;
		out << endl;
		
		break;
		
	case PO_JOIN_PROJECT:
		
		out << "Projections: ";		
		for (unsigned int p = 0 ; p < op -> numAttrs ; p++) {
			out << op -> u.JOIN_PROJECT.projs [p];
			
			if (p < op -> numAttrs - 1)
				out << ", ";
		}
		out << endl;
		
		out << "Predicate: ";
		if (op -> u.JOIN_PROJECT.pred)
			out << op -> u.JOIN_PROJECT.pred << endl;
		else
			out << "(null)" << endl;

		out << "Inner Synopsis: ";
		if (op -> u.JOIN_PROJECT.innerSyn)
			out << op -> u.JOIN_PROJECT.innerSyn -> id;
		out << endl;
		
		out << "Outer Synopsis: ";
		if (op -> u.JOIN_PROJECT.outerSyn)
			out << op -> u.JOIN_PROJECT.outerSyn -> id;
		out << endl;

		out << "Join Synopsis: ";
		if (op -> u.JOIN_PROJECT.joinSyn)
			out << op -> u.JOIN_PROJECT.joinSyn -> id;
		out << endl;		
		
		break;
				
	case PO_STR_JOIN_PROJECT:
		
		out << "Projections: ";
		for (unsigned int p = 0 ; p < op -> numAttrs ; p++) {
			out << op -> u.STR_JOIN_PROJECT.projs [p];
			
			if (p < op -> numAttrs - 1)
				out << ", ";
		}		
		out << endl;

		out << "Predicate: ";
		if (op -> u.STR_JOIN_PROJECT.pred)
			out << op -> u.STR_JOIN_PROJECT.pred << endl;
		else
			out << "(null)" << endl;		
		
		out << "Inner Synopsis: ";
		if (op -> u.STR_JOIN_PROJECT.innerSyn)
			out << op -> u.STR_JOIN_PROJECT.innerSyn -> id;
		out << endl;
		
		break;		
		
	case PO_GROUP_AGGR:
		
		out << "Grouping attributes: ";
		
		for (unsigned int a = 0 ; a < op -> u.GROUP_AGGR.numGroupAttrs ;
			 a++) {
			
			out << op -> u.GROUP_AGGR.groupAttrs [a];
			
			if (a < op -> u.GROUP_AGGR.numGroupAttrs - 1)
				out << ", ";
			
		}		
		out << endl;

		out << "Aggregations: ";
		for (unsigned int a = 0 ; a < op -> u.GROUP_AGGR.numAggrAttrs ;
			 a++) {
			out << op -> u.GROUP_AGGR.fn [a] 
				<< "("
				<< op -> u.GROUP_AGGR.aggrAttrs [a]
				<< ")";
			if (a < op -> u.GROUP_AGGR.numAggrAttrs - 1)
				out << ", ";
		}
		out << endl;

		out << "Input Synopsis: ";		
		if (op -> u.GROUP_AGGR.inSyn)
			out << op -> u.GROUP_AGGR.inSyn -> id;
		out << endl;

		out << "Output Synopsis: ";		
		if (op -> u.GROUP_AGGR.outSyn)
			out << op -> u.GROUP_AGGR.outSyn -> id;
		out << endl;

		
		break;
		
	case PO_DISTINCT:
		
		out << "Output Synopsis: ";		
		if (op -> u.DISTINCT.outSyn)
			out << op -> u.DISTINCT.outSyn -> id;
		out << endl;
		
		break;
		
	case PO_ROW_WIN:
		out << "Number of rows: " << op -> u.ROW_WIN.numRows << endl;

		out << "Window Synopsis: ";
		if (op -> u.ROW_WIN.winSyn)
			out << op -> u.ROW_WIN.winSyn -> id;
		out << endl;
		
		break;
		
	case PO_RANGE_WIN:
		out << "Window Size: " 
			<< op -> u.RANGE_WIN.timeUnits
			<< endl;

		out << "Window slide: " 
			<< op -> u.RANGE_WIN.slideUnits
			<< endl;
		
		out << "Window Synopsis: ";
		if (op -> u.RANGE_WIN.winSyn)
			out << op -> u.RANGE_WIN.winSyn -> id;
		out << endl;

		break;				
		
	case PO_PARTN_WIN:
		out << "Partition attributes: ";
		for (unsigned int a = 0 ; a < op -> u.PARTN_WIN.numPartnAttrs ; a++) {
			out << op -> u.PARTN_WIN.partnAttrs [a];
			
			if (a < op -> u.PARTN_WIN.numPartnAttrs - 1)
				out << ", ";
		}
		out << endl;

		out << "Window Size: " 
			<< op -> u.PARTN_WIN.numRows << endl;

		
		out << "Window Synopsis: ";
		if (op -> u.PARTN_WIN.winSyn)
			out << op -> u.PARTN_WIN.winSyn -> id;
		out << endl;
		
		break;

	case PO_ISTREAM:
		out << "Synopsis: ";
		if (op -> u.ISTREAM.nowSyn)
			out << op -> u.ISTREAM.nowSyn -> id;
		out << endl;
		
		break;		
		
	case PO_DSTREAM:
		out << "Synopsis: ";
		if (op -> u.DSTREAM.nowSyn)
			out << op -> u.DSTREAM.nowSyn -> id;
		out << endl;
		
		break;
		
	case PO_RSTREAM:
		out << "Synopsis: ";
		if (op -> u.RSTREAM.inSyn)
			out << op -> u.RSTREAM.inSyn -> id;
		out << endl;
		
		break;
		
	case PO_STREAM_SOURCE:
		break;
		
	case PO_RELN_SOURCE:
		break;
		
	case PO_OUTPUT:
		break;

	case PO_SINK:
		break;

	case PO_QUERY_SOURCE:
		break;

	case PO_SS_GEN:
		break;
		
	default:
		break;
	}
	
	return out;
}

ostream& Physical::operator << (ostream &out, Synopsis *syn)
{
	ASSERT (syn);
	
	out << syn -> id << ". ";

	switch (syn -> kind) {
	case REL_SYN:
		out << "Relation Synopsis";
		break;
		
	case WIN_SYN:
		out << "Window Synopsis";
		break;
		
	case PARTN_WIN_SYN:
		out << "Partition window synopsis";
		break;
		
	case LIN_SYN:
		out << "Lineage synopsis";
		break;
		
	default:
		out << "(unknown)";
		break;
	}
	
	out << ": " << endl;
	
	if (syn -> store)
		out << "Store: " << syn -> store -> id << endl;
	else
		out << "Store: " << "(null)" << endl;

	if (syn -> ownOp)
		out << "Owner: " << syn -> ownOp -> id << endl;
	else
		out << "Owner: " << "(null)" << endl;

	return out;
}

ostream& Physical::operator << (ostream &out, Store *store)
{
	ASSERT (store);

	out << store -> id << ". ";

	switch (store -> kind) {
	case SIMPLE_STORE:
		out << "Simple Store";
		break;
		
	case REL_STORE:
		out << "Relation Store";
		break;
		
	case WIN_STORE:
		out << "Window Store";
		break;
		
	case LIN_STORE:
		out << "Lineage store";
		break;
		
	case PARTN_WIN_STORE:
		out << "Partition window store";
		break;
		
	default:
		out << "Unknown store";
		break;
	}
	out << ": " << endl;
	
	if (store -> ownOp) 
		out << "Owner: " << store -> ownOp -> id << endl;
	else
		out << "Owner: " << "(null)" << endl;
	
	out << "Stubs: ";
	for (unsigned int s = 0 ; s < store -> numStubs ; s++) {
		out << store -> stubs [s] -> id;
		
		if (s < store -> numStubs - 1)
			out << ", ";
	}
	out << endl;
	
	return out;
}
	
