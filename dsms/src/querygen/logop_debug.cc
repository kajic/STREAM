#ifndef _DEBUG_
#include "common/debug.h"
#endif

#ifndef _LOGOP_DEBUG_
#include "querygen/logop_debug.h"
#endif

using namespace Logical;
using namespace std;

#ifdef _DM_
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

ostream& Logical::operator << (ostream& out, Attr attr)
{
	switch (attr.kind) {
	case NAMED:
		out << "[" << attr.u.NAMED.varId << ", " 
			<< attr.u.NAMED.attrId
			<< "]";
		break;
		
	case UNNAMED:
		out << "[U]";
		break;
		
	case AGGR:
		out << attr.u.AGGR.fn << "(" 
			<< attr.u.AGGR.varId 
			<< ", "
			<< attr.u.AGGR.attrId
			<< ")";
		break;			
		
	default:
		out << "[Err]";
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

ostream& Logical::operator << (ostream& out, Expr *expr) 
{
	switch (expr -> kind) {
	case CONST_VAL:
		
		switch (expr -> type) {
		case INT:    out << expr -> u.ival; break;
		case FLOAT:  out << expr -> u.fval; break;
		case BYTE:   out << expr -> u.bval; break;
		case CHAR:   out << expr -> u.sval; break;
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
	
ostream& Logical::operator << (ostream& out, BExpr bexpr)
{
	out << bexpr.left 
		<< bexpr.op
		<< bexpr.right;
	return out;
}

ostream& Logical::operator << (std::ostream& out, Operator *op) 
{
	out << op -> id;
	
	if (op -> bStream)
		out << ":+:";
	else 
		out << ": :";
	
	out << ": ";
	
	switch (op -> kind) {
	case LO_STREAM_SOURCE:
		out << "Stream Source [" 
			<< op -> u.STREAM_SOURCE.varId 
			<< "]";
		break;		
		
	case LO_RELN_SOURCE:
		out << "Relation Source ["
			<< op -> u.STREAM_SOURCE.varId 
			<< "]";
		break;				
		
	case LO_ROW_WIN:
		out << "Row Window ["
			<< op -> u.ROW_WIN.numRows
			<< "]"
			<< " ("
			<< op -> inputs [0] -> id
			<< ")";
		
		out << endl << endl << op -> inputs [0] << endl;

		break;

	case LO_RANGE_WIN:
		out << "Range Window ["
			<< op -> u.RANGE_WIN.timeUnits
			<< "]"
            << "(stride: "
            << op -> u.RANGE_WIN.strideUnits
            << ")"
			<< " ("
			<< op -> inputs [0] -> id
			<< ")";

		out << endl << endl << op -> inputs [0] << endl;
		
		break;
		
	case LO_NOW_WIN:
		out << "Now Window"
			<< " ("
			<< op -> inputs [0] -> id
			<< ")";

		out << endl << endl << op -> inputs [0] << endl;
		
		break;
		
	case LO_PARTN_WIN:
		out << "Partition Window {";
		
		for (unsigned int a = 0 ; a < op -> u.PARTN_WIN.numPartnAttrs ; a++) {
			out << op -> u.PARTN_WIN.partnAttrs [a];
			
			if (a < op -> u.PARTN_WIN.numPartnAttrs - 1)
				out << ", ";
		}
		
		out << "} ";		
		out << "[" << op -> u.PARTN_WIN.numRows << "]";
		
		out << " (" 
			<< op -> inputs [0] -> id
			<< ")";		
		
		out << endl << endl << op -> inputs [0] << endl;
		
		break;				
		
	case LO_SELECT:
		out << "Select {"
			<< op -> u.SELECT.bexpr
			<< "}"
			<< " ("
			<< op -> inputs [0] -> id
			<< ")";
		
		out << endl << endl << op -> inputs [0] << endl;
		
		break;
		
	case LO_PROJECT:
		out << "Project {";
		
		for (unsigned int e = 0 ; e < op -> numOutAttrs ; e++) {
			out << op -> u.PROJECT.projExprs [e];
			
			if (e < op -> numOutAttrs - 1)
				out << ", ";
		}
		
		out << "}";
		
		out << " ("
			<< op -> inputs [0] -> id
			<< ")";

		out << endl << endl << op -> inputs [0] << endl;
		break;		

	case LO_CROSS:
		out << "Cross (";
		
		for (unsigned int i = 0 ; i < op -> numInputs ; i++) {
			out << op -> inputs [i] -> id;
			
			if (i < op -> numInputs - 1)
				out << ", ";
		}
		out << ")";
		
		for (unsigned int i = 0 ; i < op -> numInputs ; i++) 
			out << endl << endl << op -> inputs [i] << endl; 				

		break;
		
	case LO_GROUP_AGGR:
		out << "Group By {";
		
		for (unsigned int a = 0 ; a < op -> u.GROUP_AGGR.numGroupAttrs ; 
			 a++) {
			out << op -> u.GROUP_AGGR.groupAttrs [a];
			
			if (a < op -> u.GROUP_AGGR.numGroupAttrs - 1)
				out << ", ";
		}
		out << "} ";
		
		for (unsigned int a = 0 ; a < op -> u.GROUP_AGGR.numAggrAttrs ;
			 a++) {
			out << op -> u.GROUP_AGGR.fn [a] 
				<< "("
				<< op -> u.GROUP_AGGR.aggrAttrs [a]
				<< ")";
			if (a < op -> u.GROUP_AGGR.numAggrAttrs - 1)
				out << ", ";
		}
		
		out << " ("
			<< op -> inputs [0] -> id
			<< ")";
		
		out << endl << endl << op -> inputs [0] << endl;

		break;
		
	case LO_DISTINCT:
		out << "Distinct ("
			<< op -> inputs [0] -> id 
			<< ")";

		out << endl << endl << op -> inputs [0] << endl;
		break;

	case LO_ISTREAM:
		out << "Istream ("
			<< op -> inputs [0] -> id
			<< ")";

		out << endl << endl << op -> inputs [0] << endl;
		break;

	case LO_DSTREAM:
		out << "Dstream ("
			<< op -> inputs [0] -> id
			<< ")";

		out << endl << endl << op -> inputs [0] << endl;
		break;

	case LO_RSTREAM:
		out << "Rstream ("
			<< op -> inputs [0] -> id
			<< ")";

		out << endl << endl << op -> inputs [0] << endl;
		break;

	case LO_STREAM_CROSS:
		
		out << "Stream Cross (";

		for (unsigned int i = 0 ; i < op -> numInputs ; i++) {
			out << op -> inputs [i] -> id;
			
			if (i < op -> numInputs - 1)
				out << ", ";
		}
		out << ")";
		
		for (unsigned int i = 0 ; i < op -> numInputs ; i++) 
			out << endl << endl << op -> inputs [i] << endl;
		
		break;

	case LO_UNION:
		out << "Union (";

		for (unsigned int i = 0 ; i < op -> numInputs ; i++) {
			out << op -> inputs [i] -> id;

			if (i < op -> numInputs - 1)
				out << ", ";
		}
		out << ")";

		for (unsigned int i = 0 ; i < op -> numInputs ; i++) 
			out << endl << endl << op -> inputs [i] << endl;

		break;
		
	case LO_EXCEPT:
		
		out << "Except (";
		
		for (unsigned int i = 0 ; i < op -> numInputs ; i++) {
			out << op -> inputs [i] -> id;
			
			if (i < op -> numInputs - 1)
				out << ", ";
		}
		
		out << ")";
		
		for (unsigned int i = 0 ; i < op -> numInputs ; i++) 
			out << endl << endl << op -> inputs [i] << endl;
		
		break;		
		
	default:
		
		break;
	}
	
	return out;
}

//----------------------------------------------------------------------
// Consistency Check routines for each operator type
//----------------------------------------------------------------------

static bool check_stream_source (const Operator *op);
static bool check_reln_source (const Operator *op);
static bool check_row_window (const Operator *op);
static bool check_range_window (const Operator *op);
static bool check_now_window (const Operator *op);
static bool check_partn_window (const Operator *op);
static bool check_select (const Operator *op);
static bool check_project (const Operator *op);
static bool check_cross (const Operator *op);
static bool check_group_aggr (const Operator *op);
static bool check_distinct (const Operator *op);
static bool check_istream (const Operator *op);
static bool check_dstream (const Operator *op);
static bool check_rstream (const Operator *op);
static bool check_stream_cross (const Operator *op);
static bool check_union (const Operator *op);
static bool check_except (const Operator *op);

static bool schemaEqual (const Operator *op1, const Operator *op2);
static bool operator == (const Attr& attr1, const Attr& attr2);

//#define REQUIRE(x) {if (!(x)) return false;}
#define REQUIRE(x) {ASSERT(x);}

bool Logical::check_plan (const Operator *op)
{
	ASSERT (op);
	
	REQUIRE (op -> output == 0);
	
	return check_operator_recursive (op);
}

bool Logical::check_operator (const Operator *op)
{
	ASSERT (op);
	
	for (unsigned int i = 0 ; i < op -> numInputs ; i++) {
		REQUIRE (op -> inputs [i]);
		REQUIRE (op -> inputs [i] -> output == op);
	}
		
	switch (op -> kind) {
	case LO_STREAM_SOURCE:
		if (!check_stream_source (op))
			return false;
		break;

	case LO_RELN_SOURCE:
		if (!check_reln_source (op))
			return false;
		break;

	case LO_ROW_WIN:
		if (!check_row_window (op))
			return false;
		break;		
		
	case LO_RANGE_WIN:
		if (!check_range_window (op))
			return false;
		break;
		
	case LO_NOW_WIN:
		if (!check_now_window (op))
			return false;
		break;
		
	case LO_PARTN_WIN:
		if (!check_partn_window (op))
			return false;
		break;
		
	case LO_SELECT:
		if (!check_select (op))
			return false;
		break;

	case LO_PROJECT:
		if (!check_project (op))
			return false;
		break;

	case LO_CROSS:
		if (!check_cross (op))
			return false;
		break;
		
	case LO_GROUP_AGGR:
		if (!check_group_aggr (op))
			return false;
		break;

	case LO_DISTINCT:
		if (!check_distinct (op))
			return false;
		break;

	case LO_ISTREAM:
		if (!check_istream (op))
			return false;
		break;

	case LO_DSTREAM:
		if (!check_dstream (op))
			return false;
		break;
		
	case LO_RSTREAM:		
		if (!check_rstream (op))
			return false;
		break;
		
	case LO_STREAM_CROSS:
		if (!check_stream_cross (op))
			return false;
		break;

	case LO_UNION:
		if (!check_union (op))
			return false;
		break;

	case LO_EXCEPT:
		if (!check_except (op))
			return false;
		break;
		
	default:
		return false;
	}
	
	return true;	
}

bool Logical::check_operator_recursive (const Operator *op) 
{
	if (!check_operator (op))
		return false;
	
	for (unsigned int c = 0 ; c < op -> numInputs ; c++)
		if (!check_operator_recursive (op -> inputs [c]))
			return false;
	
	return true;
}

static bool check_stream_source (const Operator *op)
{
	ASSERT (op -> kind == LO_STREAM_SOURCE);
	
	REQUIRE (op -> bStream);

	for (unsigned int a = 0 ; a < op -> numOutAttrs ; a++) {
		REQUIRE (op -> outAttrs [a].kind == NAMED);
		REQUIRE (op -> outAttrs [a].u.NAMED.varId == 
				 op -> u.STREAM_SOURCE.varId);
		REQUIRE (op -> outAttrs [a].u.NAMED.attrId == a);
	}
	
	return true;
}

static bool check_reln_source (const Operator *op)
{
	ASSERT (op -> kind == LO_RELN_SOURCE);

	REQUIRE (!op -> bStream);
	
	for (unsigned int a = 0 ; a < op -> numOutAttrs ; a++) {
		REQUIRE (op -> outAttrs [a].kind == NAMED);
		REQUIRE (op -> outAttrs [a].u.NAMED.varId == 
				 op -> u.RELN_SOURCE.varId);
		REQUIRE (op -> outAttrs [a].u.NAMED.attrId == a);
	}
	
	return true;	
}

static bool check_row_window (const Operator *op)
{
	ASSERT (op -> kind == LO_ROW_WIN);
	
	REQUIRE (op -> numInputs == 1);
	REQUIRE (op -> inputs [0]);
	REQUIRE (op -> inputs [0] -> bStream);
	REQUIRE (!op -> bStream);
	REQUIRE (schemaEqual(op, op -> inputs [0]));
	
	return true;
}

static bool check_range_window (const Operator *op)
{	
	ASSERT (op -> kind == LO_RANGE_WIN);

	REQUIRE (op -> numInputs == 1);
	REQUIRE (op -> inputs [0]);
	REQUIRE (op -> inputs [0] -> bStream);
	REQUIRE (!op -> bStream);
	REQUIRE (schemaEqual(op, op -> inputs [0]));
	
	return true;
}

static bool check_now_window (const Operator *op)
{
	ASSERT (op -> kind == LO_NOW_WIN);
	
	REQUIRE (op -> numInputs == 1);
	REQUIRE (op -> inputs [0]);
	REQUIRE (op -> inputs [0] -> bStream);
	REQUIRE (!op -> bStream);
	REQUIRE (schemaEqual(op, op -> inputs [0]));
	
	return true;
}

static bool check_partn_window (const Operator *op)
{
	ASSERT (op -> kind == LO_PARTN_WIN);
	
	REQUIRE (op -> numInputs == 1);
	REQUIRE (op -> inputs [0]);
	REQUIRE (op -> inputs [0] -> bStream);
	REQUIRE (!op -> bStream);
	REQUIRE (schemaEqual(op, op -> inputs [0]));
	REQUIRE (op -> u.PARTN_WIN.numPartnAttrs > 0);
	
	for (unsigned int p = 0 ; p < op -> u.PARTN_WIN.numPartnAttrs ; p++) 
		REQUIRE (check_reference (op -> u.PARTN_WIN.partnAttrs [p], 
								  op -> inputs [0]));				 			
	
	return true;
}

static bool check_select (const Operator *op)
{
	ASSERT (op -> kind == LO_SELECT);
	
	REQUIRE (op -> numInputs == 1);
	REQUIRE (op -> inputs [0]);
	REQUIRE (schemaEqual(op, op -> inputs [0]));
	REQUIRE (op -> bStream == op -> inputs [0] -> bStream);
	REQUIRE (op -> u.SELECT.bexpr.left);
	REQUIRE (op -> u.SELECT.bexpr.right);
	REQUIRE (check_reference (op -> u.SELECT.bexpr.left, 
							  op -> inputs [0]));
	REQUIRE (check_reference (op -> u.SELECT.bexpr.right, 
							  op -> inputs [0]));
	
	return true;
}

static bool check_project (const Operator *op)
{
	ASSERT (op -> kind == LO_PROJECT);
	
	REQUIRE (op -> numInputs == 1);
	REQUIRE (op -> inputs [0]);
	REQUIRE (op -> bStream == op -> inputs [0] -> bStream);
	
	for (unsigned int a = 0 ; a < op -> numOutAttrs ; a++) {
		if (op -> u.PROJECT.projExprs [a] -> kind == ATTR_REF) {
			REQUIRE (op -> u.PROJECT.projExprs [a] -> u.attr ==
					 op -> outAttrs [a]);
		}
		else {
			REQUIRE (op -> outAttrs [a].kind == UNNAMED);
			REQUIRE (op -> outAttrs [a].u.UNNAMED.type == 
					 op -> u.PROJECT.projExprs [a] -> type);
		}		
		
		REQUIRE(check_reference (op -> u.PROJECT.projExprs [a], 
								 op -> inputs [0]));   
	}		
	
	return true;
}


static bool check_cross (const Operator *op)
{
	ASSERT (op -> kind == LO_CROSS);
	
	REQUIRE (op -> numInputs > 1);
	
	for (unsigned int i = 0 ; i < op -> numInputs ; i++) {
		REQUIRE (op -> inputs [i]);
		REQUIRE (op -> inputs [i] -> bStream || !op -> bStream);
	}
	
	unsigned int a1 = 0; 
	for (unsigned int i = 0 ; i < op -> numInputs ; i++) {
		for (unsigned a2 = 0 ; a2 < op -> inputs [i] -> numOutAttrs ;  a2++) {
			REQUIRE (a1 < op -> numOutAttrs);
			REQUIRE (op -> outAttrs [a1++] == 
					 op -> inputs [i] -> outAttrs [a2]);
		}
	}
	REQUIRE (a1 == op -> numOutAttrs);	
	
	return true;
}

static bool check_stream_cross (const Operator *op)
{
	ASSERT (op -> kind == LO_STREAM_CROSS);
	
	REQUIRE (op -> numInputs > 1);
	REQUIRE (op -> bStream);

	for (unsigned int i = 0 ; i < op -> numInputs ; i++) {
		REQUIRE (op -> inputs [i]);
	}
	
	REQUIRE (op -> inputs [0] -> bStream);
	
	unsigned int a1 = 0; 
	for (unsigned int i = 0 ; i < op -> numInputs ; i++) {
		for (unsigned a2 = 0 ; a2 < op -> inputs [i] -> numOutAttrs ;  a2++) {
			REQUIRE (a1 < op -> numOutAttrs);
			REQUIRE (op -> outAttrs [a1++] == 
					 op -> inputs [i] -> outAttrs [a2]);
		}
	}
	REQUIRE (a1 == op -> numOutAttrs);	
	
	return true;
}

static bool check_group_aggr (const Operator *op)
{
	Attr aggrAttr;
	Attr outAttr;

	ASSERT (op -> kind == LO_GROUP_AGGR);
	
	REQUIRE (op -> numInputs == 1);
	REQUIRE (!op -> bStream);
		
	unsigned int a = 0;
	for (unsigned int g = 0 ; g < op -> u.GROUP_AGGR.numGroupAttrs ;
		 g++) {
		REQUIRE (a < op -> numOutAttrs);
		REQUIRE (op -> outAttrs [a++] == 
				 op -> u.GROUP_AGGR.groupAttrs [g]);
	}
	
	for (unsigned int agg = 0 ; agg < op -> u.GROUP_AGGR.numAggrAttrs ;
		 agg++) {
		REQUIRE (a < op -> numOutAttrs);
		
		aggrAttr = op -> u.GROUP_AGGR.aggrAttrs [agg];
		outAttr = op -> outAttrs [a++];
		
		if (aggrAttr.kind == NAMED) {
			REQUIRE (outAttr.kind == AGGR);
			REQUIRE (outAttr.u.AGGR.varId ==
					 aggrAttr.u.NAMED.varId);
			REQUIRE (outAttr.u.AGGR.tableId ==
					 aggrAttr.u.NAMED.tableId);
			REQUIRE (outAttr.u.AGGR.attrId ==
					 aggrAttr.u.NAMED.attrId);
		}
		else if (aggrAttr.kind == UNNAMED) {
			REQUIRE (outAttr.kind == UNNAMED);
			REQUIRE (outAttr.u.UNNAMED.type == 
					 getOutputType (op -> u.GROUP_AGGR.fn [agg],
									aggrAttr.u.UNNAMED.type));
		}
		else {
			ASSERT (aggrAttr.kind == AGGR);
			REQUIRE (outAttr.kind == UNNAMED);
		}
	}
	
	REQUIRE (a == op -> numOutAttrs);
	
	return true;
}

static bool check_distinct (const Operator *op)
{
	ASSERT (op -> kind == LO_DISTINCT);
	
	REQUIRE (op -> numInputs == 1);
	REQUIRE (op -> inputs [0]);
	REQUIRE (schemaEqual(op, op -> inputs [0]));
	REQUIRE (op -> bStream == op -> inputs [0] -> bStream);

	return true;
}

static bool check_istream (const Operator *op)
{
	ASSERT (op -> kind == LO_ISTREAM);
	
	REQUIRE (op -> numInputs == 1);
	REQUIRE (op -> inputs [0]);
	REQUIRE (schemaEqual(op, op -> inputs [0]));
	REQUIRE (op -> bStream);
	
	return true;
}

static bool check_dstream (const Operator *op)
{
	ASSERT (op -> kind == LO_DSTREAM);
	
	REQUIRE (op -> numInputs == 1);
	REQUIRE (op -> inputs [0]);
	REQUIRE (schemaEqual(op, op -> inputs [0]));
	REQUIRE (op -> bStream);
	
	return true;
}

static bool check_rstream (const Operator *op)
{
	ASSERT (op -> kind == LO_RSTREAM);
	
	REQUIRE (op -> numInputs == 1);
	REQUIRE (op -> inputs [0]);
	REQUIRE (schemaEqual(op, op -> inputs [0]));
	REQUIRE (op -> bStream);
	
	return true;
}

static bool check_union (const Operator *op)
{
	ASSERT (op -> kind == LO_UNION);
	
	REQUIRE (op -> numInputs == 2);
	REQUIRE (op -> inputs [0]);
	REQUIRE (op -> inputs [1]);
	REQUIRE (op -> inputs [0] -> bStream || !op -> inputs [0] -> bStream);
	REQUIRE (op -> inputs [1] -> bStream || !op -> inputs [1] -> bStream);	

	REQUIRE (op -> inputs [0] -> numOutAttrs = op -> numOutAttrs);
	REQUIRE (op -> inputs [1] -> numOutAttrs = op -> numOutAttrs);
	
	return true;
}

static bool check_except (const Operator *op)
{
	ASSERT (op -> kind == LO_EXCEPT);
	
	REQUIRE (op -> numInputs == 2);
	REQUIRE (op -> inputs [0]);
	REQUIRE (op -> inputs [1]);
	REQUIRE (!op -> bStream);
	
	REQUIRE (op -> inputs [0] -> numOutAttrs = op -> numOutAttrs);
	REQUIRE (op -> inputs [1] -> numOutAttrs = op -> numOutAttrs);
	
	return true;
}


//----------------------------------------------------------------------

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
		return (attr1.u.UNNAMED.type == attr2.u.UNNAMED.type);
		
	default:
		return false;		
	}

	// never comes
	return false;
}

static bool schemaEqual (const Operator *op1, const Operator *op2)
{
	REQUIRE (op1 -> numOutAttrs == op2 -> numOutAttrs);

	for (unsigned int a = 0 ; a < op1 -> numOutAttrs ; a++)
		REQUIRE (op1 -> outAttrs [a] == op2 -> outAttrs [a]);
	return true;
}

//----------------------------------------------------------------------
#endif
