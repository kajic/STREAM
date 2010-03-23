/**
 * @file  query_debug.cc 
 * @brief debug print routines for SemanticQuery::Query objects
 */

#ifndef _DEBUG_
#include "common/debug.h"
#endif

#ifndef _QUERY_DEBUG_
#include "querygen/query_debug.h"
#endif

#ifndef _TABLE_MGR_
#include "metadata/table_mgr.h"
#endif

using namespace std;
using namespace Semantic;

ostream& Semantic::operator << (ostream& out, Attr attr) 
{
	out << "[" << attr.varId << "," << attr.attrId << "]";
	return out;
}

ostream& Semantic::operator << (ostream& out, AggrFn fn) 
{
	switch(fn) {
	case COUNT:   out << "COUNT"; break;
	case SUM:     out << "SUM";   break;
	case AVG:     out << "AVG";   break;
	case MAX:     out << "MAX";   break;
	case MIN:     out << "MIN";   break;
#ifdef _DM_
	default: ASSERT(0);
#endif
	}
	return out;
}

ostream& Semantic::operator << (ostream& out, ArithOp op)
{
	switch(op) {
	case ADD:     out << " + ";   break;
	case SUB:     out << " - ";   break;
	case MUL:     out << " * ";   break;
	case DIV:     out << " / ";   break;
#ifdef _DM_
	default: ASSERT(0);
#endif
	}
	
	return out;
}

ostream& Semantic::operator << (ostream& out, CompOp op)
{
	switch(op) {
	case LT:      out << " < ";   break;
	case LE:      out << " <= ";  break;
	case GT:      out << " > ";   break;
	case GE:      out << " >= ";  break;
	case EQ:      out << " == ";  break;
	case NE:      out << " != ";  break;
#ifdef _DM_
	default: ASSERT(0);
#endif
	}
	
	return out;
}

ostream& Semantic::operator << (ostream& out, Expr *expr) 
{
	switch (expr -> exprType) {
		
	case E_CONST_VAL:
		
		switch (expr -> type) {
		case INT:   out << expr -> u.ival; break;
		case FLOAT: out << expr -> u.fval; break;
		case CHAR:  out << expr -> u.sval; break;
		case BYTE:  out << expr -> u.bval; break;
#ifdef _DM_
		default:    ASSERT(0);
#endif
		}
		break;
		
	case E_ATTR_REF:
		out << expr -> u.attr;
		break;
		
	case E_AGGR_EXPR:
		out << expr -> u.AGGR_EXPR.fn 
			<< "(" 
			<< expr -> u.AGGR_EXPR.attr
			<< ")";
		break;
		
	case E_COMP_EXPR:		
		out << "("
			<< expr -> u.COMP_EXPR.left
			<< expr -> u.COMP_EXPR.op
			<< expr -> u.COMP_EXPR.right
			<< ")";
		break;
		
#ifdef _DM_
	default:
		ASSERT(0);
#endif
	}
	
	return out;
}

ostream& Semantic::operator << (ostream& out, BExpr bexpr)
{		
	out << bexpr.left 
		<< bexpr.op
		<< bexpr.right;
	
	return out;
}

ostream& Semantic::operator << (ostream& out, WindowSpec win)
{
	unsigned int numPartnAttrs;

	out << "[";
	
	switch(win.type) {
		
	case RANGE:
		
		out << "Range " << win.u.RANGE.timeUnits;
		break;
		
	case ROW:
		
		out << "Rows " << win.u.ROW.numRows;
		break;
		
	case PARTITION:
		out << "Partition By ";
		
		numPartnAttrs = win.u.PARTITION.numPartnAttrs;
		for(unsigned int a = 0 ; a < numPartnAttrs ; a++) {
			out << win.u.PARTITION.attrs [a];
			if (a < numPartnAttrs - 1)
				out << ", ";
		}
		
		out << " Rows " << win.u.PARTITION.numRows;
		break;
		
	case NOW:
		
		out << "Now";
		break;
		
	case UNBOUNDED:
		
		out << "Unbounded";
		break;
		
	default:
		ASSERT(0);
	}
	
	out << "]";
	
	return out;
}

ostream& Semantic::operator << (ostream& out, R2SOp op) 
{
	switch(op) {
	case ISTREAM: out << "IStream"; break;
	case DSTREAM: out << "DStream"; break;
	case RSTREAM: out << "Rstream"; break;
	default: ASSERT(0);
	}
	
	return out;
}

void Semantic::printQuery (ostream&                      out, 
						   const Query&                  query, 
						   const Metadata::TableManager *tableMgr)
{				
	if(query.bRelToStr) {
		out << query.r2sOp << "(" << endl;
	}
	
	//----------------------------------------------------------------------
	// Select clause
	//----------------------------------------------------------------------
	unsigned int     numProjExprs;	
	out << "Select ";
	
	if(query.bDistinct)
		out << "Distinct ";
	
	numProjExprs = query.numProjExprs;
	for(unsigned int e = 0 ; e < numProjExprs ; e++) {
		out << query.projExprs [e];
		
		if(e < numProjExprs - 1)
			out << ", ";
	}
	
	out << endl;
	
	//----------------------------------------------------------------------
	// Project clause
	//----------------------------------------------------------------------
	unsigned int             numFromClauseTables;
	unsigned int             varId;
	unsigned int             tableId;
	
	out << "From ";
	
	numFromClauseTables = query.numFromClauseTables;
	for(unsigned int t = 0 ; t < numFromClauseTables ; t++) {
		
		// Variable Id
		varId = query.fromClauseTables [t];
		out << varId;
		
		// If stream, print window
		tableId = query.refTables [ query.fromClauseTables [t]];
		if(tableMgr -> isStream(tableId)) {
			out << " " << query.winSpec[t];
		}
		
		if(t < numFromClauseTables - 1)
			out << ", ";		
	}
	
	out << endl;
	
	//----------------------------------------------------------------------
	// Where Clause
	//----------------------------------------------------------------------
	unsigned int numPreds;
		
	numPreds = query.numPreds;
	if (numPreds > 0) {
		out << "Where ";
		
		for(unsigned int p = 0 ; p < numPreds ; p++) {
			out << query.preds[p];
			
			if(p < numPreds - 1)
				out << " and ";
		}
		
		out << endl;
	}
	
	//----------------------------------------------------------------------
	// Group By clause
	//----------------------------------------------------------------------
	unsigned int numGbyAttrs;

	numGbyAttrs = query.numGbyAttrs;
	if(numGbyAttrs > 0) {
		
		out << "Group By ";
		
		for(unsigned int a = 0 ; a < numGbyAttrs ; a++) {
			out << query.gbyAttrs[a];
			
			if(a < numGbyAttrs - 1)
				out << ", ";
		}
		
		out << endl;
	}
	
	
	if(query.bRelToStr) {
		out << ")" << endl;
	}	
	
	return;
}
