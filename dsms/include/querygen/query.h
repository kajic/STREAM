/**
 * @file         query.h
 * @date         May. 5, 2004
 * @brief        Internal (semantic) representation of a query.
 */

#ifndef _QUERY_
#define _QUERY_

#include "common/types.h"
#include "common/op.h"
#include "common/aggr.h"
#include "common/window.h"
#include "common/constants.h"

namespace Semantic {
  
	/**
	 * Reference to an attribute in a  query: indicated by a variable id and
	 * the  attribute id.   The variable  id identifies  the relation/stream
	 * that  the  attribute  belongs  to  and attribute  id  identifies  the
	 * attribute among the attributes of the relation/stream.  Note that the
	 * same  stream/relation could  occur many  times in  a relation  and so
	 * could have more than one variable id.
	 */  
	struct Attr {
		unsigned int varId;
		unsigned int attrId;
	};
	
	//----------------------------------------------------------------------
	// Windows
	//----------------------------------------------------------------------
  
	/**
	 * Specification of a sliding window in a query.
	 */  
	static const unsigned int MAX_PARTN_ATTRS = 10;
	struct WindowSpec {
		WinType      type;
		
		union {
      
			// Time-based window.
			struct {
                unsigned int slideUnits;
				unsigned int timeUnits;
			} RANGE;
      
			// Row-based windows
			struct {
				unsigned int numRows;
			} ROW;
			
			// Partition window
			struct {
				unsigned int numPartnAttrs;
				unsigned int numRows;
				Attr         attrs [ MAX_PARTN_ATTRS ];
			} PARTITION;      
		} u;
	};  
  
	//----------------------------------------------------------------------
	// Expressions
	//----------------------------------------------------------------------
	
	/**
	 * Expression Type: An expression could be a constant value, attribute,
	 * or a complex expression which is an operation over upto two other
	 * expressions.
	 */  
	enum ExprType {
		E_CONST_VAL,
		E_ATTR_REF,
		E_AGGR_EXPR,
		E_COMP_EXPR
	};
	
	/**
	 * Expressions occurring in a query:
	 * 
	 * Note the difference between Data type of an expression and
	 * "expression-type".  Data type of an expression indicates the output
	 * data type of the expression (int, float etc), which expression-type
	 * describes the structure of the expression - whether it is a constant
	 * value, attribute, or complex expression built from other expressions.
	 */  
	struct Expr {
		Type     type;      
		ExprType exprType;
		
		union {
			int         ival;  // type == INT && exprType == CONST_VAL
			float       fval;  // type == FLOAT && exprType == CONST_VAL
			char       *sval;  // type == CHAR && exprType == CONST_VAL
			char        bval;  // type == BYTE && exprType == CONST_VAL
      
			struct {           // exprType == COMPLEX_EXPR 
				ArithOp   op;
				Expr     *left;
				Expr     *right;
			} COMP_EXPR;
			
			struct {           // exprType == AGGR_EXPR
				AggrFn    fn;
				Attr      attr;
			} AGGR_EXPR;
			
			Attr        attr;  // exprType == ATTR_REF
		} u;
	};
	
	/**
	 * Boolean expressions occurring in a query
	 */
	struct BExpr {    
		CompOp       op;
		Expr        *left;
		Expr        *right;
	};
	
	//----------------------------------------------------------------------
	// Query
	//----------------------------------------------------------------------
	
	enum QueryType {
		/// Query with a select-from-where block
		SFW_QUERY,

		/// Query with a binary operator (union, except, etc)
		BINARY_OP
	};
	
	static const unsigned int MAX_PRED        = 10;
	static const unsigned int MAX_PROJ        = 50;
	static const unsigned int MAX_GROUP_ATTRS = 10;
	
	enum R2SOp {
		ISTREAM, DSTREAM, RSTREAM
	};
	
	enum BinaryOp {
		UNION, EXCEPT
	};
	
	/**
	 * Internal representation of a parsed and "annotated" query.  Internal
	 * representation differs from the parse tree (NODE *) produced by the
	 * parser in the following ways:
	 * 
	 * 1. All references to streams / relations are through their internal
	 *    identifiers and not strings which are external names.
	 * 
	 * 2. Each occurrence of a stream/relation is identified by a
	 *    variable-id (note: there could be multiple occurrences
	 *    e.g. self-join).
	 * 
	 * 3. Each attribute is identified by a <variable-id , attri-id> - so
	 *    the table to which an attribute belongs has been resolved.  For
	 *    parse trees some attributes identify the owning table and for
	 *    others this is implicit.
	 *
	 * 4. Every stream in the FROM clause is associated with a window.  In
	 *    NODE * there could be streams without a window.  We add the
	 *    default UNBOUNDED window for every stream in the FROM clause
	 *    without a window.
	 */
	struct Query {

		/// Type of the query 
		QueryType kind;
		
		/// The  list   of  tables  (stream/relations)   that  this  query
		/// references.  For SFW  queries this is the list  present in the
		/// FROM  clause.  Different  instances of  the same  relation are
		/// listed  separately.   For binary  operator  queries, the  list
		/// contains the  two tables referenced  by the operator.   In the
		/// rest  of the query  any reference  to a  table is  through the
		/// index  of the  table in  this list  - and  this is  called the
		/// variable-id of the table.		
		unsigned int refTables [MAX_TABLES];
		unsigned int numRefTables;
		
		/// Tables  listed in  the FROM  clause  of the  SFW query.   This
		/// information  is redundant,  the  fromClauseTables are  exactly
		/// those listed in tableRef for SFW queries.  Valid only if
		/// queryType is SFW_QUERY.		
		unsigned int fromClauseTables [MAX_TABLES];
		unsigned int numFromClauseTables;
		
		/// The   window   specification    for   the   streams   in   the
		/// fromClauseTables.  All  streams in the  from clause do  have a
		/// window  specification  once   the  Query  structure  is  fully
		/// constructed (we add the  default unbounded window if no window
		/// is present)		
		WindowSpec winSpec[MAX_TABLES];
		
		/// Boolean  predicates  occurring  in  the  where  clause.   This
		/// assumes that the where clause is a conjunction.		
		BExpr preds[MAX_PRED];
		unsigned int numPreds;
		
		/// Expressions occurring in the SELECT clause.  Mostly projection
		/// expressions, but could also be aggregations.		
		Expr *projExprs[MAX_PROJ];
		unsigned int numProjExprs;
		
		/// Is there a distinct operation on top of a SFW_BLOCK
		bool bDistinct;
		
		/// Attributes in the group by clause
		Attr gbyAttrs[MAX_GROUP_ATTRS];
		unsigned int numGbyAttrs;
		
		/// Is there a relation-to-stream operation (XStream) for the query>
		bool bRelToStr;
		R2SOp r2sOp;

		/// The binary operator
		BinaryOp binOp;
		
		/// If the query type is BINARY_OP, the left table
		unsigned int leftTable;
		
		/// If the query type is BINARY_OP, the right table
		unsigned int rightTable;
	};
}

#endif
