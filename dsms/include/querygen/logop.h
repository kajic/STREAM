/**
 * @file             logop.h
 * @date             May 5, 2004
 * @brief            Logical operators in the system
 */

#ifndef _LOGOP_
#define _LOGOP_

#ifndef _CONSTANTS_
#include "common/constants.h"
#endif

#ifndef _TYPES_
#include "common/types.h"
#endif

#ifndef _AGGR_
#include "common/aggr.h"
#endif

#ifndef _OP_
#include "common/op.h"
#endif

#ifndef _TABLE_MGR_
#include "metadata/table_mgr.h"
#endif

namespace Logical {
	
	//----------------------------------------------------------------------
	// Attributes & Schema
	//----------------------------------------------------------------------
	
	/**
	 * Three kinds of attributes: see description below.
	 */ 
	enum AttrKind {
		UNNAMED,
		NAMED,
		AGGR
	};
	
	/**
	 * Different kinds of attributes present at various points of a logical
	 * query plan.  
	 *
	 * ANONYMOUS attributes are those which  can not be directly traced to a
	 * named  attribute (attribute  of a  stream/relation).  For  example, a
	 * project  operator  that  computes  arbitrary expressions  over  input
	 * produces anonymous attributes (e.g., R.A + S.B).
	 * 
	 * NAMED  are  attributes that  originate  in  a stream/relation.   Most
	 * operators preserve  the named attributes from their  input into their
	 * output.
	 *
	 * AGGR attributes are attributes that are produced by a group-by
	 * aggregate operator.
	 */
	
	struct Attr {
		AttrKind   kind;
		
		union {
			struct {
				unsigned int varId;
				unsigned int tableId;
				unsigned int attrId;
			} NAMED;
			
			struct {
				unsigned int varId;
				unsigned int tableId;
				unsigned int attrId;
				AggrFn       fn;
			} AGGR;
			
			struct {
				Type         type;
			} UNNAMED;
		} u;
	};
	
	/**
	 * Three kinds of expressions: constant values, attribute references,
	 * & arithmetic expressions.
	 */ 
	enum ExprKind {
		CONST_VAL, ATTR_REF, COMP_EXPR
	};
	
	/**
	 * Expressions:
	 * 
	 * Very similar to SemanticQuery::Expression.  The differences are:
	 *
	 *     1. Attributes referenced are slightly different.
	 *     
	 *     2. They no longer represent aggregation expressions of the form 
	 *        MAX(S.A). 
	 *
	 * These  expressions  are used  in  project  operator  and in  select
	 * operator (as part  of boolean expressions).  (Aggregation functions
	 * occur  only within Gby-aggregate  operator, while  in SemanticQuery
	 * they were  pooled along with  projection expressions in  the SELECT
	 * clause.)
	 */	
	
	struct Expr {
		Type        type;
		ExprKind    kind;
		
		union {
			int         ival;  // type == INT && exprType == CONST_VAL
			float       fval;  // type == FLOAT && exprType == CONST_VAL
			const char *sval;  // type == CHAR && exprType == CONST_VAL
			char        bval;  // type == BYTE && exprType == CONST_VAL
			
			struct {           // exprType == COMPLEX_EXPR 
				ArithOp   op;
				Expr     *left;
				Expr     *right;
			} COMP_EXPR;
			
			Attr        attr;
		} u;
	};
	
	/**
	 * Boolean expression: comparison of two expressions.
	 */ 
	struct BExpr {
		CompOp         op;
		Expr          *left;
		Expr          *right;
	};
	
	/**
	 * Different  kinds of logical  operators in  the logical  query plan.
	 * Mostly  correspond to  the standard  relational  algebra operators.
	 * Others  include  operators  representing  stream/relation  sources,
	 * window operators and relation-to-stream operators.
	 */  
	enum OperatorKind {
		LO_STREAM_SOURCE,
		LO_RELN_SOURCE,
		LO_ROW_WIN,
		LO_RANGE_WIN,
		LO_NOW_WIN,
		LO_PARTN_WIN,
		LO_SELECT,
		LO_PROJECT,
		LO_CROSS,
		LO_GROUP_AGGR,
		LO_DISTINCT,
		LO_ISTREAM,
		LO_DSTREAM,
		LO_RSTREAM,
		LO_STREAM_CROSS,
		LO_UNION,
		LO_EXCEPT
	};
	
	static const unsigned int MAX_INPUT_OPS   = 10;
	static const unsigned int MAX_GROUP_ATTRS = 10;
	static const unsigned int MAX_AGGR_ATTRS  = 10;
	
	struct Operator {		
		OperatorKind      kind;
		
		// Output schema of the operator
		Attr              outAttrs[MAX_ATTRS];
		unsigned int      numOutAttrs;
		
		// Does the operator produce a stream.
		bool              bStream;
		
		// Input / output operators.
		Operator         *output;
		Operator         *inputs [MAX_INPUT_OPS];
		unsigned int      numInputs;
		
#ifdef _DM_
		unsigned int      id;
#endif		
		
		union {
			struct {
				unsigned int varId;
				unsigned int strId;
			} STREAM_SOURCE;
			
			struct {
				unsigned int varId;
				unsigned int relId;
			} RELN_SOURCE;
			
			struct {
				unsigned int timeUnits;
                unsigned int slideUnits;
			} RANGE_WIN;
			
			struct {
				unsigned int numRows;
			} ROW_WIN;
			
			struct {
				Attr         partnAttrs[MAX_GROUP_ATTRS];
				unsigned int numPartnAttrs;
				unsigned int numRows;
			} PARTN_WIN;
			
			struct {
				BExpr        bexpr;
			} SELECT;
			
			struct {
				Expr        *projExprs[MAX_ATTRS];
			} PROJECT;
			
			struct {
				Attr         groupAttrs[MAX_GROUP_ATTRS];
				Attr         aggrAttrs[MAX_AGGR_ATTRS];
				AggrFn       fn[MAX_AGGR_ATTRS];
				unsigned int numGroupAttrs;
				unsigned int numAggrAttrs;
			} GROUP_AGGR;
		} u;
	};
	
	//----------------------------------------------------------------------
	// Operator constructors.
	//----------------------------------------------------------------------
	void reset_op_pool();
	
	// Stream & relation sources
	Operator *mk_stream_source(unsigned int varId, unsigned int strId);
	Operator *mk_reln_source(unsigned int varId, unsigned int relId);
	
	// Windows 
	Operator *mk_row_window(Operator *input, unsigned int numRows);
	Operator *mk_range_window(Operator *input, unsigned int timeUnits, 
      unsigned int slideUnits);
	Operator *mk_now_window(Operator *input);
	Operator *mk_partn_window(Operator *input, unsigned int numRows);
	Operator *partn_window_add_attr(Operator *pwin, Attr attr);
	
	// Relational operators
	Operator *mk_select(Operator *input, BExpr bexpr);  
	Operator *mk_project(Operator *input);
	Operator *add_project_expr(Operator *proj, Expr *expr);  
	Operator *mk_cross();
	Operator *cross_add_input(Operator *cross, Operator *input);
	Operator *mk_group_aggr(Operator *input);
	Operator *add_group_attr(Operator *gbyop, Attr attr);
	Operator *add_aggr (Operator *gbyop, AggrFn fn, Attr aggrAttr);
	Operator *mk_distinct (Operator *input);
	
	// Xstream
	Operator *mk_istream(Operator *input);
	Operator *mk_dstream(Operator *input);
	Operator *mk_rstream(Operator *input);

	Operator *mk_union (Operator *left, Operator *right);
	Operator *mk_except (Operator *left, Operator *right);
	
	// Special Stream-based operations
	Operator *mk_stream_cross(Operator *streamInput);
	Operator *stream_cross_add_input (Operator *scross, Operator *relInput);
	
	// Update the output schema of an operator based on its input
	// operator. Propagate the changes up the tree.  This function is
	// called from within the transformations: [[ Explanation ]]
	int update_schema_recursive (Operator *op);
	
	// Update bStream [[Explanation]]
	int update_stream_property_recursive (Operator *op);
	
	// [[ Explanation ]]
	bool check_reference (const Expr *expr, const Operator *op);
	bool check_reference (const Attr &attr, const Operator *op);
	
	//----------------------------------------------------------------------
	// Expression constructors
	//----------------------------------------------------------------------
	void reset_expr_pool();
	
	Expr *mk_const_int_expr (int ival);
	Expr *mk_const_float_expr (float fval);
	Expr *mk_const_byte_expr (char bval);
	Expr *mk_const_char_expr (const char *sval);
	
	Expr *mk_attr_expr (Attr attr, Type type);
	Expr *mk_comp_expr (ArithOp op, Expr *left, Expr *right);
	
	void set_table_mgr (Metadata::TableManager *_tableMgr);
}

#endif
