/**
 * @file   nodes.h
 * @brief  nodes in the parse tree output by the parser 
 */

#ifndef _NODES_
#define _NODES_

#include "common/aggr.h"
#include "common/op.h"
#include "common/types.h"
#include "common/window.h"

namespace Parser {        
  
	enum RelToStrOp {
		ISTREAM,
		DSTREAM,
		RSTREAM
	};
  
	enum TimeUnit {
		NOTIMEUNIT,
		SECOND,
		MINUTE,
		HOUR,
		DAY
	};
}

enum NODEKIND {
	N_LIST,
	N_CONT_QUERY,
	N_XSTREAM,
	N_SFW_BLOCK,
	N_SELECT_CLAUSE,
	N_REL_VAR,
	N_ATTR_REF,
	N_CONST_VAL,
	N_ARITH_EXPR,
	N_AGGR_EXPR,
	N_CONDN,
	N_WINDOW_SPEC,
	N_TIME_SPEC,
	N_REL_SPEC,
	N_STR_SPEC,
	N_ATTR_SPEC,
	N_UNION,
	N_EXCEPT
};
  
typedef struct node {
	NODEKIND kind;
	
	union{    
		// Generic list node: 
		struct{
			struct node *curr;
			struct node *next;      
		} LIST;    
        
		// Continuous Query: A continuous query is either a
		// select-from-where block or an binary operation (union, except)
		// with an optional relation to stream operator
		struct {
			struct node *sfw_block;
			struct node *bin_op;
			struct node *reltostr_op;
		} CONT_QUERY;
		
		// Xstream (relation-to-stream) operator
		struct {
			Parser::RelToStrOp op;
		} XSTREAM;
		
		// Select from where block: one sub-parse tree for select-clause,
		// project clause, where clause(conditions), and group by clause.
		struct {
			struct node *select_clause;      
			struct node *rel_list;      
			struct node *condition_list;      
			struct node *group_list;            
		} SFW_BLOCK;
		
		// Select clause: optional distinct clause and a list of project or
		// aggregate expressions.
		struct {
			bool         b_distinct;
			struct node *proj_term_list;
		} SELECT_CLAUSE;  
		
		// A relation variable: Each relation (stream) appearing in the FROM
		// clause is a relation variable.
		struct {
			char        *rel_name;
			char        *var_name;
			struct node *window_spec;
		} REL_VAR;
		
		// Attribute reference in a query
		struct {
			char        *rel_var_name;
			char        *attr_name;
		} ATTR_REF;
    
		// Constant value: ival, fval, sval denote integer, float, and string
		// values depending on the type 
		struct {
			Type         type;
			int          ival;
			float        fval;
			char        *sval;
			char         bval;
		} CONST_VAL;
		
		// Arithmetic expression: operation, left arithmetic expression and
		// right subexpression
		struct {
			ArithOp          op;      
			struct node     *left_expr;
			struct node     *right_expr;
		} ARITH_EXPR;
		
		// Aggregation expression: aggr. function over an attribute.
		struct {
			AggrFn                fn;
			struct node          *attr;
		} AGGR_EXPR;
    
		// Single condition in where clause: boolean operaition over two
		// arithmetic expressions.
		struct {
			CompOp               op;
			struct node         *left_expr;
			struct node         *right_expr;
		} CONDN;
    
		// Window specificatin: window type.  Pointer to time-specification
		// for time-based windows and number of rows for row-based and
		// partition windows.  A partition window also has the list of
		// partitioning attributes.
		struct {
			WinType              type;
			int                  num_rows;
			struct node         *time_spec;
            struct node         *stride_spec;
			struct node         *part_win_attr_list;
		} WINDOW_SPEC;
		
		// Time specification for time-based windows.  A timeunit and the
		// length of the time interval in terms of the unit.
		struct {
			Parser::TimeUnit     unit;      
			int                  len;
		} TIME_SPEC;
		
		// Relation specification: relation name and specification of each attribute.
		struct {
			char        *rel_name;
			struct node *attr_list;
		} REL_SPEC;
		
		// Stream specification: stream name and the specification of each attribute.
		struct {
			char        *str_name;
			struct node *attr_list;
		} STR_SPEC;
		
		// Attribute specification: name of teh attribute, type and optionally length.
		struct {
			char         *attr_name;
			Type          type;
			unsigned int  len;
		} ATTR_SPEC;

		struct {
			char *left_table;
			char *right_table;
		} UNION;
		
		struct {
			char *left_table;
			char *right_table;
		} EXCEPT;		
	} u;
} NODE;


void init_parser();

NODE *newnode(NODEKIND kind);

//----------------------------------------------------------------------
// 
// List manipulation routines
//
//----------------------------------------------------------------------

NODE *list_node(NODE *n);

NODE *prepend(NODE *n, NODE *list);


//----------------------------------------------------------------------
//
// Prehistoric ancestors of constructors from the mesozoic ...
//
//----------------------------------------------------------------------

// CONT_QUERY with a select from where block
NODE *sfw_cont_query_node(NODE *sfw_block, NODE *rel2str_op);

// CONT_QUERY with a binary operator
NODE *bin_cont_query_node(NODE *bin_op, NODE *rel2str_op);

// SFW_BLOCK
NODE *sfw_block_node(NODE *select_clause, NODE *rel_list,
					 NODE *cond_list, NODE *group_list);

// XSTREAM
NODE *istream_node();
NODE *dstream_node();
NODE *rstream_node();

// SELECT_CLAUSE
NODE *select_clause_node(bool b_distinct, NODE *proj_term_list);

// REL_VAR
NODE *rel_var_node(char *rel_name, char *var_name, NODE *window_spec);

// ATTR_REF
NODE *attr_ref_node(char *rel_var_name, char *attr_name);

// CONST_VAL
NODE *int_val_node(int ival);
NODE *flt_val_node(float fval);
NODE *str_val_node(char *sval);

// ARITH_EXPR
NODE *arith_expr_node(ArithOp op, NODE *left_expr, NODE *right_expr);		      

// AGGR_EXPR
NODE *aggr_expr_node(AggrFn fn, NODE *attr);

// CONDN
NODE *condn_node(CompOp op, NODE *left_exptr, NODE *right_expr);

// WINDOW_SPEC
NODE *time_win_node(NODE *time_spec);
NODE *time_stride_win_node(NODE *time_spec, NODE *stride_spec);
NODE *row_win_node(int num_rows);
NODE *now_win_node();
NODE *unbounded_win_node();
NODE *part_win_node(NODE *part_win_attr_list, int num_rows);

// TIME_SPEC
NODE *time_spec_node(Parser::TimeUnit unit, int len);

// REL_SPEC
NODE *rel_spec_node(char *rel_name, NODE *attr_list);

// STR_SPEC
NODE *str_spec_node(char *str_name, NODE *attr_list);

// ATTR_SPEC
NODE *int_attr_spec_node(char *attr_name);
NODE *float_attr_spec_node(char *attr_name);
NODE *byte_attr_spec_node(char *attr_name);
NODE *char_attr_spec_node(char *attr_name, int len);

// UNION
NODE *union_node (char *left_table, char *right_table);
NODE *except_node (char *left_table, char *right_table);

#endif
