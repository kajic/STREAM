/*
 * nodes.cc: functions to allocate an initialize parse-tree nodes
 *
 * Modfied for STREAM: Shivnath Babu, Arvind Arasu
 *
 * Modified for Redbase; Dallan Quass
 *                       Jan Jannink
 *
 * originally by: Mark McAuliffe, University of Wisconsin - Madison, 1991
 */

#include <iostream>
#include <stdio.h>
#include <stdlib.h>
#include "parser/scan.h"
#include "parser/nodes.h"

using namespace Parser;
using namespace std;

/*
 * total number of nodes available for a given parse-tree
 */
#define MAXNODE		100

static NODE nodepool[MAXNODE];
static int nodeptr = 0;

/*
 * reset_parser: resets the scanner and parser when a syntax error occurs
 *
 * No return value
 */
void reset_parser(void)
{
	reset_scanner();
	nodeptr = 0;
}

static void (*cleanup_func)() = 0;

/*
 * init_parser: prepares for a new parsing by releasing resources from the
 * previous parses.
 *
 * No return value.
 */
void init_parser()
{
	nodeptr = 0;
	reset_charptr();
	if(cleanup_func != 0)
		(*cleanup_func)();
}

void register_cleanup_function(void (*func)())
{
	cleanup_func = func;
}

/*
 * newnode: allocates a new node of the specified kind and returns a pointer
 * to it on success.  Returns 0 on error.
 */
NODE *newnode(NODEKIND kind)
{
	NODE *n;
  
	/* if we've used up all of the nodes then error */
	if(nodeptr == MAXNODE){
		fprintf(stderr, "out of memory\n");
		exit(1);
	}
  
	/* get the next node */
	n = nodepool + nodeptr;
  
	++nodeptr;
  
	/* initialize the `kind' field */
	n -> kind = kind;
	return n;
}


/**
 * Creates a new list node
 */
NODE *list_node(NODE *n)
{
	NODE *list = newnode(N_LIST);
  
	list -> u.LIST.curr = n;
	list -> u.LIST.next = 0;
	return list;
}


/*
 * prepends node n onto the front of list.
 *
 * Returns the resulting list.
 */
NODE *prepend(NODE *n, NODE *list)
{
	NODE *newlist = newnode(N_LIST);
  
	newlist -> u.LIST.curr = n;
	newlist -> u.LIST.next = list;
	return newlist;
}

// CONT_QUERY
NODE *sfw_cont_query_node(NODE *sfw_block, NODE *reltostr_op) {
	NODE *n = newnode(N_CONT_QUERY);
	
	n -> u.CONT_QUERY.sfw_block = sfw_block;
	n -> u.CONT_QUERY.bin_op = 0;
	n -> u.CONT_QUERY.reltostr_op = reltostr_op;
	
	return n;
}

NODE *bin_cont_query_node (NODE *bin_op, NODE *reltostr_op)
{
	NODE *n = newnode (N_CONT_QUERY);

	n -> u.CONT_QUERY.sfw_block = 0;
	n -> u.CONT_QUERY.bin_op = bin_op;
	n -> u.CONT_QUERY.reltostr_op = reltostr_op;

	return n;
}

// XSTREAM
NODE *istream_node() {
	NODE *n = newnode(N_XSTREAM);

	n -> u.XSTREAM.op = Parser::ISTREAM;
  
	return n;
}

NODE *dstream_node() {
	NODE *n = newnode(N_XSTREAM);

	n -> u.XSTREAM.op = Parser::DSTREAM;
  
	return n;
}

NODE *rstream_node() {
	NODE *n = newnode(N_XSTREAM);

	n -> u.XSTREAM.op = Parser::RSTREAM;
  
	return n;
}

// SFW_BLOCK
NODE *sfw_block_node(NODE *select_clause, NODE *rel_list,     
					 NODE *cond_list, NODE *group_list) {
	NODE *n = newnode(N_SFW_BLOCK);
  
	n -> u.SFW_BLOCK.select_clause = select_clause;
	n -> u.SFW_BLOCK.rel_list = rel_list;
	n -> u.SFW_BLOCK.condition_list = cond_list;
	n -> u.SFW_BLOCK.group_list = group_list;
  
	return n;
}

NODE *select_clause_node(bool b_distinct, NODE *proj_term_list) {
	NODE *n = newnode(N_SELECT_CLAUSE);
  
	n -> u.SELECT_CLAUSE.b_distinct = b_distinct;
	n -> u.SELECT_CLAUSE.proj_term_list = proj_term_list;
  
	return n;
}

// REL_VAR
NODE *rel_var_node(char *rel_name, char *var_name, NODE *window_spec) {
	NODE *n = newnode(N_REL_VAR);

	n -> u.REL_VAR.rel_name = rel_name;
	n -> u.REL_VAR.var_name = var_name;
	n -> u.REL_VAR.window_spec = window_spec;
  
	return n;
}

// ATTR_REF
NODE *attr_ref_node(char *rel_var_name, char *attr_name) {
	NODE *n = newnode(N_ATTR_REF);
  
	n -> u.ATTR_REF.rel_var_name = rel_var_name;
	n -> u.ATTR_REF.attr_name = attr_name;
  
	return n;
}

// CONST_VAL
NODE *int_val_node(int ival) {
	NODE *n = newnode(N_CONST_VAL);
  
	n -> u.CONST_VAL.type = INT;
	n -> u.CONST_VAL.ival = ival;
  
	return n;
}

NODE *flt_val_node(float fval) {
	NODE *n = newnode(N_CONST_VAL);
  
	n -> u.CONST_VAL.type = FLOAT;
	n -> u.CONST_VAL.fval = fval;
  
	return n;
}

NODE *str_val_node(char *sval) {
	NODE *n = newnode(N_CONST_VAL);
  
	n -> u.CONST_VAL.type = CHAR;
	n -> u.CONST_VAL.sval = sval;
  
	return n;
}

// ARITH_EXPR
NODE *arith_expr_node(ArithOp op, NODE *left_expr, NODE *right_expr) {  
	NODE *n = newnode(N_ARITH_EXPR);
  
	n -> u.ARITH_EXPR.op = op;
	n -> u.ARITH_EXPR.left_expr = left_expr;
	n -> u.ARITH_EXPR.right_expr = right_expr;
  
	return n;
}

// AGGR_EXPR
NODE *aggr_expr_node(AggrFn fn, NODE *attr) {
	NODE *n = newnode(N_AGGR_EXPR);
  
	n -> u.AGGR_EXPR.fn = fn;
	n -> u.AGGR_EXPR.attr = attr;
  
	return n;
}

// CONDN_NODE
NODE *condn_node(CompOp op, NODE *left_expr, NODE *right_expr) {
	NODE *n = newnode(N_CONDN);
  
	n -> u.CONDN.op = op;
	n -> u.CONDN.left_expr = left_expr;
	n -> u.CONDN.right_expr = right_expr;
  
	return n;
}

// WINDOW_SPEC
NODE *time_win_node(NODE *time_spec) {
	NODE *n = newnode(N_WINDOW_SPEC);
  
	n -> u.WINDOW_SPEC.type = RANGE;
    n -> u.WINDOW_SPEC.slide_spec = 0;
	n -> u.WINDOW_SPEC.num_rows = 0;
	n -> u.WINDOW_SPEC.time_spec = time_spec;
	n -> u.WINDOW_SPEC.part_win_attr_list = 0;

	return n;
}

NODE *time_slide_win_node(NODE *time_spec, NODE *slide_spec) {
	NODE *n = newnode(N_WINDOW_SPEC);
  
	n -> u.WINDOW_SPEC.type = RANGE;
    n -> u.WINDOW_SPEC.slide_spec = slide_spec;
	n -> u.WINDOW_SPEC.num_rows = 0;
	n -> u.WINDOW_SPEC.time_spec = time_spec;
	n -> u.WINDOW_SPEC.part_win_attr_list = 0;

	return n;
}

NODE *row_win_node(int num_rows) {
	NODE *n = newnode(N_WINDOW_SPEC);
  
	n -> u.WINDOW_SPEC.type = ROW;
    n -> u.WINDOW_SPEC.slide_spec = 0;
	n -> u.WINDOW_SPEC.num_rows = num_rows;
	n -> u.WINDOW_SPEC.time_spec = 0;
	n -> u.WINDOW_SPEC.part_win_attr_list = 0;
  
	return n;
}


NODE *part_win_node(NODE *part_win_attr_list, int num_rows) {
	NODE *n = newnode(N_WINDOW_SPEC);
  
	n -> u.WINDOW_SPEC.type = PARTITION;
    n -> u.WINDOW_SPEC.slide_spec = 0;
	n -> u.WINDOW_SPEC.num_rows = num_rows;
	n -> u.WINDOW_SPEC.time_spec = 0;
	n -> u.WINDOW_SPEC.part_win_attr_list = part_win_attr_list;
  
	return n;
}

NODE *now_win_node() {
	NODE *n = newnode(N_WINDOW_SPEC);
  
	n -> u.WINDOW_SPEC.type = NOW;
    n -> u.WINDOW_SPEC.slide_spec = 0;
	n -> u.WINDOW_SPEC.num_rows = 0;
	n -> u.WINDOW_SPEC.time_spec = 0;
	n -> u.WINDOW_SPEC.part_win_attr_list = 0;
  
	return n;
}

NODE *unbounded_win_node() {
	NODE *n = newnode(N_WINDOW_SPEC);
  
	n -> u.WINDOW_SPEC.type = UNBOUNDED;
    n -> u.WINDOW_SPEC.slide_spec = 0;
	n -> u.WINDOW_SPEC.num_rows = 0;
	n -> u.WINDOW_SPEC.time_spec = 0;
	n -> u.WINDOW_SPEC.part_win_attr_list = 0;
  
	return n;
}

// TIME_SPEC
NODE *time_spec_node(Parser::TimeUnit unit, int len) {
	NODE *n = newnode(N_TIME_SPEC);
  
	n -> u.TIME_SPEC.unit = unit;
	n -> u.TIME_SPEC.len = len;
  
	return n;
}

// REL_SPEC
NODE *rel_spec_node(char *rel_name, NODE *attr_list) {
	NODE *n = newnode(N_REL_SPEC);
  
	n -> u.REL_SPEC.rel_name = rel_name;
	n -> u.REL_SPEC.attr_list = attr_list;
  
	return n;
}

// STR_SPEC
NODE *str_spec_node(char *str_name, NODE *attr_list) {
	NODE *n = newnode(N_STR_SPEC);
  
	n -> u.STR_SPEC.str_name = str_name;
	n -> u.STR_SPEC.attr_list = attr_list;
  
	return n;
}

// ATTR_SPEC
NODE *int_attr_spec_node(char *attr_name) {
	NODE *n = newnode(N_ATTR_SPEC);
  
	n -> u.ATTR_SPEC.attr_name = attr_name;
	n -> u.ATTR_SPEC.type = INT;
	n -> u.ATTR_SPEC.len = 0;

	return n;
}

NODE *float_attr_spec_node(char *attr_name) {
	NODE *n = newnode(N_ATTR_SPEC);
  
	n -> u.ATTR_SPEC.attr_name = attr_name;
	n -> u.ATTR_SPEC.type = FLOAT;
	n -> u.ATTR_SPEC.len = 0;

	return n;
}

NODE *byte_attr_spec_node(char *attr_name) {
	NODE *n = newnode(N_ATTR_SPEC);
  
	n -> u.ATTR_SPEC.attr_name = attr_name;
	n -> u.ATTR_SPEC.type = BYTE;
	n -> u.ATTR_SPEC.len = 0;

	return n;
}

NODE *char_attr_spec_node(char *attr_name, int len) {
	NODE *n = newnode(N_ATTR_SPEC);
  
	n -> u.ATTR_SPEC.attr_name = attr_name;
	n -> u.ATTR_SPEC.type = CHAR;
	n -> u.ATTR_SPEC.len = len;

	return n;
}

NODE *union_node (char *left_table, char *right_table)
{
	NODE *n = newnode (N_UNION);

	n -> u.UNION.left_table = left_table;
	n -> u.UNION.right_table = right_table;
	
	return n;
}

NODE *except_node (char *left_table, char *right_table)
{
	NODE *n = newnode (N_EXCEPT);
	
	n -> u.UNION.left_table = left_table;
	n -> u.UNION.right_table = right_table;
	
	return n;
}
