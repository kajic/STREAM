#ifndef _NODES_DEBUG_
#define _NODES_DEBUG_

#include "parser/nodes.h"

//----------------------------------------------------------------------
// Print routines for debugging
//----------------------------------------------------------------------
void print_list(NODE *listnode);
void print_cont_query_node(NODE *cont_query_node);
void print_xstream_node(NODE *xstream_node);
void print_sfw_block_node(NODE *sfw_block_node);
void print_select_clause_node(NODE *select_clause_node);
void print_rel_var_node(NODE *rel_var_node);
void print_attr_ref_node(NODE *attr_ref_node);
void print_const_val_node(NODE *const_val_node);
void print_arith_expr_node(NODE *arith_expr_node);
void print_aggr_expr_node(NODE *aggr_expr_node);
void print_condn_node(NODE *condn_node);
void print_window_spec_node(NODE *win_spec_node);
void print_time_spec_node(NODE *time_spec_node);
void print_rel_spec_node(NODE *rel_spec_node);
void print_str_spec_node(NODE *str_spec_node);
void print_attr_spec_node(NODE *attr_spec_node);
void print_binary_op_node (NODE *binary_op_node);

#endif
