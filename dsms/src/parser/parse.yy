%{
/**
 * parser.y: yacc specification for CQL
 *
 * Contributors: 
 *           Mark McAuliffe (University of Wisconsin - Madison, 1991)
 *           Dallan Quass, Jan Jannink, Jason McHugh (for Redbase)
 *           Shivnath Babu, Arvind Arasu (for STREAM)
 *
 */

#include <iostream>
#include <assert.h>
#include "parser/scan.h"
#include "parser/parser.h"
#include "parser/nodes.h"

static NODE* parse_tree; 


%}

%union{
  int   ival;
  float rval;
  char *sval;
  NODE *node;
}

%token RW_REGISTER      
%token RW_STREAM
%token RW_RELATION

%token RW_ISTREAM
%token RW_DSTREAM
%token RW_RSTREAM
%token RW_SELECT
%token RW_DISTINCT
%token RW_FROM
%token RW_WHERE
%token RW_GROUP
%token RW_BY
%token RW_AND
%token RW_AS
%token RW_UNION
%token RW_EXCEPT

%token RW_AVG
%token RW_MIN
%token RW_MAX
%token RW_COUNT
%token RW_SUM

%token RW_ROWS
%token RW_RANGE
%token RW_SLIDE
%token RW_NOW
%token RW_PARTITION
%token RW_UNBOUNDED

%token RW_SECOND
%token RW_MINUTE
%token RW_HOUR
%token RW_DAY

%token T_EQ
%token T_LT
%token T_LE
%token T_GT
%token T_GE
%token T_NE

%token RW_INTEGER
%token RW_FLOAT
%token RW_CHAR
%token RW_BYTE

%token NOTOKEN

%token <ival> T_INT
%token <rval> T_REAL
%token <sval> T_STRING
%token <sval> T_QSTRING

%left               '+' '-'
%left               '*' '/'

%type  <node> command
%type  <node> query
%type  <node> registerstream
%type  <node> registerrelation
%type  <node> non_mt_attrspec_list
%type  <node> attrspec
%type  <node> xstream_clause
%type  <node> sfw_block
%type  <node> select_clause
%type  <node> from_clause
%type  <node> opt_where_clause
%type  <node> opt_group_by_clause
%type  <node> non_mt_projterm_list
%type  <node> projterm
%type  <node> aggr_expr
%type  <node> attr
%type  <node> non_mt_attr_list
%type  <node> non_mt_relation_list
%type  <node> relation_variable
%type  <node> window_type 
%type  <node> time_spec
%type  <node> non_mt_cond_list
%type  <node> condition
%type  <node> arith_expr
%type  <node> const_value
%type  <node> binary_op

%%

start
   : command ';' { parse_tree = $1; YYACCEPT; }   
   ;

command
   : query       
     { $$ = $1;} 

   | registerstream 
     {$$ = $1;}
    
   | registerrelation 
     {$$ = $1;}
   ;


registerstream
   : RW_REGISTER RW_STREAM T_STRING '(' non_mt_attrspec_list ')'
     {$$ = str_spec_node($3, $5);}
   ;

registerrelation
   : RW_REGISTER RW_RELATION T_STRING '(' non_mt_attrspec_list ')'
     {$$ = rel_spec_node($3, $5);}
   ;

non_mt_attrspec_list
   : attrspec ',' non_mt_attrspec_list
     {$$ = prepend($1, $3);}

   | attrspec
     {$$ = list_node($1);}
   ;

attrspec
   : T_STRING RW_INTEGER
     {$$ = int_attr_spec_node($1);}

   | T_STRING RW_FLOAT
     {$$ = float_attr_spec_node($1);}

   | T_STRING RW_BYTE
     {$$ = byte_attr_spec_node($1);}

   | T_STRING RW_CHAR '(' T_INT ')'
     {$$ = char_attr_spec_node($1, $4);}

   ;

query
   : sfw_block
     {$$ = sfw_cont_query_node ($1, 0);}

   | xstream_clause '(' sfw_block ')'
     {$$ = sfw_cont_query_node ($3, $1);}

   | binary_op
     {$$ = bin_cont_query_node ($1, 0);}
    
   | xstream_clause '(' binary_op ')'
     {$$ = bin_cont_query_node ($3, $1);}
   ;

xstream_clause
   : RW_ISTREAM
     {$$ = istream_node();}

   | RW_DSTREAM
     {$$ = dstream_node();}

   | RW_RSTREAM
     {$$ = rstream_node();}
   ;

sfw_block
   : select_clause from_clause opt_where_clause opt_group_by_clause
     {$$ = sfw_block_node($1,$2,$3,$4);}
   ;

select_clause
   : RW_SELECT RW_DISTINCT non_mt_projterm_list
     {$$ = select_clause_node(true, $3);}

   | RW_SELECT non_mt_projterm_list
     {$$ = select_clause_node(false, $2);}

   | RW_SELECT RW_DISTINCT '*'
     {$$ = select_clause_node(true, 0);}

   | RW_SELECT '*'
     {$$ = select_clause_node(false, 0);}
   ;

from_clause
   : RW_FROM non_mt_relation_list
     {$$ = $2;}
   ;

opt_where_clause
   : RW_WHERE non_mt_cond_list
     {$$ = $2;}

   | nothing
     {$$ = 0;}
   ;

opt_group_by_clause
   : RW_GROUP RW_BY non_mt_attr_list
     {$$ = $3;}

   | nothing
     {$$ = 0;}
   ;

non_mt_projterm_list
   : projterm ',' non_mt_projterm_list
     {$$ = prepend($1, $3);}

   | projterm
     {$$ = list_node($1);}
   ;

projterm  
   : arith_expr
     {$$ = $1;}

   | aggr_expr
     {$$ = $1;}
   ;

aggr_expr
   : RW_COUNT '(' attr ')'
     {$$ = aggr_expr_node(COUNT, $3);}

   | RW_COUNT '('  '*' ')'
     {$$ = aggr_expr_node (COUNT, 0);}

   | RW_SUM   '(' attr ')'
     {$$ = aggr_expr_node(SUM, $3);} 

   | RW_AVG   '(' attr ')'
     {$$ = aggr_expr_node(AVG, $3);}

   | RW_MAX   '(' attr ')'
     {$$ = aggr_expr_node(MAX, $3);}

   | RW_MIN   '(' attr ')'
     {$$ = aggr_expr_node(MIN, $3);}
   ;

attr
   : T_STRING '.' T_STRING
     {$$ = attr_ref_node($1, $3);}

   | T_STRING
     {$$ = attr_ref_node(0, $1);}
   ;

non_mt_attr_list
   : attr ',' non_mt_attr_list
     {$$ = prepend($1, $3);}

   | attr
     {$$ = list_node($1);}
   ;

non_mt_relation_list
   : relation_variable ',' non_mt_relation_list
     {$$ = prepend($1, $3);}

   | relation_variable
     {$$ = list_node($1);}
   ;

relation_variable
   : T_STRING '[' window_type ']'
     {$$ = rel_var_node($1, 0, $3);}

   | T_STRING '[' window_type ']' RW_AS T_STRING
     {$$ = rel_var_node($1, $6, $3);}

   | T_STRING
     {$$ = rel_var_node($1, 0, 0);}

   | T_STRING RW_AS T_STRING
     {$$ = rel_var_node($1, $3, 0);}
   ;
  
window_type 
   : RW_RANGE time_spec
     {$$ = time_win_node($2);}

   | RW_RANGE time_spec RW_SLIDE time_spec
     {$$ = time_slide_win_node($2, $4);}

   | RW_NOW
     {$$ = now_win_node();}
      
   | RW_ROWS T_INT
     {$$ = row_win_node($2);}

   | RW_RANGE RW_UNBOUNDED
     {$$ = unbounded_win_node();}

   | RW_PARTITION RW_BY non_mt_attr_list RW_ROWS T_INT
     {$$ = part_win_node($3, $5);}
   ;

time_spec
   : T_INT
     {$$ = time_spec_node(Parser::NOTIMEUNIT, $1);}

   | T_INT RW_SECOND
     {$$ = time_spec_node(Parser::SECOND, $1);}

   | T_INT RW_MINUTE
     {$$ = time_spec_node(Parser::MINUTE, $1);}

   | T_INT RW_HOUR
     {$$ = time_spec_node(Parser::HOUR, $1);}

   | T_INT RW_DAY
     {$$ = time_spec_node(Parser::DAY, $1);}
   ;

non_mt_cond_list
   : condition RW_AND non_mt_cond_list
     {$$ = prepend($1, $3);}

   | condition
     {$$ = list_node($1);}

   ;

condition
   : arith_expr T_LT arith_expr
     {$$ = condn_node(LT, $1, $3);}

   | arith_expr T_LE arith_expr
     {$$ = condn_node(LE, $1, $3);}

   | arith_expr T_GT arith_expr
     {$$ = condn_node(GT, $1, $3);}

   | arith_expr T_GE arith_expr
     {$$ = condn_node(GE, $1, $3);}

   | arith_expr T_EQ arith_expr
     {$$ = condn_node(EQ, $1, $3);}

   | arith_expr T_NE arith_expr
     {$$ = condn_node(NE, $1, $3);}
     
   ;

arith_expr
   : attr
     {$$ = $1;}

   | const_value
     {$$ = $1;}

   | arith_expr '+' arith_expr
     {$$ = arith_expr_node(ADD, $1, $3);}

   | arith_expr '-' arith_expr
     {$$ = arith_expr_node(SUB, $1, $3);}

   | arith_expr '*' arith_expr
     {$$ = arith_expr_node(MUL, $1, $3);}

   | arith_expr '/' arith_expr
     {$$ = arith_expr_node(DIV, $1, $3);}

   | '(' arith_expr ')'
     {$$ = $2;}
   ;

const_value
   : T_QSTRING
     {$$ = str_val_node($1);}

   | T_INT
     {$$ = int_val_node($1);}

   | T_REAL
     {$$ = flt_val_node($1);}
   ;

binary_op
   : T_STRING RW_UNION T_STRING
     {$$ = union_node ($1, $3);}
   | T_STRING RW_EXCEPT T_STRING
     {$$ = except_node ($1, $3);}
   ;

nothing
   : /* epsilon */
   ;

%%

using namespace Parser;

NODE* Parser::parseCommand(const char *queryBuffer, unsigned int queryLen) 
{
  int rc;

  // Initialize the scanner to scan the buffer
  init_scanner(queryBuffer, queryLen);  
  
  // Initialize the parser
  init_parser();
  
  // Parse
  rc = yyparse();
    
  // Error parsing: return a null node
  if(rc != 0)
    return 0;
  
  // return the parse tree
  return parse_tree;  
}

void yyerror(char const *s) {
  std::cerr << "YYError: " << s << std::endl;
}
