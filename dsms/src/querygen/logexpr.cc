/**
 * @file        logexpr.cc
 * @date        May 26, 2004
 * @brief       Routines to manage expressions used by Logical plan
 *              generator  (Logical namespace)
 */

#ifndef _DEBUG_
#include "common/debug.h"
#endif

#ifndef _LOGOP_
#include "querygen/logop.h"
#endif


#define MAXEXPR      500

using namespace Logical;

static Expr expr_pool[MAXEXPR];
static int expr_ptr = 0;

void Logical::reset_expr_pool()
{
	expr_ptr = 0;
}

static Expr *new_expr (ExprKind kind)
{
	Expr *expr;
	
	if (expr_ptr == MAXEXPR)
		return 0;
	
	expr = expr_pool + expr_ptr;
	expr_ptr ++;
	
	expr -> kind = kind;
	
	return expr;
}

Expr *Logical::mk_const_int_expr (int ival)
{
	Expr *expr;
	
	expr = new_expr (CONST_VAL);
	if (!expr)
		return 0;
	
	expr -> type = INT;
	expr -> u.ival = ival;
	
	return expr;
}

Expr *Logical::mk_const_float_expr (float fval)
{
	Expr *expr;
	
	expr = new_expr (CONST_VAL);
	if (!expr)
		return 0;
	
	expr -> type = FLOAT;
	expr -> u.fval = fval;
	
	return expr;
}

Expr *Logical::mk_const_byte_expr (char bval)
{
	Expr *expr;
	
	expr = new_expr (CONST_VAL);
	if (!expr)
		return 0;
	
	expr -> type = BYTE;
	expr -> u.bval = bval;
	
	return expr;
}	

Expr *Logical::mk_const_char_expr (const char *sval)
{
	Expr *expr;
	
	expr = new_expr (CONST_VAL);
	if (!expr)
		return 0;
	
	expr -> type = CHAR;
	expr -> u.sval = sval;
	
	return expr;
}

Expr *Logical::mk_attr_expr (Attr attr, Type type)
{
	Expr *expr;
	
	expr = new_expr (ATTR_REF);
	if (!expr)
		return 0;
	
	expr -> type = type;
	expr -> u.attr = attr;
	return expr;	
}


Expr *Logical::mk_comp_expr (ArithOp op, Expr *left, Expr *right)
{
	Expr *expr;

	ASSERT (left -> type == right -> type);
	
	expr = new_expr (COMP_EXPR);
	if (!expr)
		return 0;
	
	expr -> type              = left -> type;
	expr -> u.COMP_EXPR.op    = op;
	expr -> u.COMP_EXPR.left  = left;
	expr -> u.COMP_EXPR.right = right;
	
	return expr;
}

