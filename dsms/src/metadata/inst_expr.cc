#ifndef _PHY_OP_
#include "metadata/phy_op.h"
#endif

#ifndef _DEBUG_
#include "common/debug.h"
#endif

#ifndef _PLAN_MGR_IMPL_
#include "metadata/plan_mgr_impl.h"
#endif

#ifndef _TUPLE_
#include "execution/internals/tuple.h"
#endif

using namespace Physical;
using namespace Metadata;

using Execution::AEval;
using Execution::BEval;
using Execution::AInstr;
using Execution::BInstr;

int PlanManagerImpl::inst_expr (Expr            *expr,
								unsigned int    *roleMap,
								Operator        *op,
								AEval          *&eval, 
								unsigned int    &outRole,
								unsigned int    &outCol,
								EvalContextInfo &evalCxt)
{
	int rc;
	unsigned int lrole, lcol, rrole, rcol;
	Attr attr;
	AInstr instr;
	
	ASSERT (expr);
	ASSERT (evalCxt.st_layout);
	ASSERT (evalCxt.ct_layout);
	
	if (expr -> kind == CONST_VAL) {
		outRole = CONST_ROLE;
		
		switch (expr -> type) {
		case INT:
			rc = evalCxt.ct_layout -> addInt (expr -> u.ival, outCol);			
			if (rc != 0) return rc;
			
			break;
			
		case FLOAT:
			rc = evalCxt.ct_layout -> addFloat (expr -> u.fval, outCol);
			if (rc != 0) return rc;
			
			break;
			
		case BYTE:
			rc = evalCxt.ct_layout -> addByte (expr -> u.bval, outCol);
			if (rc != 0) return rc;
			
			break;
			
		case CHAR:
			rc = evalCxt.ct_layout -> addChar (expr -> u.sval, outCol);
			if (rc != 0) return rc;
			
			break;
			
#ifdef _DM_
		default:
			ASSERT (0);
			break;
#endif
			
		}
	}
	
	else if (expr -> kind == ATTR_REF) {
		attr = expr -> u.attr;
		outRole = roleMap [attr.input];
		
		if ((rc = TupleLayout::getOpColumn (op->inputs[attr.input],
											attr.pos,
											outCol)) != 0)
			return rc;
	}
	
	else {		
		ASSERT (expr -> kind == COMP_EXPR);		
		
		rc = inst_expr (expr -> u.COMP_EXPR.left, roleMap, op,
						eval, lrole, lcol, evalCxt);		
		if (rc != 0) return rc;
		
		rc = inst_expr (expr -> u.COMP_EXPR.right, roleMap, op,
						eval, rrole, rcol, evalCxt);
		if (rc != 0) return rc;
		
		if (!eval)
			eval = new AEval ();
		
		ASSERT ((expr -> type == INT) || (expr -> type == FLOAT));
		
		outRole = SCRATCH_ROLE;
		if ((rc = evalCxt.st_layout -> addFixedLenAttr (expr -> type,
														outCol)) != 0)
			return rc;
		
		if (expr -> type == INT) {
			switch (expr -> u.COMP_EXPR.op) {
			case ADD: instr.op = Execution::INT_ADD; break;
			case SUB: instr.op = Execution::INT_SUB; break;				
			case MUL: instr.op = Execution::INT_MUL; break;
			case DIV: instr.op = Execution::INT_DIV; break;
#ifdef _DM_
			default: ASSERT (0); break;
#endif
			}			
		}
		
		else if (expr -> type == FLOAT) {
			switch (expr -> u.COMP_EXPR.op) {
			case ADD: instr.op = Execution::FLT_ADD; break;
			case SUB: instr.op = Execution::FLT_SUB; break;				
			case MUL: instr.op = Execution::FLT_MUL; break;
			case DIV: instr.op = Execution::FLT_DIV; break;
#ifdef _DM_
			default: ASSERT (0); break;
#endif
			}
		}
		
#ifdef _DM_
		else {
			ASSERT (0);
		}
#endif
		
		instr.r1 = lrole;
		instr.c1 = lcol;
		instr.r2 = rrole;
		instr.c2 = rcol;
		instr.dr = outRole;
		instr.dc = outCol;
		
		if ((rc = eval -> addInstr (instr)) != 0)
			return rc;
	}
	
	return 0;	
}

int PlanManagerImpl::inst_bexpr (Physical::BExpr *expr,
								 unsigned int *roleMap,
								 Physical::Operator *op,
								 BEval *&beval,
								 EvalContextInfo &evalCxt)
{
	int rc;
	BInstr instr;
	AEval *left_eval;
	AEval *right_eval;
	unsigned int left_role, left_col;
	unsigned int right_role, right_col;
	
	if (!beval)
		beval = new BEval();	
	
	ASSERT (expr -> left);
	ASSERT (expr -> right);
	ASSERT (expr -> left -> type == expr -> right -> type);
	
	left_eval = 0;
	if ((rc = inst_expr (expr -> left, roleMap, op, left_eval,
						 left_role, left_col, evalCxt)) != 0)
		return rc;
	
	right_eval = 0;
	if ((rc = inst_expr (expr -> right, roleMap, op, right_eval,
						 right_role, right_col, evalCxt)) != 0)		
		return rc;
	
	// Determine the operator type
	if (!left_eval && !right_eval) {
		switch (expr -> left -> type) {
		case INT:
			switch (expr -> op) {
			case LT: instr.op = Execution::INT_LT; break;
			case LE: instr.op = Execution::INT_LE; break;
			case GT: instr.op = Execution::INT_GT; break;
			case GE: instr.op = Execution::INT_GE; break;
			case EQ: instr.op = Execution::INT_EQ; break;
			case NE: instr.op = Execution::INT_NE; break;
#ifdef _DM_
			default:
				ASSERT (0);
				break;
#endif
			}
			
			break;
			
		case FLOAT:
			switch (expr -> op) {
			case LT: instr.op = Execution::FLT_LT; break;
			case LE: instr.op = Execution::FLT_LE; break;
			case GT: instr.op = Execution::FLT_GT; break;
			case GE: instr.op = Execution::FLT_GE; break;
			case EQ: instr.op = Execution::FLT_EQ; break;
			case NE: instr.op = Execution::FLT_NE; break;
#ifdef _DM_
			default:
				ASSERT (0);
				break;
#endif
			}
			
			break;

		case CHAR:
			switch (expr -> op) {
			case LT: instr.op = Execution::CHR_LT; break;
			case LE: instr.op = Execution::CHR_LE; break;
			case GT: instr.op = Execution::CHR_GT; break;
			case GE: instr.op = Execution::CHR_GE; break;
			case EQ: instr.op = Execution::CHR_EQ; break;
			case NE: instr.op = Execution::CHR_NE; break;
#ifdef _DM_
			default:
				ASSERT (0);
				break;
#endif
			}
			
			break;
		case BYTE:
			switch (expr -> op) {
			case LT: instr.op = Execution::BYT_LT; break;
			case LE: instr.op = Execution::BYT_LE; break;
			case GT: instr.op = Execution::BYT_GT; break;
			case GE: instr.op = Execution::BYT_GE; break;
			case EQ: instr.op = Execution::BYT_EQ; break;
			case NE: instr.op = Execution::BYT_NE; break;
#ifdef _DM_
			default:
				ASSERT (0);
				break;
#endif
			}
			
			break;

#ifdef _DM_
		default:
			ASSERT (0);
			break;
#endif
			
		}
	}
	
	else {
		switch (expr -> left -> type) {
		case INT:
			switch (expr -> op) {
			case LT: instr.op = Execution::C_INT_LT; break;
			case LE: instr.op = Execution::C_INT_LE; break;
			case GT: instr.op = Execution::C_INT_GT; break;
			case GE: instr.op = Execution::C_INT_GE; break;
			case EQ: instr.op = Execution::C_INT_EQ; break;
			case NE: instr.op = Execution::C_INT_NE; break;
#ifdef _DM_
			default:
				ASSERT (0);
				break;
#endif
			}
			
			break;
			
		case FLOAT:
			switch (expr -> op) {
			case LT: instr.op = Execution::C_FLT_LT; break;
			case LE: instr.op = Execution::C_FLT_LE; break;
			case GT: instr.op = Execution::C_FLT_GT; break;
			case GE: instr.op = Execution::C_FLT_GE; break;
			case EQ: instr.op = Execution::C_FLT_EQ; break;
			case NE: instr.op = Execution::C_FLT_NE; break;
#ifdef _DM_
			default:
				ASSERT (0);
				break;
#endif
			}
			
			break;

		case CHAR:
			switch (expr -> op) {
			case LT: instr.op = Execution::C_CHR_LT; break;
			case LE: instr.op = Execution::C_CHR_LE; break;
			case GT: instr.op = Execution::C_CHR_GT; break;
			case GE: instr.op = Execution::C_CHR_GE; break;
			case EQ: instr.op = Execution::C_CHR_EQ; break;
			case NE: instr.op = Execution::C_CHR_NE; break;
#ifdef _DM_
			default:
				ASSERT (0);
				break;
#endif
			}
			
			break;
		case BYTE:
			switch (expr -> op) {
			case LT: instr.op = Execution::C_BYT_LT; break;
			case LE: instr.op = Execution::C_BYT_LE; break;
			case GT: instr.op = Execution::C_BYT_GT; break;
			case GE: instr.op = Execution::C_BYT_GE; break;
			case EQ: instr.op = Execution::C_BYT_EQ; break;
			case NE: instr.op = Execution::C_BYT_NE; break;
#ifdef _DM_
			default:
				ASSERT (0);
				break;
#endif
			}
			
			break;
			
#ifdef _DM_
		default:
			ASSERT (0);
			break;
#endif
			
		}
	}
	
	instr.r1 = left_role;
	instr.c1 = left_col;
	instr.r2 = right_role;
	instr.c2 = right_col;
	instr.e1 = left_eval;
	instr.e2 = right_eval;
	
	if ((rc = beval -> addInstr (instr)) != 0)
		return rc;	
	
	if (expr -> next) {
		return inst_bexpr (expr -> next, roleMap, op, beval, evalCxt);
	}
	
	return 0;
}

int PlanManagerImpl::inst_expr_dest (Physical::Expr     *expr,
									 unsigned int       *roleMap,
									 Physical::Operator *op,
									 unsigned int        dest_role,
									 unsigned int        dest_col,				
									 Execution::AEval  *&eval,
									 EvalContextInfo    &evalCxt)
{
	int rc;
	unsigned int src_role, src_col;
	unsigned int left_role, right_role, left_col, right_col;
	Attr attr;
	AInstr instr;
	
	ASSERT (dest_role != SCRATCH_ROLE);
	ASSERT (dest_role != CONST_ROLE);
	
	if (!eval)
		eval = new AEval();
	
	if (expr -> kind == CONST_VAL) {
		
		switch (expr -> type) {
		case INT:
			rc = evalCxt.ct_layout -> addInt (expr -> u.ival, src_col);
			if (rc != 0) return rc;
			
			break;
			
		case FLOAT:
			rc = evalCxt.ct_layout -> addFloat (expr -> u.fval, src_col);
			if (rc != 0) return rc;
			
			break;
			
		case BYTE:
			rc = evalCxt.ct_layout -> addByte (expr -> u.bval, src_col);
			if (rc != 0) return rc;
			
			break;
			
		case CHAR:
			rc = evalCxt.ct_layout -> addChar (expr -> u.sval, src_col);
			if (rc != 0) return rc;
			
			break;
			
#ifdef _DM_
		default:
			ASSERT (0);
			break;
#endif			
		}		
		
		switch (expr -> type) {
		case INT:   instr.op = Execution::INT_CPY; break;			
		case FLOAT: instr.op = Execution::FLT_CPY; break;
		case CHAR:  instr.op = Execution::CHR_CPY; break;
		case BYTE:  instr.op = Execution::BYT_CPY; break;
#ifdef _DM_
		default:
			ASSERT (0);
			break;
#endif
		}
		
		instr.r1 = CONST_ROLE;
		instr.c1 = src_col;
		instr.r2 = 0;
		instr.c2 = 0;
		instr.dr = dest_role;
		instr.dc = dest_col;
		
		if ((rc = eval -> addInstr (instr)) != 0)
			return rc;
	}
	
	else if (expr -> kind == ATTR_REF) {
		attr = expr -> u.attr;
		
		src_role = roleMap [attr.input];
		
		if ((rc = TupleLayout::getOpColumn (op -> inputs [attr.input],
											attr.pos,
											src_col)) != 0)
			return rc;
		
		switch (expr -> type) {
		case INT:   instr.op = Execution::INT_CPY; break;			
		case FLOAT: instr.op = Execution::FLT_CPY; break;
		case CHAR:  instr.op = Execution::CHR_CPY; break;
		case BYTE:  instr.op = Execution::BYT_CPY; break;
#ifdef _DM_
		default:
			ASSERT (0);
			break;
#endif
		}
		
		instr.r1 = src_role;
		instr.c1 = src_col;
		instr.r2 = 0;
		instr.c2 = 0;
		instr.dr = dest_role;
		instr.dc = dest_col;
		
		if ((rc = eval -> addInstr (instr)) != 0)
			return rc;
	}
	
	else {
		ASSERT (expr -> kind == COMP_EXPR);
		
		if ((rc = inst_expr (expr -> u.COMP_EXPR.left, roleMap, op, eval, 
							 left_role, left_col, evalCxt)) != 0)
			return rc;
		if ((rc = inst_expr (expr -> u.COMP_EXPR.right, roleMap, op, eval, 
							 right_role, right_col, evalCxt)) != 0)
			return rc;
		
		if (expr -> type == INT) {
			switch (expr -> u.COMP_EXPR.op) {
			case ADD: instr.op = Execution::INT_ADD; break;
			case SUB: instr.op = Execution::INT_SUB; break;				
			case MUL: instr.op = Execution::INT_MUL; break;
			case DIV: instr.op = Execution::INT_DIV; break;
#ifdef _DM_
			default: ASSERT (0); break;
#endif
			}			
		}
		
		else if (expr -> type == FLOAT) {
			switch (expr -> u.COMP_EXPR.op) {
			case ADD: instr.op = Execution::FLT_ADD; break;
			case SUB: instr.op = Execution::FLT_SUB; break;				
			case MUL: instr.op = Execution::FLT_MUL; break;
			case DIV: instr.op = Execution::FLT_DIV; break;
#ifdef _DM_
			default: ASSERT (0); break;
#endif
			}
		}
		
#ifdef _DM_
		else {
			ASSERT (0);
		}
#endif
		
		instr.r1 = left_role;
		instr.c1 = left_col;
		instr.r2 = right_role;
		instr.c2 = right_col;
		instr.dr = dest_role;
		instr.dc = dest_col;
		
		if ((rc = eval -> addInstr (instr)) != 0)
			return rc;		
	}	
	
	return 0;
}
