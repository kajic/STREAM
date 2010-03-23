
#ifndef _BEVAL_
#include "execution/internals/beval.h"
#endif

#ifndef _DEBUG_
#include "common/debug.h"
#endif

using namespace Execution;

BEval::BEval ()
{
	numInstrs = 0;
	roles = 0;
}

BEval::~BEval () {

	for (int i = 0 ; i < numInstrs ; i++) {
		if (instrs[i].e1)
			delete instrs[i].e1;
		if (instrs[i].e2)
			delete instrs[i].e2;
	}
}

int BEval::setEvalContext (EvalContext *evalContext)
{
	int rc;
	ASSERT (evalContext);
	
	roles = evalContext -> roles;

	for (unsigned int i = 0 ; i < numInstrs ; i++) {
		if (instrs[i].e1) 
			if ((rc = instrs[i].e1 -> setEvalContext (evalContext)) != 0) 
				return rc;
		
		if (instrs[i].e2) 
			if ((rc = instrs[i].e2 -> setEvalContext (evalContext)) != 0)
				return rc;
	}
	
	return 0;
}

int BEval::addInstr (BInstr instr)
{
	ASSERT (numInstrs <= MAX_INSTRS);

	if (numInstrs == MAX_INSTRS)
		return -1;

	instrs [numInstrs ++] = instr;
	return 0;
}
