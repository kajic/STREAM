#ifndef _AEVAL_
#include "execution/internals/aeval.h"
#endif

#ifndef _DEBUG_
#include "common/debug.h"
#endif

using namespace Execution;

AEval::AEval ()
{
	numInstrs = 0;
	roles = 0;
}

AEval::~AEval () {}

int AEval::addInstr (AInstr instr)
{
	ASSERT (numInstrs <= MAX_INSTRS);
	
	if (numInstrs == MAX_INSTRS)
		return -1;

	instrs [numInstrs ++] = instr;
	return 0;
}

int AEval::setEvalContext (EvalContext *evalContext)
{
	ASSERT (evalContext);
	
	this -> roles = evalContext -> roles;
	return 0;
}

