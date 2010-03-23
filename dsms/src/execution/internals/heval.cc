#ifndef _HEVAL_
#include "execution/internals/heval.h"
#endif

using namespace Execution;

HEval::HEval ()
{
	roles = 0;
	numInstrs = 0;
}

HEval::~HEval () {}

int HEval::setEvalContext (EvalContext *evalContext)
{
	ASSERT (evalContext);

	this -> roles = evalContext -> roles;
	return 0;
}

int HEval::addInstr (HInstr instr)
{
	ASSERT (numInstrs <= MAX_INSTRS);

	if (numInstrs == MAX_INSTRS)
		return -1;
	
	instrs [numInstrs ++] = instr;
	return 0;
}
