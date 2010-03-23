#ifndef _EVAL_CONTEXT_
#include "execution/internals/eval_context.h"
#endif

#ifndef _DEBUG_
#include "common/debug.h"
#endif

using namespace Execution;

void EvalContext::bind (Tuple tuple, unsigned int role)
{
	ASSERT (role < MAX_ROLES);
	
	roles [role] = tuple;
}
