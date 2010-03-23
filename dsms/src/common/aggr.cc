
#ifndef _AGGR_
#include "common/aggr.h"
#endif

Type getOutputType(AggrFn fn, Type inputType)
{
	return (fn == AVG)? FLOAT : inputType;
}
