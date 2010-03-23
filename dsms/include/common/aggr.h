/**
 * @file            aggr.h
 * @date            May 11, 2004
 * @brief           Aggregation functions supported by the system
 */

#ifndef _AGGR_
#define _AGGR_

#ifndef  _TYPES_
#include "common/types.h"
#endif

enum AggrFn {
	COUNT, SUM, AVG, MAX, MIN
};

Type getOutputType(AggrFn fn, Type inputType);

#endif
