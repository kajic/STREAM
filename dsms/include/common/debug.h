#ifndef _DEBUG_
#define _DEBUG_

#ifdef _DM_
#include <assert.h>
#define ASSERT(x) assert(x)
#else
#define ASSERT(x) {}
#endif

#endif
