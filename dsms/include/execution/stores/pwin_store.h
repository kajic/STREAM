#ifndef _PWIN_STORE_
#define _PWIN_STORE_

#ifndef _TUPLE_
#include "execution/internals/tuple.h"
#endif

namespace Execution {
	
	class PwinStore {
	public:
		
		virtual int insertTuple_p (Tuple tuple, 
								   unsigned int synId) = 0;
		
		virtual int deleteOldestTuple_p (Tuple partnSpec,
										 Tuple &oldestTuple,
										 unsigned int synId) = 0;
		
		virtual int getPartnSize_p (Tuple partnSpec,
									unsigned int &partnSize,
									unsigned int synId) = 0;		
	};
}

#endif
