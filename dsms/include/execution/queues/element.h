#ifndef _ELEMENT_
#define _ELEMENT_

#ifndef _TUPLE_
#include "execution/internals/tuple.h"
#endif

#ifndef _TYPES_
#include "common/types.h"
#endif

namespace Execution {
	enum ElementKind {
		E_PLUS, E_MINUS, E_HEARTBEAT
	};
	
	struct Element {
		ElementKind  kind;
		Tuple        tuple;
		Timestamp    timestamp;

		Element () : kind (E_PLUS), tuple (0), timestamp (0) {}
		
		Element (ElementKind _k, Tuple _t, Timestamp _ts)
			: kind (_k), tuple (_t), timestamp (_ts) {}
		
		static Element Heartbeat (Timestamp _tstamp) {
			Element e;
			
			e.kind = E_HEARTBEAT;
			e.timestamp = _tstamp;
			
			return e;
		}		
	};
}

#endif
