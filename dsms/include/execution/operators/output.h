#ifndef _OUTPUT_
#define _OUTPUT_

#include <ostream>

#ifndef _OPERATOR_
#include "execution/operators/operator.h"
#endif

#ifndef _QUEUE_
#include "execution/queues/queue.h"
#endif

#ifndef _QUERY_OUTPUT_
#include "interface/query_output.h"
#endif

#ifndef _STORE_ALLOC_
#include "execution/stores/store_alloc.h"
#endif

#ifndef _TYPES_
#include "common/types.h"
#endif

#ifndef _CONSTANTS_
#include "common/constants.h"
#endif

namespace Execution {
	class Output : public Operator {
	private:
		unsigned int   id;
		Queue         *inputQueue;
		
		StorageAlloc *inStore;
		
		// My internal representation of an attribute: obvious semantics:
		struct Attr {
			Type type;
			unsigned int len;
		};
		
		//----------------------------------------------------------------------
		// Schema specification of the stream/relation that I am
		// outputting.
		//----------------------------------------------------------------------
		
		// Number of attributes in the schema
		unsigned int numAttrs;
		
		// Details of attributes.
		Attr attrs [MAX_ATTRS];
		
		// Columns used to access the attributes in the input tuples 
		Column inCols [MAX_ATTRS];
		
		//----------------------------------------------------------------------
		// Encoding details
		//----------------------------------------------------------------------

		// Buffer that I use to encode output tuples
		static const unsigned MAX_BUFFER_SIZE = 512;
		char buffer [MAX_BUFFER_SIZE];		
		
		// Length of the tuples that I encode & send to the output.  It is
		// fixed since we do not have varying length attributes
		unsigned int tupleLen;
		
		// Offsets where the attributes are encoded
		unsigned int offsets [MAX_ATTRS];
		
		// timestamp appears first ...
		static const unsigned int TIMESTAMP_OFFSET = 0;
		
		// ... effect comes after timestamp ...
		static const unsigned int EFFECT_OFFSET = TIMESTAMP_SIZE;

		// Size of the effect
		static const unsigned int EFFECT_SIZE = sizeof(char);
		
		// ... then come the data attributes.
		static const unsigned int DATA_OFFSET = TIMESTAMP_SIZE + EFFECT_SIZE;
		
        // How do I encode PLUS / MINUS (specified in the QueryOutput interface)
		static const int PLUS_ENCODING  = 1;
		static const int MINUS_ENCODING = 2;
		
		//----------------------------------------------------------------------
		
		// Object that accepts my output.
		Interface::QueryOutput *output;
		
#ifdef _DM_
		Timestamp   lastInputTs;
#endif

		std::ostream& LOG;
		
	public:
		Output(unsigned int id, std::ostream& LOG);
		virtual ~Output();
		
		//----------------------------------------------------------------------
		// Initialization methods
		//----------------------------------------------------------------------		
		int setInputQueue (Queue *inputQueue);
		int setInStore (StorageAlloc *store);
		int setQueryOutput (Interface::QueryOutput *output);
		int addAttr(Type type, unsigned int len, Column inCol);		
		int initialize();
		
		int run(TimeSlice timeSlice);		
	};
}

#endif
