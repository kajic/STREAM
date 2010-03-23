#ifndef _STREAM_SOURCE_
#define _STREAM_SOURCE_

/**
 * @file             stream_source.h
 * @date             June 4, 2004
 * @brief            Stream source operator
 */

#include <ostream>

#ifndef _OPERATOR_
#include "execution/operators/operator.h"
#endif

#ifndef _QUEUE_
#include "execution/queues/queue.h"
#endif

#ifndef _TABLE_SOURCE_
#include "interface/table_source.h"
#endif

#ifndef _STORE_ALLOC_
#include "execution/stores/store_alloc.h"
#endif

#ifndef _CONSTANTS_
#include "common/constants.h"
#endif

namespace Execution {
	class StreamSource : public Operator {
	private:
		unsigned int   id;
		
		Queue         *outputQueue;
		StorageAlloc  *storeAlloc;
		
		// Attribute specification: these may not be in the input tuple
		// format, so we do not use the usual AEval for copying ... [[
		// explanation ]]
		struct Attr {
			Type type;
			unsigned int len;
		};
		
		// Specification of the attributes in the input stream
		Attr attrs [MAX_ATTRS];
		unsigned int numAttrs;

		static const unsigned int TIMESTAMP_OFFSET = 0;
		static const unsigned int DATA_OFFSET = TIMESTAMP_SIZE;
		unsigned int offsets [MAX_ATTRS];
		
		// Where do these go in the output ...
		Column outCols [MAX_ATTRS];
		
		// Source who is feeding us the tuples
		Interface::TableSource *source;
		
		Timestamp lastInputTs;	   
		Timestamp lastOutputTs;

		
		std::ostream& LOG;
		
	public:
		StreamSource (unsigned int id, std::ostream& LOG);
		virtual ~StreamSource ();
		
		int setOutputQueue (Queue *outputQueue);
		int setStoreAlloc (StorageAlloc *storeAlloc);
		int setTableSource (Interface::TableSource *tableSource);
		int addAttr (Type type, unsigned int len, Column outCol);
		
		int initialize ();
		
		int run (TimeSlice timeSlice);
	};
}

#endif
