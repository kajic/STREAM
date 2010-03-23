#ifndef _RANGE_WIN_
#define _RANGE_WIN_

#ifndef _OPERATOR_
#include "execution/operators/operator.h"
#endif

#ifndef _QUEUE_
#include "execution/queues/queue.h"
#endif

#ifndef _WIN_SYN_
#include "execution/synopses/win_syn.h"
#endif

#ifndef _STORE_ALLOC_
#include "execution/stores/store_alloc.h"
#endif

#ifndef _CPP_OSTREAM_
#include <ostream>
#endif

namespace Execution {
	class RangeWindow : public Operator {
	private:
		/// system wide id
		unsigned int id;

		/// System log
		std::ostream &LOG;
		
		Queue          *inputQueue;
		Queue          *outputQueue;
        Timestamp       windowStart;
		TimeDuration    windowSize;
        TimeDuration    strideSize;
		WindowSynopsis *winSynopsis;
		StorageAlloc   *inStore;
		
		// Used for heartbeats
		Timestamp       lastInputTs;
		Timestamp       lastOutputTs;
		
		// Used to handle stalls.  [[ Explanation ]]
		bool            bStalled;
		Element         stalledElement;
		
	public:		
		RangeWindow (unsigned int id, std::ostream &LOG);
		virtual ~RangeWindow ();
		
		int setInputQueue (Queue *inputQueue);
		int setOutputQueue (Queue *outputQueue);
		int setInStore (StorageAlloc *inStore);
		int setWindowSize (unsigned int windowSize);
        int setWindowStride (unsigned int strideSize);
		int setWindowSynopsis (WindowSynopsis *winSynopsis);
		
		int run(TimeSlice timeSlice);
		
	private:
		int clearStall();
		int expireTuples(Timestamp expTs);
        int strideExpireTuples();
	};
}

#endif
