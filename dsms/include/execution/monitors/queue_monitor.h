#ifndef _QUEUE_MONITOR_
#define _QUEUE_MONITOR_

#ifndef _PROPERTY_MONITOR_
#include "execution/monitors/property_monitor.h"
#endif

#ifndef _TYPES_
#include "common/types.h"
#endif

namespace Monitor {

	class QueueMonitor : public PropertyMonitor {
	private:
		/// Number of elements which have flown through this queue
		int numElements;
		
		/// The timestamp of the last enqueued element
		Timestamp lastTs;
		
	public:		
		QueueMonitor () {
			numElements = 0;
			lastTs = 0;
		}
		
		virtual ~QueueMonitor () {}

		void logTs (Timestamp ts) {
			lastTs = ts;
		}
		
		void logElem () {
			numElements++;
		}

		virtual int getIntProperty (int property, int &val);
	};
}

#endif
