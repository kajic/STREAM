#ifndef _SYN_MONITOR_
#define _SYN_MONITOR_

#ifndef _PROPERTY_MONITOR_
#include "execution/monitors/property_monitor.h"
#endif

namespace Monitor {

	/**
	 * A generic monitor for synopses
	 */
	class SynMonitor : public PropertyMonitor {
	private:
		/// Size of synopsis
		int numTuples;
		
		/// Max size of synopsis
		int maxTuples;
		
	public:
		SynMonitor ();
		virtual ~SynMonitor ();
		
		void logIns ();
		void logDel ();
		virtual int getIntProperty (int property, int &val);
	};
}

#endif
