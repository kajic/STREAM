#ifndef _STORE_MONITOR_
#define _STORE_MONITOR_

#ifndef _PROPERTY_MONITOR_
#include "execution/monitors/property_monitor.h"
#endif

namespace Monitor {

	/**
	 * A generic monitor for storage allocators.  Currently keeps track of
	 * number of pages allocated & the maximum number of pages allocated
	 * at any point
	 */
	class StoreMonitor : public PropertyMonitor {
	private:
		/// Number of pages currently allocated
		int numPages;
		
		/// Maximum number of pages allocated
		int maxPages;
		
	public:
		StoreMonitor ();
		virtual ~StoreMonitor ();
		
		void logPageAlloc ();
		void logPageFree ();
		virtual int getIntProperty (int property, int& val);
	};
}

#endif
