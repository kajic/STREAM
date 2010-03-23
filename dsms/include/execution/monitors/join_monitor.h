#ifndef _JOIN_MONITOR_
#define _JOIN_MONITOR_

/**
 * @file      join_monitor.h
 * @date      Jan 04, 2005
 * @brief     Monitor for a join operator to keep track of join selectivity
 */


#ifndef _PROPERTY_MONITOR_
#include "execution/monitors/property_monitor.h"
#endif

namespace Monitor {

	class JoinMonitor : public PropertyMonitor {
	private:
		/// The number of joined output tuples
		long numJoined;

		/// The number of input tuples seen (inner and outer)
		long numInput;
		
	public:
		JoinMonitor () {
			numJoined = 0;
			numInput = 0;
		}
		
		virtual ~JoinMonitor () { }
		
		void logJoin () {
			numJoined ++;
		}
		
		void logInput () {
			numInput ++;
		}

		virtual int getIntProperty (int property, int&val);
	};
}

#endif
