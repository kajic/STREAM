#ifndef _OP_MONITOR_
#define _OP_MONITOR_

#ifndef _PROPERTY_MONITOR_
#include "execution/monitors/property_monitor.h"
#endif

#ifndef _TIMER_
#include "execution/monitors/timer.h"
#endif

#ifndef _TYPES_
#include "common/types.h"
#endif

namespace Monitor {
	
	/**
	 * A generic monitor for operators.  Currently keeps track of the time
	 * used by an operator.
	 */ 
	
	class OperatorMonitor : public PropertyMonitor {
	private:
		Timer timer;
		Timestamp outTs;

		/// The number of joined output tuples
		long numJoined;
		
		/// The number of input tuples seen (inner and outer)
		long numInput;		
		
	public:
		OperatorMonitor ();
		virtual ~OperatorMonitor();
		
		void startTimer ();
		void stopTimer ();
		void logOutTs (Timestamp ts);

		void logJoin () {
			numJoined ++;
		}
		
		void logInput () {
			numInput ++;
		}
		
		virtual int getDoubleProperty (int property, double &val);
		virtual int getIntProperty (int property, int &val);			
	};
}

#endif
