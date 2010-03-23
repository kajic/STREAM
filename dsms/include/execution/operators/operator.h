#ifndef _OPERATOR_
#define _OPERATOR_

#ifndef _OP_MONITOR_
#include "execution/monitors/op_monitor.h"
#endif

/**
 * @file          operator.h
 * @date          May 30, 2004
 * @brief         Operator interface, as seen by the Scheduler.
 */

typedef unsigned int TimeSlice;

namespace Execution {

#ifdef _MONITOR_
	
	/**
	 * An operator - the basic unit of execution & scheduling.
	 */
	
	class Operator : public Monitor::OperatorMonitor {
	public:
		virtual ~Operator() {}
		
		/**
		 * Run the operator for a specified amount of "time" indicated by
		 * timeSlice parameter.  Called by the scheduler
		 */ 
		virtual int run (TimeSlice timeSlice) = 0;
	};
	
#else
	
	/**
	 * An operator - the basic unit of execution & scheduling.
	 */
	
	class Operator {
	public:
		virtual ~Operator() {}
		
		/**
		 * Run the operator for a specified amount of "time" indicated by
		 * timeSlice parameter.  Called by the scheduler
		 */ 
		virtual int run (TimeSlice timeSlice) = 0;
	};

#endif
}
#endif
