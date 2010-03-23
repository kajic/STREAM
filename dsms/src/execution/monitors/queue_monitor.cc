#ifndef _QUEUE_MONITOR_
#include "execution/monitors/queue_monitor.h"
#endif

#ifndef _PROPERTY_
#include "execution/monitors/property.h"
#endif

using namespace Monitor;

int QueueMonitor::getIntProperty (int property, int&val) {
	
	if (property == QUEUE_NUM_ELEM) {
		val = numElements;
		return 0;
	}
	
	if (property == QUEUE_LAST_TS) {
		val = (int)lastTs;
		return 0;
	}
	
	return PropertyMonitor::getIntProperty (property, val);
}
