#ifndef _JOIN_MONITOR_
#include "execution/monitors/join_monitor.h"
#endif

#ifndef _PROPERTY_
#include "execution/monitors/property.h"
#endif

#ifndef _DEBUG_
#include "common/debug.h"
#endif

using namespace Monitor;

#include <iostream>
using namespace std;

int JoinMonitor::getIntProperty (int property, int &val)
{	
	if (property == JOIN_NINPUT) {
		val = numInput;
		return 0;
	}
	
	if (property == JOIN_NOUTPUT) {
		val = numJoined;
		return 0;
	}
	
	return PropertyMonitor::getIntProperty (property, val);
}
