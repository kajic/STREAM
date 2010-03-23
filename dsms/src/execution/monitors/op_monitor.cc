#ifndef _OP_MONITOR_
#include "execution/monitors/op_monitor.h"
#endif

#ifndef _PROPERTY_
#include "execution/monitors/property.h"
#endif

#ifndef _DEBUG_
#include "common/debug.h"
#endif

using namespace Monitor;

OperatorMonitor::OperatorMonitor ()
{
	timer.reset ();
	outTs = 0;
	numJoined = 0;
	numInput = 0;
}
OperatorMonitor::~OperatorMonitor () {}

void OperatorMonitor::startTimer ()
{
	timer.start ();
}

void OperatorMonitor::stopTimer ()
{
	timer.stop ();
}

void OperatorMonitor::logOutTs (Timestamp ts)
{
	ASSERT (outTs <= ts);
	
	outTs = ts;
}

int OperatorMonitor::getDoubleProperty (int property, double &val)
{
	if (property == OP_TIME_USED) {
		val = timer.getSecs ();
		return 0;
	}
	
	return PropertyMonitor::getDoubleProperty (property, val);		
}

int OperatorMonitor::getIntProperty (int property, int &val)
{
	if (property == OP_LAST_OUT_TS) {
		val = (int)outTs;
		return 0;
	}
	
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
