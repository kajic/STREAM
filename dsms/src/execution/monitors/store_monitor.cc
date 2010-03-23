#ifndef _STORE_MONITOR_
#include "execution/monitors/store_monitor.h"
#endif

#ifndef _PROPERTY_
#include "execution/monitors/property.h"
#endif

#ifndef _DEBUG_
#include "common/debug.h"
#endif

using namespace Monitor;

StoreMonitor::StoreMonitor ()
{
	numPages = 0;
	maxPages = 0;
}

StoreMonitor::~StoreMonitor () {}

void StoreMonitor::logPageAlloc ()
{
	ASSERT (numPages >= 0);
	ASSERT (maxPages >= 0);
	if (++numPages > maxPages)
		maxPages = numPages;
}

void StoreMonitor::logPageFree ()
{
	numPages--;
	ASSERT (numPages >= 0);
}

int StoreMonitor::getIntProperty (int property, int& val)
{
	if (property == STORE_MAX_PAGES) {
		val = maxPages;
		return 0;
	}
	
	if (property == STORE_NUM_PAGES) {
		val = numPages;
		return 0;
	}
	
	return PropertyMonitor::getIntProperty (property, val);
}
	
		
	
