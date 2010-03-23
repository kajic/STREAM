#ifndef _PROPERTY_MONITOR_
#define _PROPERTY_MONITOR_

namespace Monitor {
	
	/**
	 * Abstract superclass of all kinds of dynamic property monitors in
	 * the system.
	 */ 	
	class PropertyMonitor {
	public:
		virtual int getIntProperty (int property, int& val);
		virtual int getDoubleProperty (int property, double& val);
	};
}

#endif
