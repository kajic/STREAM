#ifndef _WIN_SYN_
#define _WIN_SYN_

/**
 * @file            win_syn.h
 * @date            May 30, 2004
 * @brief           A window synopsis interface.
 */

#ifndef _TUPLE_
#include "execution/internals/tuple.h"
#endif

#ifndef _TYPES_
#include "common/types.h"
#endif

#ifdef _MONITOR_
#ifndef _SYN_MONITOR_
#include "execution/monitors/syn_monitor.h"
#endif
#endif

namespace Execution {
#ifdef _MONITOR_
	class WindowSynopsis : public Monitor::SynMonitor {
	public:
		virtual ~WindowSynopsis() {}
		
		virtual int insertTuple (Tuple tuple, 
								 Timestamp timestamp) = 0;
		
		virtual bool isEmpty() const = 0;
		
		virtual int getOldestTuple (Tuple& tuple, 
									Timestamp& timestamp) = 0;
		
		virtual int deleteOldestTuple () = 0;
	};
#else
	class WindowSynopsis : public Monitor::SynMonitor {
	public:
		virtual ~WindowSynopsis() {}
		
		virtual int insertTuple (Tuple tuple, 
								 Timestamp timestamp) = 0;
		
		virtual bool isEmpty() const = 0;
		
		virtual int getOldestTuple (Tuple& tuple, 
									Timestamp& timestamp) = 0;
		
		virtual int deleteOldestTuple () = 0;
	};
#endif   
}

#endif
