#ifndef _PARTN_WIN_SYN_
#define _PARTN_WIN_SYN_

/**
 * @file          partn_win_syn.h
 * @date          May 30, 2004
 * @brief         Synopsis interface for partition windows
 */

#ifndef _TUPLE_
#include "execution/internals/tuple.h"
#endif

#ifdef _MONITOR_
#ifndef _SYN_MONITOR_
#include "execution/monitors/syn_monitor.h"
#endif
#endif

namespace Execution {
#ifdef _MONITOR_
	class PartnWindowSynopsis : public Monitor::SynMonitor {
	public:
		virtual ~PartnWindowSynopsis() {}
		
		virtual int insertTuple (Tuple tuple) = 0;		
		virtual int deleteOldestTuple (Tuple partnSpec, Tuple& oldestTuple) = 0;		
		virtual int getPartnSize (Tuple partnSpec, unsigned int& partnSize) = 0;
	};
#else
	class PartnWindowSynopsis {
	public:
		virtual ~PartnWindowSynopsis() {}
		
		virtual int insertTuple (Tuple tuple) = 0;		
		virtual int deleteOldestTuple (Tuple partnSpec, Tuple& oldestTuple) = 0;		
		virtual int getPartnSize (Tuple partnSpec, unsigned int& partnSize) = 0;
	};
#endif	
}

#endif
