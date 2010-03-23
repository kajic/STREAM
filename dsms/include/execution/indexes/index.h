#ifndef _INDEX_
#define _INDEX_

#ifndef _TUPLE_
#include "execution/internals/tuple.h"
#endif

#ifndef _TUPLE_ITER_
#include "execution/internals/tuple_iter.h"
#endif

#ifndef _PROPERTY_MONITOR_
#include "execution/monitors/property_monitor.h"
#endif

/**
 * The index interface.
 */ 
namespace Execution {

#ifdef _MONITOR_
	class Index : public Monitor::PropertyMonitor {
	public:
		Index () {}
		virtual ~Index() {}
		virtual int insertTuple (Tuple tuple) = 0;
		virtual int deleteTuple (Tuple tuple) = 0;
		virtual int getScan (TupleIterator *& iter) = 0;
		virtual int releaseScan (TupleIterator *iter) = 0;
	};
#else
	class Index {
	public:
		Index () {}
		virtual ~Index() {}
		virtual int insertTuple (Tuple tuple) = 0;
		virtual int deleteTuple (Tuple tuple) = 0;
		virtual int getScan (TupleIterator *& iter) = 0;
		virtual int releaseScan (TupleIterator *iter) = 0;
	};
#endif	
	
}

#endif
