#ifndef _STORE_ALLOC_
#define _STORE_ALLOC_

#ifndef _TUPLE_
#include "execution/internals/tuple.h"
#endif

/**
 * @file       store_alloc.h
 * @date       May 30, 2004
 * @brief      Storage allocator interface. All operators that create new
 *             tuples see this interface.  Example: project, join,
 *             aggregate
 *             
 *             Different implementations of this interface exist to
 *             support different sharing requirements. [[ Explain ]]
 */

#ifdef _MONITOR_


#ifndef _STORE_MONITOR_
#include "execution/monitors/store_monitor.h"
#endif

namespace Execution {
	/**
	 * Abstract class defining the storage allocator interface.  The
	 * interface supports just one method that allocates new tuples.
	 */   
	class StorageAlloc : public Monitor::StoreMonitor  {
	public:
		virtual ~StorageAlloc () {}
		
		virtual int newTuple (Tuple& tuple) = 0;
		virtual int addRef (Tuple tuple) = 0;
		virtual int addRef (Tuple tuple, unsigned int ref) = 0;
		virtual int decrRef (Tuple tuple) = 0;
	};
}


#else

namespace Execution {
	/**
	 * Abstract class defining the storage allocator interface.  The
	 * interface supports just one method that allocates new tuples.
	 */   
	class StorageAlloc {
	public:
		virtual ~StorageAlloc () {}
		
		virtual int newTuple (Tuple& tuple) = 0;
		virtual int addRef (Tuple tuple) = 0;
		virtual int addRef (Tuple tuple, unsigned int ref) = 0;
		virtual int decrRef (Tuple tuple) = 0;
	};
}


#endif

#endif
