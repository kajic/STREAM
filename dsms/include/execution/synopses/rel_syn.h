#ifndef _REL_SYN_
#define _REL_SYN_

/**
 * @file          rel_syn.h
 * @date          May 30, 2004
 * @brief         Relational synopsis interface.  
 */


#ifndef _TUPLE_
#include "execution/internals/tuple.h"
#endif

#ifndef _TUPLE_ITER_
#include "execution/internals/tuple_iter.h"
#endif

#ifdef _MONITOR_
#ifndef _SYN_MONITOR_
#include "execution/monitors/syn_monitor.h"
#endif
#endif

namespace Execution {
	
		
#ifdef _MONITOR_
	class RelationSynopsis	: public Monitor::SynMonitor {
	public:
		virtual ~RelationSynopsis() {}
		
		/**
		 * Insert a new tuple into the synopsis.
		 *
		 * @param    tuple     the inserted tuple
		 * @return             [[ usual convention ]]
		 */
		virtual int insertTuple (Tuple tuple) = 0;
		
		/**
		 * Delete a tuple from the synopsis.
		 *
		 * @param    tuple     the tuple to be deleted.
		 * @return             [[ usual convention ]]
		 */		
		virtual int deleteTuple (Tuple tuple) = 0;
		
		/**
		 * Scan the  current bag  of tuples subject  to some  condition. A
		 * synopsis only performs  certain pre-registered scans, and these
		 * are identified by a  unique identifier.  In other words, scanId
		 * implicitly identifies the scan conditions.
		 * 
		 * @param   scanId     Specification of which scan we want.
		 * @param   iter       (output) iterator for the scan.
		 * @return             [[ usual convention ]]
		 */
		virtual int getScan (unsigned int    scanId, 
							 TupleIterator  *&iter) = 0;

		/**
		 * Release a scan.
		 */
		virtual int releaseScan (unsigned int scanId,
								 TupleIterator *iter) = 0;
		
	};
#else
	
	class RelationSynopsis	{				
	public:
		virtual ~RelationSynopsis() {}
		
		/**
		 * Insert a new tuple into the synopsis.
		 *
		 * @param    tuple     the inserted tuple
		 * @return             [[ usual convention ]]
		 */
		virtual int insertTuple (Tuple tuple) = 0;
		
		/**
		 * Delete a tuple from the synopsis.
		 *
		 * @param    tuple     the tuple to be deleted.
		 * @return             [[ usual convention ]]
		 */		
		virtual int deleteTuple (Tuple tuple) = 0;
		
		/**
		 * Scan the  current bag  of tuples subject  to some  condition. A
		 * synopsis only performs  certain pre-registered scans, and these
		 * are identified by a  unique identifier.  In other words, scanId
		 * implicitly identifies the scan conditions.
		 * 
		 * @param   scanId     Specification of which scan we want.
		 * @param   iter       (output) iterator for the scan.
		 * @return             [[ usual convention ]]
		 */
		virtual int getScan (unsigned int    scanId, 
							 TupleIterator  *&iter) = 0;

		/**
		 * Release a scan.
		 */
		virtual int releaseScan (unsigned int scanId,
								 TupleIterator *iter) = 0;
		
	};	
#endif
}
#endif
