#ifndef _LIN_SYN_
#define _LIN_SYN_

#ifndef _TUPLE_
#include "execution/internals/tuple.h"
#endif

/**
 * @file         lin_syn.h
 * @date         Aug. 25, 2004
 * @brief        Lineage synopsis interface
 */

#ifdef _MONITOR_
#ifndef _SYN_MONITOR_
#include "execution/monitors/syn_monitor.h"
#endif
#endif

namespace Execution {
	/**
	 * The basic functionality exported by lineage synopsis is as follows.
	 * When a  tuple is  inserted, we specify  a *lineage* for  the tuple.
	 * The lineage for  a tuple is the collection  of tuples that produced
	 * the inserted  tuple.  The synopsis  offers methods to  lookup using
	 * lineage.
	 *
	 * Lineage  synopsis  is  useful  for  joins  and  project  operators.
	 * [[ Explanation ]]
	 */

#ifdef _MONITOR_
	class LineageSynopsis : public Monitor::SynMonitor {
	public:
		
		virtual ~LineageSynopsis() {}
		
		/**
		 * Insert a tuple with a given lineage.  At any given point in
		 * time there should be exactly one tuple in the synopsis with a
		 * given lineage.
		 *
		 * @param tuple      the inserted tuple
		 * @param lineage    array of lineage tuples
		 * @return           [[ usual convention ]]
		 */
		
		virtual int insertTuple (Tuple tuple, Tuple *lineage) = 0;
		
		/**
		 * Delete a tuple from the synopsis.
		 * 
		 * @param tuple     the tuple to be deleted.
		 * @return          [[ usual convention ]]
		 */ 
		
		virtual int deleteTuple (Tuple tuple) = 0;

		/**
		 * Get a tuple with a given lineage if it exists.
		 *
		 * @param lineage   array of lineage tuples
		 * @param tuple     output tuple corresponding to the lineage
		 * @return          [[ usual convention ]]
		 */
		virtual int getTuple (Tuple *lineage, Tuple &tuple)
			= 0;
	};
#else
	class LineageSynopsis : public Monitor::SynMonitor {
	public:
		
		virtual ~LineageSynopsis() {}

		/**
		 * Insert a tuple with a given lineage.  At any given point in
		 * time there should be exactly one tuple in the synopsis with a
		 * given lineage.
		 *
		 * @param tuple      the inserted tuple
		 * @param lineage    array of lineage tuples
		 * @return           [[ usual convention ]]
		 */
		
		virtual int insertTuple (Tuple tuple, Tuple *lineage) = 0;
		
		/**
		 * Delete a tuple from the synopsis.
		 * 
		 * @param tuple     the tuple to be deleted.
		 * @return          [[ usual convention ]]
		 */ 
		
		virtual int deleteTuple (Tuple tuple) = 0;

		/**
		 * Get a tuple with a given lineage if it exists.
		 *
		 * @param lineage   array of lineage tuples
		 * @param tuple     output tuple corresponding to the lineage
		 * @return          [[ usual convention ]]
		 */
		virtual int getTuple (Tuple *lineage, Tuple &tuple)
			= 0;
	};
#endif	
}

#endif
