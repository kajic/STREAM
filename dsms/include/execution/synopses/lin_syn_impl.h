#ifndef _LIN_SYN_IMPL_
#define _LIN_SYN_IMPL_

/**
 * @file      lin_syn_impl.h
 * @date      Aug. 25, 2004
 * @brief     An implementation of the lineage synopsis interface
 */

#ifndef _LIN_SYN_
#include "execution/synopses/lin_syn.h"
#endif

#ifndef _LIN_STORE_
#include "execution/stores/lin_store.h"
#endif

#ifndef _CPP_OSTREAM_
#include <ostream>
#endif

namespace Execution {
	class LineageSynopsisImpl : public LineageSynopsis {
	private:
		
		/// System-wide unique identifier
		unsigned int id;
		
		/// System log
		std::ostream &LOG;
		
		/// The stubId used to identify the synopsis with the store
		unsigned int stubId;
		
		/// The store that stores the tuples of the synopsis
		LinStore *store;
		
	public:
		LineageSynopsisImpl (unsigned int id, std::ostream &LOG);
		
		~LineageSynopsisImpl ();
		
		/**
		 * Specify the store and the stubId
		 */
		
		int setStore (LinStore *store, unsigned int stubId);		
		
		/**
		 * Insert a tuple with a given lineage.  At any given point in
		 * time there should be exactly one tuple in the synopsis with a
		 * given lineage.
		 *
		 * @param tuple      the inserted tuple
		 * @param lineage    array of lineage tuples
		 * @return           [[ usual convention ]]
		 */
		
		int insertTuple (Tuple tuple, Tuple *lineage);
		
		/**
		 * Delete a tuple from the synopsis.
		 * 
		 * @param tuple     the tuple to be deleted.
		 * @return          [[ usual convention ]]
		 */ 
		
		int deleteTuple (Tuple tuple);
		
		/**
		 * Get a tuple with a given lineage if it exists.
		 *
		 * @param lineage   array of lineage tuples
		 * @param tuple     output tuple corresponding to the lineage
		 * @param bValid    bValid is true if there exists a tuple with
		 *                  the lineage and false otherwise.
		 * @return          [[ usual convention ]]
		 */
		
		int getTuple (Tuple *lineage, Tuple &tuple);
	};
}

#endif
