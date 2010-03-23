#ifndef _PARTN_WIN_SYN_IMPL_
#define _PARTN_WIN_SYN_IMPL_

/**
 * @file      partn_win_syn_impl.h
 * @date      Sept. 9, 2004
 * @brief     Implementation of partition window synopsis
 */

#include <ostream>

#ifndef _PARTN_WIN_SYN_
#include "execution/synopses/partn_win_syn.h"
#endif

#ifndef _PWIN_STORE_
#include "execution/stores/pwin_store.h"
#endif

namespace Execution {
	class PartnWindowSynopsisImpl : public PartnWindowSynopsis {
	private:
		/// System-wide identifier
		unsigned int id;

		/// System log
		std::ostream &LOG;
		
		/// Stub Id to identify me with the store
		unsigned int stubId;
		
		/// Store which allocates & stores my tuples
		PwinStore *store;
		
	public:
		
		PartnWindowSynopsisImpl (unsigned int id, std::ostream &LOG);
		virtual ~PartnWindowSynopsisImpl ();
		
		//----------------------------------------------------------------------
		// Initialization routines
		//----------------------------------------------------------------------
		int setStore (PwinStore *store, unsigned int stubId);
		
		//----------------------------------------------------------------------
		// Methods inherited from PartnWindowSynopsis
		//----------------------------------------------------------------------
		int insertTuple (Tuple tuple);
		int deleteOldestTuple (Tuple partnSpec, Tuple &oldestTuple);
		int getPartnSize (Tuple partnSpec, unsigned int &partnSize);
	};
}

#endif
