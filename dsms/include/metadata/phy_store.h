#ifndef _PHY_STORE_
#define _PHY_STORE_

/**
 * @file      phy_store.h
 * @date      Aug. 25, 2004
 * @brief     Representation of stores in the system
 */

#ifndef _STORE_ALLOC_
#include "execution/stores/store_alloc.h"
#endif

namespace Physical {

	enum StoreKind {
		SIMPLE_STORE,
		
		REL_STORE,
		
		WIN_STORE,
		
		LIN_STORE,
		
		PARTN_WIN_STORE
	};
	
	// forward decl.
	struct Synopsis;
	
	struct Operator;
	
	struct Index;
	
	static const unsigned int MAX_STUBS = 10;
	
	struct Store {
		
		/// indexes the array PlanManagerImpl.stores
		unsigned int id;
		
		/// Type of store
		StoreKind kind;
		
		/// Operator who owns the store
		Operator *ownOp;
		
		/// Synopses which store tuples whose memory is allocated by this
		/// store
		Synopsis *stubs [MAX_STUBS];
		
		/// number of stubs
		unsigned int numStubs;
		
		/// Instantiated store
		Execution::StorageAlloc *instStore;
		
		union {
			struct {
				Index *idx;
				unsigned int numLineage;
			} LIN_STORE;
			
			struct {
				Index *hdrIdx;
			} PWIN_STORE;
		} u;
	};
}

#endif

