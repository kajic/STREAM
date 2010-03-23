#ifndef _PHY_SYN_
#define _PHY_SYN_

#ifndef _REL_SYN_
#include "execution/synopses/rel_syn.h"
#endif

#ifndef _WIN_SYN_
#include "execution/synopses/win_syn.h"
#endif

#ifndef _LIN_SYN_
#include "execution/synopses/lin_syn.h"
#endif

#ifndef _PARTN_WIN_SYN_
#include "execution/synopses/partn_win_syn.h"
#endif

/**
 * @file     phy_syn.h
 * @date     Aug. 24, 2004
 * @brief    Representation of synopses in the system
 */

namespace Physical {

	/// All synpses store a bag of tuples, but they offer different
	/// interfaces to access and update the bag of tuples, depending on
	/// their kind/type
	
	enum SynopsisKind {
		// Relation synopsis
		REL_SYN,
		
		// Window Synopsis
		WIN_SYN,
		
		// Partition window synopsis
		PARTN_WIN_SYN,
		
		// "Lineage" synopsis [[ Explanation ]]
		LIN_SYN
	};
	
	// forward decl.
	struct Operator;
	
	struct Store;
	
	struct Synopsis {
		
		/// indexes the array PlanManagerImpl.syns
		unsigned int id;
		
		/// Type of synopsis
		SynopsisKind kind;
		
		/// Store for the synopsis that allocates space for tuples in the
		/// synopsis 
		Store *store;
		
		/// Operator that owns the synopsis
		Operator *ownOp;
		
		/// Instantiated synopsis object
		union {
			Execution::RelationSynopsis    *relSyn;
			Execution::WindowSynopsis      *winSyn;
			Execution::LineageSynopsis     *linSyn;
			Execution::PartnWindowSynopsis *pwinSyn;
		} u;
	};
}

#endif
