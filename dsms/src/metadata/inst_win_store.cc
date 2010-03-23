#ifndef _PLAN_MGR_IMPL_
#include "metadata/plan_mgr_impl.h"
#endif

#ifndef _WIN_STORE_IMPL_
#include "execution/stores/win_store_impl.h"
#endif

using namespace Metadata;
using Execution::WinStoreImpl;


int PlanManagerImpl::inst_win_store (Physical::Store *store,
									 TupleLayout *tupLayout)
{
	int rc;
	
	unsigned int tstampCol;
	unsigned int tupleLen;
	
	WinStoreImpl *winStore;
	
	ASSERT (store);
	ASSERT (store -> kind == WIN_STORE);

	// Determine the tuple layout
	if ((rc = tupLayout -> addTimestampAttr (tstampCol)) != 0)
		return rc;	
	tupleLen = tupLayout -> getTupleLen ();
	
	// Store object
	winStore = new WinStoreImpl (store -> id, LOG);

	if ((rc = winStore -> setMemoryManager (memMgr)) != 0)
		return rc;
	
	if ((rc = winStore -> setTupleLen (tupleLen)) != 0)
		return rc;

	if ((rc = winStore -> setNumStubs (store -> numStubs)) != 0)
		return rc;
	
	if ((rc = winStore -> setTimestampCol (tstampCol)) != 0)
		return rc;

	if ((rc = winStore -> initialize ()) != 0)
		return rc;
	
	store -> instStore = winStore;

	return 0;
}
	
