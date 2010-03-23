#ifndef _PLAN_MGR_IMPL_
#include "metadata/plan_mgr_impl.h"
#endif

#ifndef _SIMPLE_STORE_
#include "execution/stores/simple_store.h"
#endif

using namespace Metadata;
using Execution::SimpleStore;

int PlanManagerImpl::inst_simple_store (Physical::Store *store,
										TupleLayout *dataLayout)
{
	int rc;
	unsigned int tupleLen;
	SimpleStore *simpleStore;
	
	ASSERT (store);
	ASSERT (!store -> instStore);
	
	// Create the store object
	simpleStore = new SimpleStore (store -> id, LOG);
	
	// Set the tuple length
	tupleLen = dataLayout -> getTupleLen ();	
	
	if ((rc = simpleStore -> setTupleLen(tupleLen)) != 0)
		return rc;
	if ((rc = simpleStore -> setMemoryManager (memMgr)) != 0)
		return rc;
	if ((rc = simpleStore -> initialize ()) != 0)
		return rc;
	
	store -> instStore = simpleStore;
	
	return 0;
}

