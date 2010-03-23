#ifndef _PLAN_MGR_IMPL_
#include "metadata/plan_mgr_impl.h"
#endif

#ifndef _REL_STORE_IMPL_
#include "execution/stores/rel_store_impl.h"
#endif

using namespace Metadata;
using Execution::RelStoreImpl;

static const float THRESHOLD = 0.50;

int PlanManagerImpl::inst_rel_store (Physical::Store *store,
									 TupleLayout *tupleLayout)
{
	int rc;	
	unsigned int tupleLen;
	unsigned int usageCol, nextCol, prevCol, refCountCol;
	RelStoreImpl *relStore;
	
	ASSERT (store);
	ASSERT (store -> kind == REL_STORE);
	
	// Determine the data layout of a tuple including the metadata portion 
	
	if ((rc = tupleLayout -> addFixedLenAttr (INT, usageCol)) != 0)
		return rc;
	
	if ((rc = tupleLayout -> addCharPtrAttr (nextCol)) != 0)
		return rc;
	
	if ((rc = tupleLayout -> addCharPtrAttr (prevCol)) != 0)
		return rc;
	
	if ((rc = tupleLayout -> addFixedLenAttr (INT, refCountCol)) != 0)
		return rc;
	
	// Update tuplelen to include the next col
	tupleLen = tupleLayout -> getTupleLen ();
	
	relStore = new RelStoreImpl(store -> id, LOG);
	
	if ((rc = relStore -> setMemoryManager (memMgr)) != 0)
		return rc;
	
	if ((rc = relStore -> setTupleLen (tupleLen)) != 0)
		return rc;
	
	if ((rc = relStore -> setNextCol (nextCol)) != 0)
		return rc;
	
	if ((rc = relStore -> setPrevCol (prevCol)) != 0)
		return rc;
	
	if ((rc = relStore -> setUsageCol (usageCol)) != 0)
		return rc;

	if ((rc = relStore -> setRefCountCol (refCountCol)) != 0)
		return rc;
	
	if ((rc = relStore -> setNumStubs (store -> numStubs)) != 0)
		return rc;
	
	if ((rc = relStore -> initialize ()) != 0)
		return rc;
	
	store -> instStore = relStore;
	
	return 0;
}
