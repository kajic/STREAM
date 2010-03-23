#ifndef _PLAN_MGR_
#include "metadata/plan_mgr.h"
#endif

#ifndef _PLAN_MGR_IMPL_
#include "metadata/plan_mgr_impl.h"
#endif

using namespace Metadata;

PlanManager *PlanManager::newPlanManager(TableManager *tableMgr,
										 std::ostream &LOG)
{
	return new PlanManagerImpl(tableMgr, LOG);
}

