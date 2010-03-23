#ifndef _PLAN_MGR_IMPL_
#include "metadata/plan_mgr_impl.h"
#endif

#ifndef _RANGE_WIN_
#include "execution/operators/range_win.h"
#endif

#ifndef _WIN_SYN_IMPL_
#include "execution/synopses/win_syn_impl.h"
#endif

using namespace Metadata;
using Execution::RangeWindow;
using Execution::WindowSynopsisImpl;


int PlanManagerImpl::inst_range_win (Physical::Operator *op)
{
	int rc;
	RangeWindow *window;
	WindowSynopsisImpl *syn;
	
	window = new RangeWindow (op -> id, LOG);
	
	if ((rc = window -> setWindowSize (op -> u.RANGE_WIN.timeUnits)) != 0)		
		return rc;
	if ((rc = window -> setWindowStride (op -> u.RANGE_WIN.strideUnits)) != 0)		
		return rc;
	
	ASSERT (op -> u.RANGE_WIN.winSyn);
	ASSERT (op -> u.RANGE_WIN.winSyn -> kind == WIN_SYN);
	
	syn = new WindowSynopsisImpl (op -> u.RANGE_WIN.winSyn -> id, LOG);
	op -> u.RANGE_WIN.winSyn -> u.winSyn = syn;
	
	if ((rc = window -> setWindowSynopsis (syn)) != 0)
		return rc;
	
	op -> instOp = window;
	
	return 0;
}

int PlanManagerImpl::inst_now_win (Physical::Operator *op)
{
	int rc;
	RangeWindow *window;
	WindowSynopsisImpl *syn;
	
	window = new RangeWindow (op -> id, LOG);
	
	if ((rc = window -> setWindowSize (0)) != 0)		
		return rc;
	
	ASSERT (op -> u.RANGE_WIN.winSyn);
	ASSERT (op -> u.RANGE_WIN.winSyn -> kind == WIN_SYN);
	
	syn = new WindowSynopsisImpl (op -> u.RANGE_WIN.winSyn -> id, LOG);
	op -> u.RANGE_WIN.winSyn -> u.winSyn = syn;
	
	if ((rc = window -> setWindowSynopsis (syn)) != 0)
		return rc;
	
	op -> instOp = window;
	
	return 0;
}

