#ifndef _PLAN_MGR_IMPL_
#include "metadata/plan_mgr_impl.h"
#endif

#ifndef _ROW_WIN_
#include "execution/operators/row_win.h"
#endif

#ifndef _WIN_SYN_IMPL_
#include "execution/synopses/win_syn_impl.h"
#endif

using namespace Metadata;
using Execution::RowWindow;
using Execution::WindowSynopsisImpl;

int PlanManagerImpl::inst_row_win (Physical::Operator *op)
{
	int rc;
	RowWindow *window;
	WindowSynopsisImpl *syn;

	// Create the synopsis
	ASSERT (op -> u.ROW_WIN.winSyn);
	ASSERT (op -> u.ROW_WIN.winSyn -> kind == WIN_SYN);
	
	syn = new WindowSynopsisImpl (op -> u.ROW_WIN.winSyn -> id, LOG);
	op -> u.ROW_WIN.winSyn -> u.winSyn = syn;

	// Create the window object
	window = new RowWindow (op -> id, LOG);
	
	if ((rc = window -> setWindowSize (op -> u.ROW_WIN.numRows)) != 0)
		return rc;
	
	if ((rc = window -> setWindowSynopsis (syn)) != 0)
		return rc;
	
	op -> instOp = window;
	
	return 0;
}
	
	
