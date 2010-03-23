#ifndef _PLAN_MGR_IMPL_
#include "metadata/plan_mgr_impl.h"
#endif

#ifndef _STREAM_SOURCE_
#include "execution/operators/stream_source.h"
#endif

using namespace Metadata;

using Execution::StreamSource;
using Interface::TableSource;
using Execution::StorageAlloc;

int PlanManagerImpl::inst_str_source (Physical::Operator *op)
{
	int rc;

	TupleLayout *tupleLayout;
	TableSource *source;
	StreamSource *sourceOp;
	StorageAlloc *outStore;
	
	unsigned int dataCol;
	
	// Table source
	source = baseTableSources [op -> u.STREAM_SOURCE.srcId];
	
	sourceOp = new StreamSource (op -> id, LOG);
	
	if ((rc = sourceOp -> setTableSource (source)) != 0)
		return rc;
	
	tupleLayout = new TupleLayout (op);
	
	ASSERT (op -> store);
	ASSERT (op -> store -> kind == SIMPLE_STORE ||
			op -> store -> kind == WIN_STORE);
	
	if (op -> store -> kind == SIMPLE_STORE) {
		if ((rc = inst_simple_store (op -> store, tupleLayout)) != 0)
			return rc;
	}
	else {
		if ((rc = inst_win_store (op -> store, tupleLayout)) != 0)
			return rc;
	}
	outStore = op -> store -> instStore;
	
	for (unsigned int a = 0 ; a < op -> numAttrs ; a++) {

		dataCol = tupleLayout -> getColumn (a);

		if ((rc = sourceOp -> addAttr (op -> attrTypes [a],
									   op -> attrLen [a],
									   dataCol)) != 0)
			return rc;
	}

	if ((rc = sourceOp -> setStoreAlloc (outStore)) != 0)
		return rc;

	if ((rc = sourceOp -> initialize ()) != 0)
		return rc;
	
	op -> instOp = sourceOp;

	delete tupleLayout;
	
	return 0;
}
	
