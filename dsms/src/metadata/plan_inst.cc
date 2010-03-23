#ifndef _PLAN_MGR_IMPL_
#include "metadata/plan_mgr_impl.h"
#endif

#ifndef _DEBUG_
#include "common/debug.h"
#endif

#ifndef _BIN_JOIN_
#include "execution/operators/bin_join.h"
#endif

#ifndef _BIN_STR_JOIN_
#include "execution/operators/bin_str_join.h"
#endif

#ifndef _DISTINCT_
#include "execution/operators/distinct.h"
#endif

#ifndef _DSTREAM_
#include "execution/operators/dstream.h"
#endif

#ifndef _GROUP_AGGR_
#include "execution/operators/group_aggr.h"
#endif

#ifndef _ISTREAM_
#include "execution/operators/istream.h"
#endif

#ifndef _OUTPUT_
#include "execution/operators/output.h"
#endif

#ifndef _PARTN_WIN_
#include "execution/operators/partn_win.h"
#endif

#ifndef _PROJECT_
#include "execution/operators/project.h"
#endif

#ifndef _RANGE_WIN_
#include "execution/operators/range_win.h"
#endif

#ifndef _REL_SOURCE_
#include "execution/operators/rel_source.h"
#endif

#ifndef _ROW_WIN_
#include "execution/operators/row_win.h"
#endif

#ifndef _RSTREAM_
#include "execution/operators/rstream.h"
#endif

#ifndef _SELECT_
#include "execution/operators/select.h"
#endif

#ifndef _SINK_
#include "execution/operators/sink.h"
#endif

#ifndef _UNION_
#include "execution/operators/union.h"
#endif

#ifndef _EXCEPT_
#include "execution/operators/except.h"
#endif

#ifndef _STREAM_SOURCE_
#include "execution/operators/stream_source.h"
#endif

#ifndef _SYS_STREAM_GEN_
#include "execution/operators/sys_stream_gen.h"
#endif

#ifndef _SIMPLE_QUEUE_
#include "execution/queues/simple_queue.h"
#endif

#ifndef _SHARED_QUEUE_WRITER_
#include "execution/queues/shared_queue_writer.h"
#endif

#ifndef _SHARED_QUEUE_READER_
#include "execution/queues/shared_queue_reader.h"
#endif

#ifndef _REL_SYN_IMPL_
#include "execution/synopses/rel_syn_impl.h"
#endif

#ifndef _WIN_SYN_IMPL_
#include "execution/synopses/win_syn_impl.h"
#endif

#ifndef _LIN_SYN_IMPL_
#include "execution/synopses/lin_syn_impl.h"
#endif

#ifndef _PARTN_WIN_SYN_IMPL_
#include "execution/synopses/partn_win_syn_impl.h"
#endif

#ifndef _REL_STORE_IMPL_
#include "execution/stores/rel_store_impl.h"
#endif

#ifndef _WIN_STORE_IMPL_
#include "execution/stores/win_store_impl.h"
#endif

#ifndef _LIN_STORE_IMPL_
#include "execution/stores/lin_store_impl.h"
#endif

#ifndef _PWIN_STORE_IMPL_
#include "execution/stores/pwin_store_impl.h"
#endif

using namespace Metadata;
using Physical::Operator;
using Physical::Store;

using Execution::Select;
using Execution::Project;
using Execution::BinaryJoin;
using Execution::BinStreamJoin;
using Execution::GroupAggr;
using Execution::Distinct;
using Execution::RowWindow;
using Execution::RangeWindow;
using Execution::RangeWindow;
using Execution::PartnWindow;
using Execution::Istream;
using Execution::Dstream;
using Execution::Rstream;
using Execution::StreamSource;
using Execution::RelSource;
using Execution::Queue;
using Execution::Output;
using Execution::Sink;
using Execution::Union;
using Execution::Except;
using Execution::SimpleQueue;
using Execution::SharedQueueWriter;
using Execution::SharedQueueReader;
using Execution::PartnWindowSynopsisImpl;
using Execution::LineageSynopsisImpl;
using Execution::WindowSynopsisImpl;
using Execution::RelationSynopsisImpl;
using Execution::RelStoreImpl;
using Execution::WinStoreImpl;
using Execution::PwinStoreImpl;
using Execution::LinStoreImpl;
using Execution::StorageAlloc;


static int set_output_queue (Operator *op, Execution::Queue *queue);
static int set_input_queue (Operator *op, Execution::Queue *queue,							
							unsigned int inputPos);
static int set_out_queue_select (Operator *op, Execution::Queue *queue);
static int set_out_queue_project (Operator *op, Execution::Queue *queue);
static int set_out_queue_join (Operator *op, Execution::Queue *queue);
static int set_out_queue_str_join (Operator *op, Execution::Queue *queue);
static int set_out_queue_aggr (Operator *op, Execution::Queue *queue);
static int set_out_queue_distinct (Operator *op, Execution::Queue *queue);
static int set_out_queue_row_win (Operator *op, Execution::Queue *queue);
static int set_out_queue_range_win (Operator *op, Execution::Queue *queue);
static int set_out_queue_pwin (Operator *op, Execution::Queue *queue);
static int set_out_queue_istream (Operator *op, Execution::Queue *queue);
static int set_out_queue_dstream (Operator *op, Execution::Queue *queue);
static int set_out_queue_rstream (Operator *op, Execution::Queue *queue);
static int set_out_queue_str_source (Operator *op, Execution::Queue *queue);
static int set_out_queue_rel_source (Operator *op, Execution::Queue *queue);
static int set_out_queue_union (Operator *op, Execution::Queue *queue);
static int set_out_queue_except (Operator *op, Execution::Queue *queue);
static int set_input_queue_select (Operator *op, Execution::Queue *queue, unsigned int pos);
static int set_input_queue_project (Operator *op, Execution::Queue *queue, unsigned int pos);
static int set_input_queue_join (Operator *op, Execution::Queue *queue, unsigned int pos);
static int set_input_queue_str_join (Operator *op, Execution::Queue *queue, unsigned int pos);
static int set_input_queue_aggr (Operator *op, Execution::Queue *queue, unsigned int pos);
static int set_input_queue_distinct (Operator *op, Execution::Queue *queue, unsigned int pos);
static int set_input_queue_row_win (Operator *op, Execution::Queue *queue, unsigned int pos);
static int set_input_queue_range_win (Operator *op, Execution::Queue *queue, unsigned int pos);
static int set_input_queue_pwin (Operator *op, Execution::Queue *queue, unsigned int pos);
static int set_input_queue_istream (Operator *op, Execution::Queue *queue, unsigned int pos);
static int set_input_queue_dstream (Operator *op, Execution::Queue *queue, unsigned int pos);
static int set_input_queue_rstream (Operator *op, Execution::Queue *queue, unsigned int pos);
static int set_input_queue_output (Operator *op, Execution::Queue *queue, unsigned int pos);
static int set_input_queue_sink (Operator *op, Execution::Queue *queue, unsigned int pos);
static int set_input_queue_union (Operator *op, Execution::Queue *queue, unsigned int pos);
static int set_input_queue_except (Operator *op, Execution::Queue *queue, unsigned int pos);
static int link_relstore_syn (Physical::Store *store);
static int link_winstore_syn (Physical::Store *store);
static int link_linstore_syn (Physical::Store *store);
static int link_pwinstore_syn (Physical::Store *store);
static int set_in_store_select (Operator *op);
static int set_in_store_project (Operator *op);
static int set_in_store_join (Operator *op);
static int set_in_store_str_join (Operator *op);
static int set_in_store_aggr (Operator *op);
static int set_in_store_distinct (Operator *op);
static int set_in_store_row_win (Operator *op);
static int set_in_store_range_win (Operator *op);
static int set_in_store_pwin (Operator *op);
static int set_in_store_istream (Operator *op);
static int set_in_store_dstream (Operator *op);
static int set_in_store_rstream (Operator *op);
static int set_in_store_output (Operator *op);
static int set_in_store_sink (Operator *op);
static int set_in_store_union (Operator *op);
static int set_in_store_except (Operator *op);

// Size of queue in terms of pages
extern unsigned int QUEUE_SIZE;

// Size of shared queue
extern unsigned int SHARED_QUEUE_SIZE;

/// Memory that memory manager gets from the system [6 MB]
extern unsigned int MEMORY;


int PlanManagerImpl::inst_mem_mgr ()
{
	int rc;
	
	memMgr = new Execution::MemoryManager (0, LOG);
	if ((rc = memMgr -> setMemorySize (MEMORY)) != 0)
		return rc;	
	if ((rc = memMgr -> initialize ()) != 0)
		return rc;
	
	return 0;
}

int PlanManagerImpl::inst_static_tuple_alloc ()
{	
	ASSERT (memMgr);
	staticTupleAlloc = new StaticTupleAlloc (memMgr);
	return 0;
}

int PlanManagerImpl::getStaticTuple (char *&tuple, unsigned int tupleSize)
{	
	if (!staticTupleAlloc)
		return -1;
	
	return staticTupleAlloc -> getStaticTuple (tupleSize, tuple);
}


int PlanManagerImpl::inst_queues ()
{
	int rc;
	
	for (unsigned int q = 0 ; q < numQueues ; q++) {

		// already instantiated
		if (queues[q].instQueue)
			continue;
		
		switch (queues[q].kind) {
		case SIMPLE_Q:
			if ((rc = inst_simple_queue (queues + q)) != 0)
				return rc;
			break;
			
		case READER_Q:
			if ((rc = inst_reader_queue (queues + q)) != 0)
				return rc;
			break;
			
		case WRITER_Q:
			if ((rc = inst_writer_queue (queues + q)) != 0)
				return rc;
			break;
			
		default:
			ASSERT (0);
			break;
		}
	}

	// Link the readers to their writers
	for (unsigned int q = 0 ; q < numQueues ; q++) {
		if (queues[q].kind == READER_Q) {			
			if ((rc = link_reader_writer (queues + q)) != 0)
				return rc;
		}
	}

	
	return 0;	
}

int PlanManagerImpl::link_reader_writer (Physical::Queue *reader)
{
	int rc;
	
	Physical::Queue   *writer;
	SharedQueueReader *sharedQueueReader;
	SharedQueueWriter *sharedQueueWriter;

	writer = reader -> u.READER.writer;
	
	sharedQueueReader = (SharedQueueReader*)reader -> instQueue;
	sharedQueueWriter = (SharedQueueWriter*)writer -> instQueue;

	ASSERT (sharedQueueReader);
	ASSERT (sharedQueueWriter);

	if ((rc = sharedQueueReader -> setWriter (sharedQueueWriter)) != 0)
		return rc;
	
	return 0;	
}

int PlanManagerImpl::inst_simple_queue (Physical::Queue *queue)
{
	int rc;
	Operator *source, *dest;
	unsigned int index;
	SimpleQueue *simpleQueue;
	
	// Sanity check
	ASSERT (queue);
	ASSERT (queue -> kind == SIMPLE_Q);
	ASSERT (queue -> u.SIMPLE.source);
	ASSERT (queue -> u.SIMPLE.dest);
	
	source = queue -> u.SIMPLE.source;
	dest = queue -> u.SIMPLE.dest;
	index = queue -> u.SIMPLE.index;

	// Instantiate the queue
	simpleQueue = new SimpleQueue (queue -> id, LOG);
	if ((rc = simpleQueue -> setNumPages (QUEUE_SIZE)) != 0)
		return rc;
	if ((rc = simpleQueue -> setMemoryManager (memMgr)) != 0)
		return rc;
	if ((rc = simpleQueue -> initialize ()) != 0)
		return rc;	
	queue -> instQueue = simpleQueue;
	
	// Update the operators
	if ((rc = set_output_queue (source, simpleQueue)) != 0)
		return rc;
	if ((rc = set_input_queue (dest, simpleQueue, index)) != 0)
		return rc;
	
	return 0;	
}

int PlanManagerImpl::inst_reader_queue (Physical::Queue *queue) 
{
	int rc;
	SharedQueueReader *sharedQueueReader;
	unsigned int index;
	Operator *dest;
	
	ASSERT (queue);
	ASSERT (queue -> kind == READER_Q);
	ASSERT (queue -> u.READER.dest);
	ASSERT (queue -> u.READER.writer);

	dest = queue -> u.READER.dest;
	index = queue -> u.READER.index;
	
	sharedQueueReader = new SharedQueueReader (queue -> id, LOG);
	rc = sharedQueueReader -> setReaderId (queue -> u.READER.readerId);
	if (rc != 0) return rc;
	queue -> instQueue = sharedQueueReader;

	// Update the operators
	rc = set_input_queue (dest, sharedQueueReader, index);
	if (rc != 0)
		return rc;	
	
	return 0;
}

int PlanManagerImpl::inst_writer_queue (Physical::Queue *queue)
{
	int rc;
	Operator *source;
	SharedQueueWriter *sharedQueueWriter;
	
	ASSERT (queue);
	ASSERT (queue -> u.WRITER.source);
	ASSERT (queue -> u.WRITER.numReaders > 1);
	
	// set in set_in_store() method
	ASSERT (queue -> u.WRITER.store);
	
	sharedQueueWriter = new SharedQueueWriter (queue -> id, LOG);
	
	if ((rc = sharedQueueWriter -> setNumPages (SHARED_QUEUE_SIZE)) != 0)
		return rc;
	
	if ((rc = sharedQueueWriter -> setMemoryManager (memMgr)) != 0)				
		return rc;
	
	rc = sharedQueueWriter -> setNumReaders (queue -> u.WRITER.numReaders);
	if (rc != 0) return rc;
	
	rc = sharedQueueWriter -> setStore (queue->u.WRITER.store->instStore);
	if (rc != 0) return rc;
	
	if ((rc = sharedQueueWriter -> initialize ()) != 0)
		return rc;	
	queue -> instQueue = sharedQueueWriter;
	
	source = queue -> u.WRITER.source;
	if ((rc = set_output_queue (source, sharedQueueWriter)) != 0)
		return rc;	
	
	return 0;
}

int PlanManagerImpl::inst_op (Operator *op) {
	int rc;
	
	switch (op -> kind) {
	case PO_SELECT:
		if ((rc = inst_select (op)) != 0)
			return rc;
		break;
			
	case PO_PROJECT:
		if ((rc = inst_project (op)) != 0)
			return rc;
		break;
			
	case PO_JOIN_PROJECT: 
	case PO_JOIN:
		if ((rc = inst_join (op)) != 0)
			return rc;
		break;
			
	case PO_STR_JOIN_PROJECT: 
	case PO_STR_JOIN:
		if ((rc = inst_str_join (op)) != 0)
			return rc;
		break;
			
	case PO_GROUP_AGGR:
		if ((rc = inst_aggr (op)) != 0)
			return rc;
		break;

	case PO_DISTINCT:
		if ((rc = inst_distinct (op)) != 0)
			return rc;
		break;

	case PO_ROW_WIN:
		if ((rc = inst_row_win (op)) != 0)
			return rc;
		break;

	case PO_RANGE_WIN:
		if ((rc = inst_range_win (op)) != 0)
			return rc;
		break;

	case PO_PARTN_WIN:
		if ((rc = inst_pwin (op)) != 0)
			return rc;
		break;

	case PO_ISTREAM:
		if ((rc = inst_istream (op)) != 0)
			return rc;
		break;

	case PO_DSTREAM:
		if ((rc = inst_dstream (op)) != 0)
			return rc;
		break;

	case PO_RSTREAM:
		if ((rc = inst_rstream (op)) != 0)
			return rc;
		break;

	case PO_STREAM_SOURCE:
		if ((rc = inst_str_source (op)) != 0)
			return rc;
		break;

	case PO_RELN_SOURCE:
		if ((rc = inst_rel_source (op)) != 0)
			return rc;
		break;
			
	case PO_OUTPUT:
		if ((rc = inst_output (op)) != 0)
			return rc;
		break;

	case PO_SINK:
		if ((rc = inst_sink (op)) != 0)
			return rc;
		break;
			
	case PO_UNION:
		if ((rc = inst_union (op)) != 0)
			return rc;
		break;

	case PO_EXCEPT:
		if ((rc = inst_except (op)) != 0)
			return rc;
		break;			
		
	case PO_SS_GEN:
		break;
		
#ifdef _DM_
	default:
		ASSERT (0);
		break;
#endif
	}

	return 0;
}
	

int PlanManagerImpl::inst_ops ()
{
	int rc;
	Operator *op;

	op = usedOps;

	while (op) {
		if ((rc = inst_op (op)) != 0)
			return rc;
		
		op = op -> next;
	}
	
	return 0;
}

int PlanManagerImpl::inst_ops_mon (Physical::Operator *&plan,
								   Physical::Operator **opList,
								   unsigned int &numOps)
{
	int rc;
	
	for (unsigned int o = 0 ; o < numOps ; o++) {
		if (opList [o] -> kind == PO_SS_GEN) {
			if ((rc = inst_ss_gen_store (opList [o])) != 0) {
				return rc;
			}
		}
		else {
			if ((rc = inst_op (opList [o])) != 0) {
				return rc;
			}
		}
	}

	return 0;
}

int PlanManagerImpl::link_syn_store ()
{
	int rc;
	
	for (unsigned int s = 0 ; s < numStores ; s++ ){
		
		switch (stores [s].kind) {
		case SIMPLE_STORE:
			ASSERT (stores [s].numStubs == 0);
			break;
			
		case REL_STORE:
			if ((rc = link_relstore_syn (stores + s)) != 0)
				return rc;
			break;
			
		case WIN_STORE:
			if ((rc = link_winstore_syn (stores + s)) != 0)
				return rc;
			break;
			
		case LIN_STORE:
			if ((rc = link_linstore_syn (stores + s)) != 0)
				return rc;
			break;
			
		case PARTN_WIN_STORE:
			if ((rc = link_pwinstore_syn (stores + s)) != 0)
				return rc;
			break;
			
		default:
			ASSERT (0);
			break;
		}		
	}
	
	return 0;
}

int PlanManagerImpl::link_syn_store_mon (Physical::Store **storeList,
										 unsigned int numMonStores)
{
	int rc;
	
	for (unsigned int s = 0 ; s < numMonStores ; s++ ){
		
		switch (storeList [s] -> kind) {
		case SIMPLE_STORE:
			ASSERT (storeList [s] -> numStubs == 0);
			break;
			
		case REL_STORE:
			if ((rc = link_relstore_syn (storeList [s])) != 0)
				return rc;
			break;
			
		case WIN_STORE:
			if ((rc = link_winstore_syn (storeList [s])) != 0)
				return rc;
			break;
			
		case LIN_STORE:
			if ((rc = link_linstore_syn (storeList [s])) != 0)
				return rc;
			break;
			
		case PARTN_WIN_STORE:
			if ((rc = link_pwinstore_syn (storeList [s])) != 0)
				return rc;
			break;
			
		default:
			ASSERT (0);
			break;
		}		
	}
	
	return 0;
}


static int set_output_queue (Operator *op, Execution::Queue *queue)
{	
	switch (op -> kind) {
	case PO_SELECT:
		return set_out_queue_select (op, queue);
		
	case PO_PROJECT:
		return set_out_queue_project (op, queue);
		
	case PO_JOIN_PROJECT: 
	case PO_JOIN:
		return set_out_queue_join (op, queue);
		
	case PO_STR_JOIN_PROJECT: 
	case PO_STR_JOIN:
		return set_out_queue_str_join (op, queue);
			
	case PO_GROUP_AGGR:
		return set_out_queue_aggr (op, queue);

	case PO_DISTINCT:
		return set_out_queue_distinct (op, queue);

	case PO_ROW_WIN:
		return set_out_queue_row_win (op, queue);

	case PO_RANGE_WIN:
		return set_out_queue_range_win (op, queue);

	case PO_PARTN_WIN:
		return set_out_queue_pwin (op, queue);

	case PO_ISTREAM:
		return set_out_queue_istream (op, queue);
		
	case PO_DSTREAM:
		return set_out_queue_dstream (op, queue);
		
	case PO_RSTREAM:
		return set_out_queue_rstream (op, queue);
		
	case PO_STREAM_SOURCE:
		return set_out_queue_str_source (op, queue);
		
	case PO_RELN_SOURCE:
		return set_out_queue_rel_source (op, queue);

	case PO_UNION:
		return set_out_queue_union (op, queue);

	case PO_EXCEPT:
		return set_out_queue_except (op, queue);

	case PO_SS_GEN:
		break;
		
	default:
		ASSERT (0);
		break;
	}
	
	return 0;
}

static int set_input_queue (Operator *op,
							Execution::Queue *queue,
							unsigned int inputPos)
{	
	switch (op -> kind) {
	case PO_SELECT:
		return set_input_queue_select (op, queue, inputPos);
		
	case PO_PROJECT:
		return set_input_queue_project (op, queue, inputPos);
		
	case PO_JOIN_PROJECT: 
	case PO_JOIN:
		return set_input_queue_join (op, queue, inputPos);
		
	case PO_STR_JOIN_PROJECT: 
	case PO_STR_JOIN:
		return set_input_queue_str_join (op, queue, inputPos);
			
	case PO_GROUP_AGGR:
		return set_input_queue_aggr (op, queue, inputPos);

	case PO_DISTINCT:
		return set_input_queue_distinct (op, queue, inputPos);

	case PO_ROW_WIN:
		return set_input_queue_row_win (op, queue, inputPos);

	case PO_RANGE_WIN:
		return set_input_queue_range_win (op, queue, inputPos);

	case PO_PARTN_WIN:
		return set_input_queue_pwin (op, queue, inputPos);

	case PO_ISTREAM:
		return set_input_queue_istream (op, queue, inputPos);
		
	case PO_DSTREAM:
		return set_input_queue_dstream (op, queue, inputPos);
		
	case PO_RSTREAM:
		return set_input_queue_rstream (op, queue, inputPos);
		
	case PO_OUTPUT:
		return set_input_queue_output(op, queue, inputPos);

	case PO_SINK:
		return set_input_queue_sink (op, queue, inputPos);
		
	case PO_EXCEPT:
		return set_input_queue_except (op, queue, inputPos);

	case PO_UNION:
		return set_input_queue_union (op, queue, inputPos);
		
	case PO_STREAM_SOURCE:
	case PO_RELN_SOURCE:
	case PO_SS_GEN:
		break;
	default:
		ASSERT (0);
		break;
	}
	
	return 0;
}

int PlanManagerImpl::link_in_stores ()
{
	int rc;
	Operator *op = usedOps;
	
	while (op) {
		
		switch (op -> kind) {
		case PO_SELECT:
			if ((rc = set_in_store_select (op)) != 0)
				return rc;
			break;
			
		case PO_PROJECT:
			if ((rc = set_in_store_project (op)) != 0)
				return rc;
			break;
		
		case PO_JOIN_PROJECT:			
		case PO_JOIN:
			if ((rc = set_in_store_join (op)) != 0)
				return rc;
			break;
		
		case PO_STR_JOIN_PROJECT:			
		case PO_STR_JOIN:
			if ((rc = set_in_store_str_join (op)) != 0)
				return rc;
			break;
			
		case PO_GROUP_AGGR:
			if ((rc = set_in_store_aggr (op)) != 0)
				return rc;
			break;

		case PO_DISTINCT:
			if ((rc = set_in_store_distinct (op)) != 0)
				return rc;
			break;

		case PO_ROW_WIN:
			if ((rc = set_in_store_row_win (op)) != 0)
				return rc;
			break;

		case PO_RANGE_WIN:
			if ((rc = set_in_store_range_win (op)) != 0)
				return rc;
			break;
			
		case PO_PARTN_WIN:
			if ((rc = set_in_store_pwin (op)) != 0)
				return rc;
			break;

		case PO_ISTREAM:
			if ((rc = set_in_store_istream (op)) != 0)
				return rc;
			break;
		
		case PO_DSTREAM:
			if ((rc = set_in_store_dstream (op)) != 0)
				return rc;
			break;
		
		case PO_RSTREAM:
			if ((rc = set_in_store_rstream (op)) != 0)
				return rc;
			break;
		
		case PO_OUTPUT:
			if ((rc = set_in_store_output(op)) != 0)
				return rc;
			break;

		case PO_SINK:
			if ((rc = set_in_store_sink (op)) != 0)
				return rc;
			break;
			
		case PO_STREAM_SOURCE:		
		case PO_RELN_SOURCE:
		case PO_SS_GEN:
			break;

		case PO_UNION:
			if ((rc = set_in_store_union (op)) != 0)
				return rc;
			break;
			
		case PO_EXCEPT:
			if ((rc = set_in_store_except (op)) != 0)
				return rc;
			break;
			
		default:
			ASSERT (0);
			break;			
		}
		
		op = op -> next;
	}
	
	return 0;
}

int PlanManagerImpl::link_in_stores_mon (Physical::Operator **opList,
										 unsigned int numOps)
{
	int rc;
	Operator *op;
	
	for (unsigned int o = 0 ; o < numOps ; o++) {
		op = opList [o];
		
		switch (op -> kind) {
		case PO_SELECT:
			if ((rc = set_in_store_select (op)) != 0)
				return rc;
			break;
			
		case PO_PROJECT:
			if ((rc = set_in_store_project (op)) != 0)
				return rc;
			break;
		
		case PO_JOIN_PROJECT:			
		case PO_JOIN:
			if ((rc = set_in_store_join (op)) != 0)
				return rc;
			break;
		
		case PO_STR_JOIN_PROJECT:			
		case PO_STR_JOIN:
			if ((rc = set_in_store_str_join (op)) != 0)
				return rc;
			break;
			
		case PO_GROUP_AGGR:
			if ((rc = set_in_store_aggr (op)) != 0)
				return rc;
			break;

		case PO_DISTINCT:
			if ((rc = set_in_store_distinct (op)) != 0)
				return rc;
			break;

		case PO_ROW_WIN:
			if ((rc = set_in_store_row_win (op)) != 0)
				return rc;
			break;

		case PO_RANGE_WIN:
			if ((rc = set_in_store_range_win (op)) != 0)
				return rc;
			break;
			
		case PO_PARTN_WIN:
			if ((rc = set_in_store_pwin (op)) != 0)
				return rc;
			break;

		case PO_ISTREAM:
			if ((rc = set_in_store_istream (op)) != 0)
				return rc;
			break;
		
		case PO_DSTREAM:
			if ((rc = set_in_store_dstream (op)) != 0)
				return rc;
			break;
		
		case PO_RSTREAM:
			if ((rc = set_in_store_rstream (op)) != 0)
				return rc;
			break;
		
		case PO_OUTPUT:
			if ((rc = set_in_store_output(op)) != 0)
				return rc;
			break;

		case PO_SINK:
			if ((rc = set_in_store_sink (op)) != 0)
				return rc;
			break;
			
		case PO_STREAM_SOURCE:		
		case PO_RELN_SOURCE:
		case PO_SS_GEN:
			break;

		case PO_UNION:
			if ((rc = set_in_store_union (op)) != 0)
				return rc;
			break;
			
		case PO_EXCEPT:
			if ((rc = set_in_store_except (op)) != 0)
				return rc;
			break;
			
		default:
			ASSERT (0);
			break;			
		}		
	}
	
	return 0;
}


static int set_out_queue_select (Operator *op, Execution::Queue *queue)
{	
	return ((Select *)(op -> instOp)) -> setOutputQueue (queue);
}

static int set_out_queue_project (Operator *op, Execution::Queue *queue)
{	
	return ((Project *)(op -> instOp)) -> setOutputQueue (queue);
}

static int set_out_queue_join (Operator *op, Execution::Queue *queue)
{	
	return ((BinaryJoin *)(op -> instOp)) -> setOutputQueue (queue);
}

static int set_out_queue_str_join (Operator *op, Execution::Queue *queue)
{	
	return ((BinStreamJoin *)(op -> instOp)) -> setOutputQueue (queue);
}

static int set_out_queue_aggr (Operator *op, Execution::Queue *queue)
{	
	return ((GroupAggr *)(op -> instOp)) -> setOutputQueue (queue);
}

static int set_out_queue_distinct (Operator *op, Execution::Queue *queue)
{	
	return ((Distinct *)(op -> instOp)) -> setOutputQueue (queue);
}

static int set_out_queue_row_win (Operator *op, Execution::Queue *queue)
{	
	return ((RowWindow *)(op -> instOp)) -> setOutputQueue (queue);
}

static int set_out_queue_range_win (Operator *op, Execution::Queue *queue)
{	
	return ((RangeWindow *)(op -> instOp)) -> setOutputQueue (queue);
}

static int set_out_queue_pwin (Operator *op, Execution::Queue *queue)
{	
	return ((PartnWindow *)(op -> instOp)) -> setOutputQueue (queue);
}

static int set_out_queue_istream (Operator *op, Execution::Queue *queue)
{	
	return ((Istream *)(op -> instOp)) -> setOutputQueue (queue);
}

static int set_out_queue_dstream (Operator *op, Execution::Queue *queue)
{	
	return ((Dstream *)(op -> instOp)) -> setOutputQueue (queue);
}

static int set_out_queue_rstream (Operator *op, Execution::Queue *queue)
{	
	return ((Rstream *)(op -> instOp)) -> setOutputQueue (queue);
}

static int set_out_queue_str_source (Operator *op, Execution::Queue *queue)
{	
	return ((StreamSource *)(op -> instOp)) -> setOutputQueue (queue);
}

static int set_out_queue_rel_source (Operator *op, Execution::Queue *queue)
{	
	return ((RelSource *)(op -> instOp)) -> setOutputQueue (queue);
}

static int set_out_queue_union (Operator *op, Execution::Queue *queue)
{
	return ((Union *)(op -> instOp)) -> setOutputQueue (queue);
}

static int set_out_queue_except (Operator *op, Execution::Queue *queue)
{
	return ((Except *)(op -> instOp)) -> setOutputQueue (queue);
}

static int set_input_queue_select (Operator *op, Execution::Queue *queue, unsigned int pos)
{
	ASSERT (pos == 0);
	return ((Select *)(op -> instOp)) -> setInputQueue (queue);
}

static int set_input_queue_project (Operator *op, Execution::Queue *queue, unsigned int pos)
{
	ASSERT (pos == 0);
	return ((Project *)(op -> instOp)) -> setInputQueue (queue);
}

static int set_input_queue_join (Operator *op, Execution::Queue *queue, unsigned int pos)
{
	ASSERT (pos <= 1);
	if (pos == 0)
		return ((BinaryJoin *)(op -> instOp)) -> setOuterInputQueue (queue);
	else
		return ((BinaryJoin *)(op -> instOp)) -> setInnerInputQueue (queue);
}

static int set_input_queue_str_join (Operator *op, Execution::Queue *queue, unsigned int pos)
{
	ASSERT (pos <= 1);
	if (pos == 0)
		return ((BinStreamJoin *)(op -> instOp)) -> setOuterInputQueue (queue);
	else
		return ((BinStreamJoin *)(op -> instOp)) -> setInnerInputQueue (queue);
}

static int set_input_queue_aggr (Operator *op, Execution::Queue *queue, unsigned int pos)
{
	ASSERT (pos == 0);
	return ((GroupAggr *)(op -> instOp)) -> setInputQueue (queue);
}

static int set_input_queue_distinct (Operator *op, Execution::Queue *queue, unsigned int pos)
{
	ASSERT (pos == 0);
	return ((Distinct *)(op -> instOp)) -> setInputQueue (queue);
}

static int set_input_queue_row_win (Operator *op, Execution::Queue *queue, unsigned int pos)
{
	ASSERT (pos == 0);
	return ((RowWindow *)(op -> instOp)) -> setInputQueue (queue);
}

static int set_input_queue_range_win (Operator *op, Execution::Queue *queue, unsigned int pos)
{
	ASSERT (pos == 0);
	return ((RangeWindow *)(op -> instOp)) -> setInputQueue (queue);
}

static int set_input_queue_pwin (Operator *op, Execution::Queue *queue, unsigned int pos)
{
	ASSERT (pos == 0);
	return ((PartnWindow *)(op -> instOp)) -> setInputQueue (queue);
}

static int set_input_queue_istream (Operator *op, Execution::Queue *queue, unsigned int pos)
{
	ASSERT (pos == 0);
	return ((Istream *)(op -> instOp)) -> setInputQueue (queue);
}

static int set_input_queue_dstream (Operator *op, Execution::Queue *queue, unsigned int pos)
{
	ASSERT (pos == 0);
	return ((Dstream *)(op -> instOp)) -> setInputQueue (queue);
}

static int set_input_queue_rstream (Operator *op, Execution::Queue *queue, unsigned int pos)
{
	ASSERT (pos == 0);
	return ((Rstream *)(op -> instOp)) -> setInputQueue (queue);
}

static int set_input_queue_output (Operator *op, Execution::Queue *queue, unsigned int pos)
{
	ASSERT (pos == 0);
	return ((Output *)(op -> instOp)) -> setInputQueue (queue);
}

static int set_input_queue_sink (Operator *op, Execution::Queue *queue, unsigned int pos)
{
	ASSERT (pos == 0);
	return ((Sink *)(op -> instOp)) -> setInputQueue (queue);
}

static int set_input_queue_union (Operator *op, Execution::Queue *queue, unsigned int pos)
{
	ASSERT (pos <= 1);
	if (pos == 0) {
		return ((Union *)(op -> instOp)) -> setLeftInputQueue (queue);
	}
	else {
		return ((Union *)(op -> instOp)) -> setRightInputQueue (queue);
	}

	return -1;
}

static int set_input_queue_except (Operator *op, Execution::Queue *queue, unsigned int pos)
{
	ASSERT (pos <= 1);
	if (pos == 0) {
		return ((Except *)(op -> instOp)) -> setLeftInputQueue (queue);
	}
	else {
		return ((Except *)(op -> instOp)) -> setRightInputQueue (queue);
	}
	
	return -1;
}

static int link_relstore_syn (Physical::Store *store)
{
	int rc;
	
	RelStoreImpl *relStore;
	RelationSynopsisImpl *relSyn;
	
	ASSERT (store -> kind == REL_STORE);
	ASSERT (store -> instStore);

	relStore = (RelStoreImpl *)(store -> instStore);
	
	for (unsigned int s = 0 ; s < store -> numStubs ; s++) {
		
		ASSERT (store -> stubs [s]);
		ASSERT (store -> stubs [s] -> kind == REL_SYN);
		ASSERT (store -> stubs [s] -> u.relSyn);
		
		relSyn = (RelationSynopsisImpl *)store -> stubs[s] -> u.relSyn;
		
		if ((rc = relSyn -> setStore (relStore, s)) != 0)
			return rc;
	}
	
	return 0;
}
	
static int link_winstore_syn (Physical::Store *store)
{
	int rc;
	
	WinStoreImpl *winStore;
	RelationSynopsisImpl *relSyn;
	WindowSynopsisImpl *winSyn;
	
	ASSERT (store -> kind == WIN_STORE);
	ASSERT (store -> instStore);

	winStore = (WinStoreImpl *)(store -> instStore);
	for (unsigned int s = 0 ; s < store -> numStubs ; s++) {
		
		ASSERT (store -> stubs [s]);
		ASSERT (store -> stubs [s] -> kind == REL_SYN ||
				store -> stubs [s] -> kind == WIN_SYN);
		
		if (store -> stubs [s] -> kind == REL_SYN) {
			
			relSyn = (RelationSynopsisImpl *)
				store -> stubs[s] -> u.relSyn;			
			if ((rc = relSyn -> setStore (winStore, s)) != 0)
				return rc;			
		}
		
		else {
			winSyn = (WindowSynopsisImpl *)
				store -> stubs [s] -> u.winSyn;
			if ((rc = winSyn -> setStore (winStore, s)) != 0)
				return rc;			
		}		
	}

	return 0;
}
	
static int link_linstore_syn (Physical::Store *store)
{
	int rc;
	
	LinStoreImpl *linStore;
	RelationSynopsisImpl *relSyn;
	LineageSynopsisImpl *linSyn;
	
	ASSERT (store -> kind == LIN_STORE);
	ASSERT (store -> instStore);
	
	linStore = (LinStoreImpl *)(store -> instStore);
	for (unsigned int s = 0 ; s < store -> numStubs ; s++) {
		
		ASSERT (store -> stubs [s]);
		ASSERT (store -> stubs [s] -> kind == REL_SYN ||
				store -> stubs [s] -> kind == LIN_SYN);
		
		if (store -> stubs [s] -> kind == REL_SYN) {
			
			relSyn = (RelationSynopsisImpl *)
				store -> stubs[s] -> u.relSyn;			
			if ((rc = relSyn -> setStore (linStore, s)) != 0)
				return rc;			
		}
		
		else {
			linSyn = (LineageSynopsisImpl *)
				store -> stubs [s] -> u.linSyn;
			if ((rc = linSyn -> setStore (linStore, s)) != 0)
				return rc;			
		}		
	}
	
	return 0;
}

static int link_pwinstore_syn (Physical::Store *store)
{
	int rc;
	
	PwinStoreImpl *pwinStore;
	RelationSynopsisImpl *relSyn;
	PartnWindowSynopsisImpl *pwinSyn;

	ASSERT (store -> kind == PARTN_WIN_STORE);
	ASSERT (store -> instStore);

	pwinStore = (PwinStoreImpl *)(store -> instStore);
	for (unsigned int s = 0 ; s < store -> numStubs ; s++) {
		
		ASSERT (store -> stubs [s]);
		ASSERT (store -> stubs [s] -> kind == REL_SYN ||
				store -> stubs [s] -> kind == PARTN_WIN_SYN);
		
		if (store -> stubs [s] -> kind == REL_SYN) {
			
			relSyn = (RelationSynopsisImpl *)
				store -> stubs[s] -> u.relSyn;
			if ((rc = relSyn -> setStore (pwinStore, s)) != 0)
				return rc;

		}

		else {

			pwinSyn = (PartnWindowSynopsisImpl *)
				store -> stubs [s] -> u.pwinSyn;

			if ((rc = pwinSyn -> setStore (pwinStore, s)) != 0)
				return rc;

		}

	}
	
	return 0;
}

static int set_in_store_select (Operator *op)
{
	return ((Select *)(op -> instOp)) -> setInStore
		(op -> inStores [0] -> instStore);
}

static int set_in_store_project (Operator *op)
{
	return ((Project *)(op -> instOp)) -> setInStore
		(op -> inStores [0] -> instStore);
}

static int set_in_store_aggr (Operator *op)
{
	return ((GroupAggr *)(op -> instOp)) -> setInStore
		(op -> inStores [0] -> instStore);
}

static int set_in_store_distinct (Operator *op)
{
	return ((Distinct *)(op -> instOp)) -> setInStore
		(op -> inStores [0] -> instStore);
}

static int set_in_store_row_win (Operator *op)
{
	return ((RowWindow *)(op -> instOp)) -> setInStore
		(op -> inStores [0] -> instStore);
}

static int set_in_store_range_win (Operator *op)
{
	return ((RangeWindow *)(op -> instOp)) -> setInStore
		(op -> inStores [0] -> instStore);
}
	
static int set_in_store_pwin (Operator *op)
{
	return ((PartnWindow *)(op -> instOp)) -> setInStore
		(op -> inStores [0] -> instStore);
}

static int set_in_store_istream (Operator *op)
{
	return ((Istream *)(op -> instOp)) -> setInStore
		(op -> inStores [0] -> instStore);
}

static int set_in_store_dstream (Operator *op)
{
	return ((Dstream *)(op -> instOp)) -> setInStore
		(op -> inStores [0] -> instStore);
}
	
static int set_in_store_rstream (Operator *op)
{
	return ((Rstream *)(op -> instOp)) -> setInStore
		(op -> inStores [0] -> instStore);
}

static int set_in_store_output (Operator *op)
{
	return ((Output *)(op -> instOp)) -> setInStore
		(op -> inStores [0] -> instStore);
}

static int set_in_store_sink (Operator *op)
{
	return ((Sink *)(op -> instOp)) -> setInStore
		(op -> inStores [0] -> instStore);
}

static int set_in_store_join (Operator *op)
{
	int rc;
    rc = ((BinaryJoin *)(op -> instOp)) -> setOuterInputStore
		(op -> inStores [0] -> instStore);
	if (rc != 0) return rc;
	
	return ((BinaryJoin *)(op -> instOp)) -> setInnerInputStore
		(op -> inStores [1] -> instStore);
}

static int set_in_store_str_join (Operator *op)
{
	int rc;
    rc = ((BinStreamJoin *)(op -> instOp)) -> setOuterInputStore
		(op -> inStores [0] -> instStore);
	if (rc != 0) return rc;

	return ((BinStreamJoin *)(op -> instOp)) -> setInnerInputStore
		(op -> inStores [1] -> instStore);
}

static int set_in_store_union (Operator *op)
{
	int rc;

	rc = ((Union *)(op -> instOp)) -> setLeftInputStore
		(op -> inStores [0] -> instStore);
	if (rc != 0) return rc;

	rc = ((Union *)(op -> instOp)) -> setRightInputStore
		(op -> inStores [1] -> instStore);
	if (rc != 0) return rc;

	return 0;
}

static int set_in_store_except (Operator *op)
{
	int rc;
	
	rc = ((Except *)(op -> instOp)) -> setLeftInputStore
		(op -> inStores [0] -> instStore);
	if (rc != 0) return rc;

	rc = ((Except *)(op -> instOp)) -> setRightInputStore
		(op -> inStores [1] -> instStore);
	if (rc != 0) return rc;
	
	return 0;
}
