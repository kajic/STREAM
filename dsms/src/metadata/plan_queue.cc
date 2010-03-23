#ifndef _PLAN_MGR_IMPL_
#include "metadata/plan_mgr_impl.h"
#endif

#ifndef _PHY_QUEUE_
#include "metadata/phy_queue.h"
#endif

#ifndef _DEBUG_
#include "common/debug.h"
#endif

using namespace Physical;
using namespace Metadata;

Queue *PlanManagerImpl::new_queue (QueueKind kind) {
	Queue *queue;
	
	if (numQueues < MAX_QUEUES) {
		
		queue = (queues + numQueues);
		queue -> id = numQueues;
		queue -> kind = kind;
		queue -> instQueue = 0;
		
		numQueues++;
		return queue;
	}
	
	return 0;
}

static unsigned int getInputIndex (Operator *child, Operator *parent)
{
	ASSERT (child);
	ASSERT (parent);

	for (unsigned int c = 0 ; c < parent -> numInputs ; c++)
		if (parent -> inputs [c] == child)
			return c;
	
	ASSERT (0);
	return 0;
}

int PlanManagerImpl::add_queues ()
{
	Queue *queue, *reader, *writer;	
	Operator *op;
	
	op = usedOps;
	
	while (op) {
		
		// Single output, so a simple queue suffices
		if (op -> numOutputs == 1) {
			queue = new_queue (SIMPLE_Q);
			if (!queue) {
				LOG << "Plan manager out of resources" << std::endl;
				return -1;
			}
			
			queue -> u.SIMPLE.source = op;
			queue -> u.SIMPLE.dest = op -> outputs [0];
			queue -> u.SIMPLE.index = getInputIndex (op, op -> outputs [0]);
			
			op -> outQueue = queue;
			op -> outputs [0] ->
				inQueues [queue -> u.SIMPLE.index] = queue;
		}
		
		else if (op -> numOutputs > 1) {
			
			writer = new_queue (WRITER_Q);
			if (!writer) {
				LOG << "Plan manager out of resources" << std::endl;
				return -1;
			}
			
			writer -> u.WRITER.source = op;
			writer -> u.WRITER.numReaders = op -> numOutputs;
			
			op -> outQueue = writer;			
			
			// Create the readers
			ASSERT (op -> numOutputs < MAX_OUT_BRANCHING);
			for (unsigned int o = 0 ; o < op -> numOutputs ; o++) {
				
				reader = new_queue (READER_Q);
				if (!reader) {
					LOG << "Plan manager out of resources" << std::endl;
					return -1;
				}
				
				reader -> u.READER.writer = writer;
				reader -> u.READER.dest = op -> outputs [o];
				reader -> u.READER.index = getInputIndex (op, op ->
														  outputs [o]);
				reader -> u.READER.readerId = o;
				
				writer -> u.WRITER.readers [o] = reader;
				
				op -> outputs [o] ->
					inQueues [reader ->u.READER.index] = reader;
			}
		}
		
		else {
			ASSERT (op -> kind == PO_OUTPUT || op -> kind == PO_SINK ||
				op -> kind == PO_SS_GEN);
		}
		
		op = op -> next;
	}
	
	return 0;
}

int PlanManagerImpl::add_queues_mon (Operator *&plan,
									 Operator **opList,
									 unsigned int &numOps)
{
	Queue *queue, *reader, *writer;	
	Operator *op;	
	
	for (unsigned int o = 0 ; o < numOps ; o++) {
		op = opList [o];
		
		// Single output, so a simple queue suffices
		if (op -> numOutputs == 1) {
			queue = new_queue (SIMPLE_Q);
			if (!queue) {
				LOG << "Plan manager out of resources" << std::endl;
				return -1;
			}
			
			queue -> u.SIMPLE.source = op;
			queue -> u.SIMPLE.dest = op -> outputs [0];
			queue -> u.SIMPLE.index = getInputIndex (op, op -> outputs [0]);
			
			op -> outQueue = queue;
			op -> outputs [0] ->
				inQueues [queue -> u.SIMPLE.index] = queue;
		}
		
		else if (op -> numOutputs > 1) {
			
			writer = new_queue (WRITER_Q);
			if (!writer) {
				LOG << "Plan manager out of resources" << std::endl;
				return -1;
			}
			
			writer -> u.WRITER.source = op;
			writer -> u.WRITER.numReaders = op -> numOutputs;
			
			op -> outQueue = writer;			
			
			// Create the readers
			ASSERT (op -> numOutputs < MAX_OUT_BRANCHING);
			for (unsigned int o = 0 ; o < op -> numOutputs ; o++) {
				
				reader = new_queue (READER_Q);
				if (!reader) {
					LOG << "Plan manager out of resources" << std::endl;
					return -1;
				}
				
				reader -> u.READER.writer = writer;
				reader -> u.READER.dest = op -> outputs [o];
				reader -> u.READER.index = getInputIndex (op, op ->
														  outputs [o]);
				reader -> u.READER.readerId = o;
				
				writer -> u.WRITER.readers [o] = reader;
				
				op -> outputs [o] ->
					inQueues [reader ->u.READER.index] = reader;
			}
		}
		
		else {
			ASSERT (op -> kind == PO_OUTPUT || op -> kind == PO_SINK);
		}		
	}
	
	return 0;
}
