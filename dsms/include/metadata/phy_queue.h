#ifndef _PHY_QUEUE_
#define _PHY_QUEUE_

#ifndef _QUEUE_
#include "execution/queues/queue.h"
#endif

/**
 * @file     phy_op.h
 * @date     Nov 29, 2004
 * @brief    Representation of physical queues in the system.
 */ 

namespace Physical {

	// forward decl
	struct Operator;
	struct Store;
	
	/**
	 * Three types of queues in the system.  A simple queue has one source
	 * op and one destn op, while a shared queue has one source op and
	 * many destn ops.  
	 */
	
	enum QueueKind {
		SIMPLE_Q,
		READER_Q,
		WRITER_Q
	};
	
	struct Queue {		
		/// indexes in PlanManagerImpl.queues
		unsigned int id;
		
		/// Kind of queue
		QueueKind kind;

		/// Instantiation of this queue
		Execution::Queue *instQueue;
		
		union {
			struct {
				/// The source operator
				Operator *source;

				/// The destination operator
				Operator *dest;

				/// The input index in the destination operator for this
				/// queue
				unsigned int index;
			} SIMPLE;
			
			struct {
				/// The shared writer who holds our state
				Queue *writer;
				
				/// The destination operator
				Operator *dest;

				/// The input index in the destination operator for this
				/// queue
				unsigned int index;

				/// The id of the reader with its writer
				unsigned int readerId;
				
			} READER;
			
			
			struct {
				/// The source operator
				Operator *source;

				/// The number of readers
				unsigned int numReaders;
				
				/// The readers
				Queue *readers [MAX_OUT_BRANCHING];

				/// The storage allocator for tuples flowing through me
				Store *store;
			} WRITER;
		} u;						
	};
}

#endif
