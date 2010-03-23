#ifndef _SYS_STREAM_GEN_
#define _SYS_STREAM_GEN_

/**
 * @file    sys_stream_gen.h
 * @date    Jan 13, 2005
 * @brief   System stream generator
 */

#ifndef _OPERATOR_
#include "execution/operators/operator.h"
#endif

#ifndef _QUEUE_
#include "execution/queues/queue.h"
#endif

#ifndef _STORE_ALLOC_
#include "execution/stores/store_alloc.h"
#endif

#ifndef _SYS_STREAM_
#include "common/sys_stream.h"
#endif

#ifndef _CPP_OSTREAM
#include <ostream>
#endif

using std::ostream;

extern unsigned int CPU_SPEED;

namespace Execution {
	
	// Operator entity
	struct OpEntity {		
		/// Id of the operator
		int id;
		
		/// Property monitor for the operator
		Monitor::PropertyMonitor *monitor;
		
		/// time used by operator last time we measured
		float lastOpTime;
	};
	
	// Queue entity
	struct QueueEntity {
		/// Id of the queue
		int id;
		
		/// Property monitor for the queue
		Monitor::PropertyMonitor *monitor;
		
		/// Number of tuples last time we measured
		int numTuples;
		
		/// The time when we measured numTuples last
		Timestamp lastTs;
	};
	
	// Synopsis entity
	struct SynEntity {
		// id of the synopsis
		int id;
		
		// Property monitor for the synopsis
		Monitor::PropertyMonitor *monitor;
	};
	
	// Store Entity
	struct StoreEntity {
		// id of the store
		int id;
		
		// Property monitor for the store
		Monitor::PropertyMonitor *monitor;
	};
	
	// Join Entity
	struct JoinEntity {
		// id for the join
		int id;
		
		// Property monitor for the join
		Monitor::PropertyMonitor *monitor;

		int numInputLast;

		int numJoinedLast;
	};
	
	// Property Measurement
	struct PMeasure {
		int type;
		int id;
		int property;
		int ival;
		float fval;
	};
	
	class SysStreamGen : public Operator {
		/// System-wide id
		unsigned int id;
		
		/// System log
		ostream &LOG;
		
		//------------------------------------------------------------
		// Entity Information
		//------------------------------------------------------------
		
		static const int MAX_OPS    = 1000;
		static const int MAX_QUEUES = 1000;
		static const int MAX_SYNS   = 1000;
		static const int MAX_STORES = 1000;
		static const int MAX_JOINS  = 50;
		
		OpEntity ops [MAX_OPS];
		JoinEntity joins [MAX_JOINS];
		QueueEntity queues [MAX_QUEUES];
		SynEntity syns [MAX_SYNS];
		StoreEntity stores [MAX_STORES];
		
		int numOps;
		int numJoins;
		int numQueues;
		int numSyns;
		int numStores;
		
		//------------------------------------------------------------		
		// Output information
		//------------------------------------------------------------
		
		/// Maximum number of output operators who read from the stream
		static const int MAX_OUTPUT = 50;
		
		/// Number of operators reading my output
		int numOutput;
		
		/// Output queues 
		Queue *outputQueues [MAX_OUTPUT];
		
		/// storage allocator to allocate tuples for each output
		StorageAlloc *outStore [MAX_OUTPUT];
		
		//------------------------------------------------------------
		// Property measurement
		//------------------------------------------------------------
		
		/// Size of property table
		static const int MAX_MEASURE = 2000;
		
		/// Property table:  contains the latest  measured properties.
		/// Why do we store them  instead of streaming?  We need to do
		/// so to handle stalling at a given input due to which we may
		/// not stream a property measurement as soon as we measure.
		PMeasure pmeasure [MAX_MEASURE];
		
		// Number of entries in property table
		int numPMeasure;
		
		/// Index into pmeasure table.  For output o, all tuples
		/// index[o] ... (numPMeasure-1) should be sent out
		int index [MAX_OUTPUT];
		
		/// Number of outputs for which we still have one or more tuples
		/// to send currently
		int numDirty;		
		
		//------------------------------------------------------------
		// Timing related information
		//------------------------------------------------------------
		
		/// Last output timestamp.  (Interpretation - 10 units = 1 sec)
		Timestamp lastOutTs;
		
		/// Manually hardcoded reciprocal of number of clock ticks
		/// per time unit (2 Gz machine)
		double iCPS;
		
		/// time at which we started executing ... (for calculating
		/// elapsed time)
		unsigned long long int startTime;
		
		//------------------------------------------------------------
		// Tuple layout related
		//------------------------------------------------------------
		
		static const Column TYPE_COL = 0;
		static const Column ID_COL = 1;
		static const Column PROPERTY_COL = 2;
		static const Column IVAL_COL = 3;
		static const Column FVAL_COL = 4;		
		
	public:
		SysStreamGen (unsigned int id, ostream &LOG);
		virtual ~SysStreamGen ();
		
		int addOutput (Queue *queue, StorageAlloc *store);
		int addOpEntity (unsigned int id, Monitor::PropertyMonitor *mon);
		int addQueueEntity (unsigned int id, Monitor::PropertyMonitor *mon);
		int addJoinEntity (unsigned int id, Monitor::PropertyMonitor *mon);
		int addSynEntity (unsigned int id, Monitor::PropertyMonitor *mon);
		int addStoreEntity (unsigned int id, Monitor::PropertyMonitor *mon);
		
		int run (TimeSlice timeSlice);
		
	private:
		
		int refresh (Timestamp curTs);
		int refreshOps (Timestamp curTs);
		int refreshQueues (Timestamp curTs);
		int refreshJoins (Timestamp curTs);
		int refreshSyns (Timestamp curTs);
		int refreshStores (Timestamp curTs);
		Timestamp getCurTs ();
	};
}

#endif
