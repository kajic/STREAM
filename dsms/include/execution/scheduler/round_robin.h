#ifndef _ROUND_ROBIN_
#define _ROUND_ROBIN_

#ifndef _SCHEDULER_
#include "execution/scheduler/scheduler.h"
#endif

namespace Execution {
	class RoundRobinScheduler : public Scheduler {
	private:
		static const unsigned int MAX_OPS = 100;
		
		// Operators that we are scheduling
		Operator *ops [MAX_OPS];
		
		// Number of operators that we have to schedule
		unsigned int numOps;
		
		bool bStop;
		
	public:
		RoundRobinScheduler ();		
		virtual ~RoundRobinScheduler ();
		
		// Inherited from Scheduler
		int addOperator (Operator *op);				
		int run (long long int numTimeUnits);
		int stop ();
		int resume ();
	};
}

#endif
