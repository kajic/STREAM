/**
 * @file     sys_stream_gen.cc
 * @date     Jan 13, 2004
 * @brief    System stream generator
 */

#include <sys/time.h>

#ifndef _SYS_STREAM_GEN_
#include "execution/operators/sys_stream_gen.h"
#endif

#ifndef _PROPERTY_H_
#include "execution/monitors/property.h"
#endif

#ifndef _JOIN_MONITOR_
#include "execution/monitors/join_monitor.h"
#endif

#ifndef _DEBUG_
#include "common/debug.h"
#endif

using namespace Execution;
using std::endl;

extern unsigned int CPU_SPEED;

SysStreamGen::SysStreamGen (unsigned int _id, ostream &_LOG)
	: LOG (_LOG)
{
	id           = _id;
	
	numOps       = 0;
	numQueues    = 0;
	numSyns      = 0;
	numStores    = 0;
	numJoins     = 0;
	
	numOutput    = 0;
	numPMeasure  = 0;
	numDirty     = 0;
	lastOutTs    = 0;
	startTime    = 0;
	
	for (int o = 0 ; o < MAX_OUTPUT ; o++) {
		outputQueues [o] = 0;
		outStore [o] = 0;
		index [o] = 0;
	}
	
	iCPS = TIME_PER_SEC * 1.0 /
		(1.0 * CPU_SPEED * 1000 * 1000);
}

SysStreamGen::~SysStreamGen () {}

int SysStreamGen::addOutput (Queue *queue, StorageAlloc *store)
{
	if (numOutput >= MAX_OUTPUT)
		return -1;
	
	outputQueues [numOutput] = queue;
	outStore [numOutput] = store;
	numOutput ++;
	
	return 0;
}

int SysStreamGen::addOpEntity (unsigned int id,
							   Monitor::PropertyMonitor *mon)
{
	if (numOps >= MAX_OPS) {
		LOG << "SysStreamGen: too many operators" << endl;
		return -1;
	}
	
	ops [numOps].id = (int)id;
	ops [numOps].monitor = mon;
	ops [numOps].lastOpTime = 0.0;	
	
	numOps++;
	return 0;
}

int SysStreamGen::addQueueEntity (unsigned int id,
								  Monitor::PropertyMonitor *mon)
{
	if (numQueues >= MAX_QUEUES) {
		LOG << "SysStreamGen: too many queues" << endl;
		return -1;
	}

	queues [numQueues].id = (int)id;
	queues [numQueues].monitor = mon;
	queues [numQueues].numTuples = 0;
	queues [numQueues].lastTs = 0;
	
	numQueues++;
	return 0;
}

int SysStreamGen::addJoinEntity (unsigned int id,
								 Monitor::PropertyMonitor *mon)
{
	if (numJoins >= MAX_JOINS) {
		LOG << "SysStreamGen: too many joins" << endl;
		return -1;
	}

	joins [numJoins].id = (int)id;
	joins [numJoins].monitor = mon;
	joins [numJoins].numInputLast = 0;
	joins [numJoins].numJoinedLast = 0;
	numJoins ++;
	
	return 0;
}

int SysStreamGen::addSynEntity (unsigned int id,
								Monitor::PropertyMonitor *mon)
{
	if (numSyns >= MAX_SYNS) {
		LOG << "SysStreamGen: too many syns" << endl;
		return -1;
	}

	syns [numSyns].id = (int)id;
	syns [numSyns].monitor = mon;
	numSyns++;

	return 0;
}

int SysStreamGen::addStoreEntity (unsigned int id,
								  Monitor::PropertyMonitor *mon)
{
	if (numStores >= MAX_STORES) {
		LOG << "SysStreamGen: too many stores" << endl;
		return -1;
	}

	stores [numStores].id = (int)id;
	stores [numStores].monitor = mon;
	numStores ++;

	return 0;
}

int SysStreamGen::run (TimeSlice timeSlice)
{
	int rc;
	Timestamp curTs;
	Tuple outTuple;
	Element outElement;
	
#ifdef _MONITOR_
	startTimer ();
#endif
	
	// Get the current time
	curTs = getCurTs ();
	
	// time to refresh our measurements?
	if (curTs > lastOutTs) {
		if ((rc = refresh (curTs)) != 0)
			return rc;
		
		lastOutTs = curTs;
	}

	// send out pending tuples on each output
	for (int o = 0 ; (o < numOutput) && (numDirty > 0) ; o++) {
		
		for (; index [o] < numPMeasure && !outputQueues [o] -> isFull() ;
			 index [o]++) {
			
			// allocate tuple for the output
			if ((rc = outStore [o] -> newTuple (outTuple)) != 0)
				return rc;
			
			// copy the tuple values
			ICOL (outTuple, TYPE_COL) = pmeasure [index [o]].type;
			ICOL (outTuple, ID_COL) = pmeasure [index [o]].id;
			ICOL (outTuple, PROPERTY_COL) = pmeasure [index [o]].property;
			ICOL (outTuple, IVAL_COL) = pmeasure [index [o]].ival;
			FCOL (outTuple, FVAL_COL) = pmeasure [index [o]].fval;
			
			// output element
			outElement.tuple = outTuple;
			outElement.kind = E_PLUS;
			outElement.timestamp = curTs;
			
			outputQueues [o] -> enqueue (outElement);			
		}
		
		if (index [o] == numPMeasure)
			numDirty--;
	}
	
#ifdef _MONITOR_
	stopTimer ();
#endif
	
	return 0;
}

int SysStreamGen::refresh (Timestamp curTs)
{
	int rc;
	
	numPMeasure = 0;
	for (int o = 0 ; o < numOutput ; o++)
		index [o] = 0;

	// Get property measurements from various entities
	if ((rc = refreshOps (curTs)) != 0)
		return rc;
	if ((rc = refreshJoins (curTs)) != 0)
		return rc;
	if ((rc = refreshQueues (curTs)) != 0)
		return rc;
	if ((rc = refreshSyns (curTs)) != 0)
		return rc;
	if ((rc = refreshStores (curTs)) != 0)
		return rc;
	
	if (numPMeasure > 0)
		numDirty = numOutput;
	
	return 0;
}

int SysStreamGen::refreshOps (Timestamp curTs)
{
	int rc;
	double opTime;
	float opTimeFrac;
	
	for (int o = 0 ; o < numOps && numPMeasure < MAX_MEASURE ; o++) {
		
		rc = ops [o].monitor -> getDoubleProperty (Monitor::OP_TIME_USED,
												   opTime);
		if (rc != 0) return rc;
		
		opTimeFrac = ((float)opTime - ops [o].lastOpTime) * TIME_PER_SEC;
		ops [o].lastOpTime = (float)opTime;
		
		pmeasure [numPMeasure].type = OP;
		pmeasure [numPMeasure].id = ops [o].id;
		pmeasure [numPMeasure].property = SS_OP_TIME;
		pmeasure [numPMeasure].ival = 0; // unused
		pmeasure [numPMeasure].fval = opTimeFrac; 
		
		numPMeasure ++;
	}
	
	if (numPMeasure == MAX_MEASURE) {
		LOG << "SysStreamGen: Property table overflow" << endl;
		return -1;
	}
	
	return 0;
}

int SysStreamGen::refreshQueues (Timestamp curTs)
{
	int rc;
	int numTuples;
	int lastAppTs;
	float rate;	
	
	for (int q = 0 ; q < numQueues ; q++) {
		ASSERT (curTs > queues [q].lastTs);

		if (numPMeasure >= MAX_MEASURE) {
			LOG << "SysStreamGen: Property table overflow" << endl;
			return -1;
		}
		
		rc = queues [q].monitor -> getIntProperty (Monitor::QUEUE_NUM_ELEM,
												   numTuples);
		if (rc != 0) return rc;
		
		ASSERT (numTuples >= queues [q].numTuples);
		
		rate = (numTuples - queues [q].numTuples) * 1.0 * TIME_PER_SEC /
			(curTs - queues [q].lastTs);
		
		// update state
		queues [q].numTuples = numTuples;
		queues [q].lastTs = curTs;
		
		pmeasure [numPMeasure].type = QUEUE;
		pmeasure [numPMeasure].id = queues [q].id;
		pmeasure [numPMeasure].property = SS_QUEUE_RATE;
		pmeasure [numPMeasure].ival = 0; // unused
		pmeasure [numPMeasure].fval = rate;
		
		numPMeasure ++;
		
		if (numPMeasure >= MAX_MEASURE) {
			LOG << "SysStreamGen: Property table overflow" << endl;
			return -1;
		}
		
		rc = queues [q].monitor -> getIntProperty (Monitor::QUEUE_LAST_TS,
												   lastAppTs);
		if (rc != 0) return rc;		
		
		pmeasure [numPMeasure].type = QUEUE;
		pmeasure [numPMeasure].id = queues [q].id;
		pmeasure [numPMeasure].property = SS_QUEUE_TS;
		pmeasure [numPMeasure].ival = lastAppTs;
		pmeasure [numPMeasure].fval = 0.0; // unused
		
		numPMeasure ++;		
	}
	
	return 0;
}

int SysStreamGen::refreshJoins (Timestamp curTs)
{
	int rc;
	int numInput, numJoined;
	int numInputDiff, numJoinedDiff;
	float selectivity;

	for (int j = 0 ; j < numJoins && numPMeasure < MAX_MEASURE ; j++) {
		
		rc = joins[j].monitor -> getIntProperty (Monitor::JOIN_NINPUT,
												 numInput);
		if (rc != 0) return rc;

		rc = joins [j].monitor -> getIntProperty (Monitor::JOIN_NOUTPUT,
												  numJoined);
		if (rc != 0) return rc;

		numInputDiff = numInput - joins[j].numInputLast;
		numJoinedDiff = numJoined - joins[j].numJoinedLast;

		ASSERT (numInputDiff >= 0);
		ASSERT (numJoinedDiff >= 0);
		
		if (numInputDiff > 0)
			selectivity = numJoinedDiff * 1.0 / numInputDiff;
		else
			selectivity = 0.0;
		
		pmeasure [numPMeasure].type = OP;
		pmeasure [numPMeasure].id = joins [j].id;
		pmeasure [numPMeasure].property = SS_JOIN_SEL;
		pmeasure [numPMeasure].ival = 0;
		pmeasure [numPMeasure].fval = selectivity;

		numPMeasure ++;

		joins [j].numInputLast = numInput;
		joins [j].numJoinedLast = numJoined;
	}
	
	if (numPMeasure >= MAX_MEASURE) {
		LOG << "SysStreamGen: Property table overflow" << endl;
		return -1;
	}

	return 0;
}

int SysStreamGen::refreshSyns (Timestamp curTs)
{
	int rc;
	int numTuples;
	
	for (int s = 0 ; s < numSyns && numPMeasure < MAX_MEASURE ; s++) {
		
		// number of tuples in the synopsis
		rc = syns [s].monitor -> getIntProperty (Monitor::SYN_NUM_TUPLES,
												 numTuples);
		if (rc != 0) return rc;
		
		pmeasure [numPMeasure].type = SYN;
		pmeasure [numPMeasure].id = syns [s].id;
		pmeasure [numPMeasure].property = SS_SYN_CARD;
		pmeasure [numPMeasure].ival = numTuples;
		pmeasure [numPMeasure].fval = 0.0;
		
		numPMeasure ++;
	}
	
	return 0;
}

int SysStreamGen::refreshStores (Timestamp curTs)
{
	int rc;
	int numPages;

	for (int s = 0 ; s < numStores && numPMeasure < MAX_MEASURE ; s++) {

		rc = stores [s].monitor -> getIntProperty (Monitor::STORE_NUM_PAGES,
												   numPages);
		if (rc != 0) return rc;

		pmeasure [numPMeasure].type = STORE;
		pmeasure [numPMeasure].id = stores [s].id;
		pmeasure [numPMeasure].property = SS_STORE_SIZE;
		pmeasure [numPMeasure].ival = numPages;
		pmeasure [numPMeasure].fval = 0.0;
		
		numPMeasure ++;
	}
	
	return 0;
}
	

/**
 * Assembly code fragment from:
 * 
 * http://www.ncsa.uiuc.edu/UserInfo/Resources/Hardware/IA32LinuxCluster/Doc/timing.html
 * 
 * to get the wall clock time
 */ 

static unsigned long long int nanotime_ia32(void)
{
	unsigned long long int val;
	__asm__ __volatile__("rdtsc" : "=A" (val) : );
	return(val);
}

Timestamp SysStreamGen::getCurTs ()
{
	Timestamp curTs;
	unsigned long long int curTime;
	
	if (startTime == 0) {
		startTime = nanotime_ia32();
		return (Timestamp)0;
	}
	
	curTime = nanotime_ia32();
	curTs = (Timestamp) ((curTime - startTime) * iCPS);
	return curTs;
}
