
#ifndef _DEBUG_
#include "common/debug.h"
#endif

#ifndef _REL_SYN_IMPL_
#include "execution/synopses/rel_syn_impl.h"
#endif

using namespace Execution;

RelationSynopsisImpl::RelationSynopsisImpl (unsigned int _id,
											std::ostream &_LOG)
	: LOG (_LOG)
{
	this -> id          = _id;
	this -> store       = 0;
	this -> numScans    = 0;
	this -> numIndexes  = 0;
	this -> evalContext = 0;
}

RelationSynopsisImpl::~RelationSynopsisImpl () {}

int RelationSynopsisImpl::setStore (RelStore *store, unsigned int stubId)
{
	ASSERT (store);
	
	this -> store  = store;
	this -> stubId = stubId;
	
	return 0;
}

int RelationSynopsisImpl::setEvalContext (EvalContext *evalContext)
{
	ASSERT (evalContext);

	this -> evalContext = evalContext;
	return 0;
}

int RelationSynopsisImpl::setIndexScan (BEval *predicate, Index *index, 
										unsigned int& scanId) 
{
	ASSERT (numIndexes <= MAX_INDEXES);
	ASSERT (numScans <= MAX_SCANS);
	ASSERT (index);

	if (numScans == MAX_SCANS)
		return -1;
	if (numIndexes == MAX_INDEXES)
		return -1;
	
	scanId = numScans++;	
	scans [scanId].predicate = predicate;
	scans [scanId].index     = index;
	
	indexes [numIndexes++] = index;
	
	return 0;
}

int RelationSynopsisImpl::setScan (BEval *predicate, unsigned int& scanId)
{
	ASSERT (numScans < MAX_SCANS);
	
	scanId = numScans ++;
	
	scans [scanId].predicate = predicate;
	scans [scanId].index     = 0;
	
	return 0;
}

int RelationSynopsisImpl::initialize ()
{
	for (unsigned int s = 0 ; s < numScans ; s++) {
		
		if (scans [s].predicate) {
			filterIters [s] = new FilterIterator (evalContext,
												  scans [s].predicate);
		}
		
		else {
			filterIters [s] = 0;
		}

		sourceIters [s] = 0;
	}
	
	return 0;
}

int RelationSynopsisImpl::insertTuple (Tuple tuple)
{
	int rc;
	
	for (unsigned int i = 0 ; i < numIndexes ; i++)
		if ((rc = indexes [i] -> insertTuple (tuple)) != 0)
			return rc;

#ifdef _MONITOR_
	logIns ();
#endif
	
	return store -> insertTuple_r (tuple, stubId);
}

int RelationSynopsisImpl::deleteTuple (Tuple tuple)
{
	int rc;

	for (unsigned int i = 0 ; i < numIndexes ; i++)
		if ((rc = indexes [i] -> deleteTuple (tuple)) != 0)
			return rc;

#ifdef _MONITOR_
	logDel ();
#endif
	
	return store -> deleteTuple_r (tuple, stubId);
}

int RelationSynopsisImpl::getScan (unsigned int scanId, TupleIterator *&iter)
{
	int rc;
	TupleIterator *indexIter, *fullIter;
	
	ASSERT (scanId < numScans);

	if (scans [scanId].index) {
		if ((rc = scans [scanId].index -> getScan (indexIter)) != 0)
			return rc;
		
		if (scans [scanId].predicate) {
			ASSERT (filterIters [scanId]);

			rc = filterIters [scanId] -> initialize (indexIter);
			if (rc != 0) return rc;
			
			iter = filterIters [scanId];

			sourceIters [scanId] = indexIter;
		}
		else {			
			iter = indexIter;
		}		
	}
	
	else {
		if ((rc = store -> getScan_r (fullIter, stubId)) != 0)
			return rc;
		
		if (scans [scanId].predicate) {
			ASSERT (filterIters [scanId]);
			
			rc = filterIters [scanId] -> initialize (fullIter);
			if (rc != 0) return rc;
			
			iter = filterIters [scanId];

			sourceIters [scanId] = fullIter;
		}
		
		else {
			iter = fullIter;
		}
	}
	
	return 0;
}

int RelationSynopsisImpl::releaseScan (unsigned int scanId, TupleIterator *iter)
{
	int rc = 0;
	if (sourceIters [scanId]) {
		ASSERT (scans [scanId].predicate);
		if (scans [scanId].index) {
			rc = scans [scanId].index -> releaseScan (sourceIters [scanId]);
		}
		
		else {
			rc = store -> releaseScan_r (sourceIters [scanId], stubId);
		}		
	}
	else {
		ASSERT (!scans [scanId].predicate);
	}
	
	return rc;
}
