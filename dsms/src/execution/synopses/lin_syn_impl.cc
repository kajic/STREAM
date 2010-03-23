#ifndef _LIN_SYN_IMPL_
#include "execution/synopses/lin_syn_impl.h"
#endif

#ifndef _DEBUG_
#include "common/debug.h"
#endif

using namespace Execution;

LineageSynopsisImpl::LineageSynopsisImpl (unsigned int id,
										  std::ostream &_LOG)
	: LOG (_LOG)
{
	this -> id = id;
	this -> store = 0;
}

LineageSynopsisImpl::~LineageSynopsisImpl () {}

int LineageSynopsisImpl::setStore (LinStore *store, unsigned int stubId)
{
	ASSERT (store);

	this -> stubId = stubId;
	this -> store = store;

	return 0;
}

int LineageSynopsisImpl::insertTuple (Tuple tuple, Tuple *lineage)
{
	ASSERT (store);

#ifdef _MONITOR_
	logIns ();
#endif
	
	return store -> insertTuple_l (tuple, lineage, stubId);
}

int LineageSynopsisImpl::deleteTuple (Tuple tuple)
{
	ASSERT (store);

#ifdef _MONITOR_
	logDel ();
#endif
	
	return store -> deleteTuple_l (tuple, stubId);
}

int LineageSynopsisImpl::getTuple (Tuple *lineage, Tuple &tuple)
{
	ASSERT (store);
	
	return store -> getTuple_l (lineage, tuple, stubId);
}

	
