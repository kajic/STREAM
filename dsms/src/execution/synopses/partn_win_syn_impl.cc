/**
 * @file          partn_win_syn_impl.cc
 * @date          Sept. 9, 2004
 * @brief         Implementation of partition window synopsis
 */

#ifndef _PARTN_WIN_SYN_IMPL_
#include "execution/synopses/partn_win_syn_impl.h"
#endif

#ifndef _DEBUG_
#include "common/debug.h"
#endif

using namespace Execution;
using namespace std;

PartnWindowSynopsisImpl::PartnWindowSynopsisImpl (unsigned int id,
												  ostream &_LOG)
	: LOG (_LOG)
{
	this -> id = id;
	this -> store = 0;
}

PartnWindowSynopsisImpl::~PartnWindowSynopsisImpl () {}

int PartnWindowSynopsisImpl::setStore (PwinStore *store,
									   unsigned int stubId)
{
	ASSERT (store);
	
	this -> store = store;
	this -> stubId = stubId;
	return 0;
}

int PartnWindowSynopsisImpl::insertTuple (Tuple tuple)
{
#ifdef _MONITOR_
	logIns ();
#endif
	
	return store -> insertTuple_p (tuple, stubId);
}

int PartnWindowSynopsisImpl::deleteOldestTuple (Tuple partnSpec,
												Tuple &oldestTuple)
{
#ifdef _MONITOR_
	logDel ();
#endif
	
	return store -> deleteOldestTuple_p (partnSpec, oldestTuple, stubId);	
}

int PartnWindowSynopsisImpl::getPartnSize (Tuple partnSpec,
										   unsigned int &partnSize)
{
	return store -> getPartnSize_p (partnSpec, partnSize, stubId);
}
