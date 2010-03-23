/**
 * @file     win_syn_impl.cc
 * @date     June 2, 2004
 * @brief    Window synopsis store implementation
 */

#ifndef _DEBUG_
#include "common/debug.h"
#endif

#ifndef _WIN_SYN_IMPL_
#include "execution/synopses/win_syn_impl.h"
#endif

using namespace Execution;

WindowSynopsisImpl::WindowSynopsisImpl (unsigned int _id,
										std::ostream &_LOG)
	: LOG (_LOG)
{
	this -> id      = _id;
	this -> store   = 0;
	this -> stubId  = 0;
}

WindowSynopsisImpl::~WindowSynopsisImpl () {}

int WindowSynopsisImpl::setStore (WinStore *store, unsigned int stubId)
{
	ASSERT (store);
	
	this -> store       = store;
	this -> stubId      = stubId;
	
	return 0;
}

int WindowSynopsisImpl::insertTuple (Tuple tuple, Timestamp timestamp)
{

#ifdef _MONITOR_
	logIns ();
#endif
	
	return store -> insertTuple_w (tuple, timestamp, stubId);
}

bool WindowSynopsisImpl::isEmpty () const
{
	return store -> isEmpty_w (stubId);
}

int WindowSynopsisImpl::getOldestTuple(Tuple& tuple, Timestamp& timestamp)
{
	return store -> getOldestTuple_w (tuple, timestamp, stubId);
}

int WindowSynopsisImpl::deleteOldestTuple ()
{
	
#ifdef _MONITOR_
	logDel ();
#endif
	
	return store -> deleteOldestTuple_w (stubId);
}
