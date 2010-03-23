#ifndef _SYS_STREAM_
#define _SYS_STREAM_

#ifndef _TYPES_
#include "common/types.h"
#endif

/// The name by which the system stream is referred to in the queries
static const char *SYS_STREAM = "SysStream";

/// System wide identifier for SYS_STREAM (TableManager ensures that
/// lookup for "SysStream" returns Id '0')
static const unsigned int SS_ID = 0;

/// Number of attributes in SYS_STREAM
static const unsigned int SS_NUM_ATTRS = 5;

/// Attributes of SYS_STREAM
static const char *SS_ATTRS[] = {
	// Type of entity (operator, queue, syn, store)
	"Type",
	
	// Id of the entity
	"Id",
	
	// Property id
	"Property",
	
	// Property value (integer)
	"Ival",
	
	// property value (real)
	"Fval"
};

/// Attribute types
static const Type SS_ATTR_TYPES[] = {INT, INT, INT, INT, FLOAT};

static const unsigned int SS_ATTR_LEN[] = {INT_SIZE, INT_SIZE, INT_SIZE,
										   INT_SIZE, FLOAT_SIZE};

//------------------------------------------------------------
// Entity types
//------------------------------------------------------------

static const int OP    = 0;
static const int QUEUE = 1;
static const int SYN   = 2;
static const int STORE = 3;

//------------------------------------------------------------
// Property Ids
//------------------------------------------------------------

// Time consumed by an operator
static const int SS_OP_TIME = 0;

// Rate of tuple flow in a queue
static const int SS_QUEUE_RATE = 1;

// Last enqueued timestamp in a queue
static const int SS_QUEUE_TS = 2;

// Join selectivity
static const int SS_JOIN_SEL = 3;

// Number of tuples in synopsis
static const int SS_SYN_CARD = 4;

// Number of pages used by store
static const int SS_STORE_SIZE = 5;

/// Number of time units per second
static const int TIME_PER_SEC = 5;

#endif
