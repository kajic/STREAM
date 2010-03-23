#ifndef _PROPERTY_
#define _PROPERTY_

namespace Monitor {
	/// Time used by an operator
	static const int OP_TIME_USED = 10;
	
	static const int OP_LAST_OUT_TS = 17;
	
	/// Maximum pages alloced by a store
	static const int STORE_MAX_PAGES = 11;
	
	/// Current no of pages alloced by a store
	static const int STORE_NUM_PAGES = 12;
	
	/// Current no of tuples in a synopsis
	static const int SYN_NUM_TUPLES = 13;
	
	/// Max no of tuples in a synopsis
	static const int SYN_MAX_TUPLES = 14;

	/// debug
	static const int RSTORE_NUM_LIST = 15;
	static const int RSTORE_NUM_LIST_UNUSED = 16;

	/// Index
	static const int HINDEX_NUM_BUCKETS = 18;
	static const int HINDEX_NUM_NONMT_BUCKETS = 19;
	static const int HINDEX_NUM_ENTRIES = 20;
	
	/// Join selectivity
	static const int JOIN_NINPUT = 21;
	static const int JOIN_NOUTPUT = 22;
	
	static const int QUEUE_NUM_ELEM = 23;
	static const int QUEUE_LAST_TS = 24;
};

#endif
