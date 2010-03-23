#ifndef _NET_TABLE_SRC_
#define _NET_TABLE_SRC_


#ifndef _SHARED_BUF_
#include "shared_buf.h"
#endif

#ifndef _CONSTANTS_
#include "common/constants.h"
#endif

#ifndef _TYPES_
#include "common/types.h"
#endif


/**
 * @file         net_table_src.h
 * @date         Nov. 9, 2004
 * @brief        A table source that reads over the network
 */

namespace Network {
	
	class NetTableSrc : public Interface::TableSource {
	private:
		/// Shared buffer from which we read the input 
		SharedBuf *sharedBuf;
		
		/// Pointer into the shared buffer
		char *bufPtr;
		
		/// Number of bytes of the shared buffer accessible to us.
		int nValidBytes;		
		
		/// Length of each input tuple
		int inTupLen;
		
		static const int MAX_TUPLE_LEN = 1024;
		
		/// Buffer for output tuples
		char outTupBuf [MAX_TUPLE_LEN];
		
		/// Length of output tuples (usually inTupLen)
		int outTupLen;
		
		static const int SCRATCH_BUF_LEN = 1024;
		
		/// Scratch buffer
		char scratchBuf [SCRATCH_BUF_LEN];
		
		/// Number of bytes of scratch buffer used
		int scrBufUsed;

		//----------------------------------------------------------------------
		// Initialization related
		//----------------------------------------------------------------------
		
		/// Did we get properly initialized?
		bool bInit;
		
		/// States of the object during initialization.
		enum State {
			START,
			READ_SCHEMA_LEN,
			READ_SCHEMA
		};
		
		/// initState is initially set to  START.  After the length of the
		/// schema   message   is   completely   read,  it   is   set   to
		/// READ_SCHEMA_LEN.  After  the schema string  is completely read
		/// it is set to READ_SCHEMA.		
		State initState;		
		
		/// Length of the schema message
		int schemaLen;

		//----------------------------------------------------------------------
		// Schema information
		//----------------------------------------------------------------------
		
		/// Types of attributes
		Type attrTypes [MAX_ATTRS];
		
		/// Attr lengthts
		int attrLen [MAX_ATTRS];
		
		/// Offsets of attributes in the tupleBuf
		int outOffsets [MAX_ATTRS];

		/// Offsets of attributes in the input tuple strings
		int inOffsets [MAX_ATTRS];
		
		/// Number of attributes
		int numAttrs;
		
	public:
		int start ();
		int getNext (char *&tuple, unsigned int &len);
		int end();
		
	private:
		
		int init ();
		int parseSchema (char *schemaStr);
		int computeOffsets ();			
		int decodeTuple (char *inTup, char *outTup);
		
		/// Read the next 'n' bytes from the buffer.  'n' has to be less
		/// than SCRATCH_BUF_LEN
		int readn (char *&ptr, int n);
	};
}

#endif
