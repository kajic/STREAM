#ifndef _OUT_CONN_
#define _OUT_CONN_

#ifndef _THREAD_
#include "thread.h"
#endif

#ifndef _QUERY_OUTPUT_
#include "interface/query_output.h"
#endif

#ifndef _TYPES_
#include "common/types.h"
#endif

#ifndef _CONSTANTS_
#include "common/constants.h"
#endif

#include <pthread.h>

#include <ostream>
using std::ostream;

namespace Network {
	class NetworkManager;
	
	class OutputConnection : public Thread, public Interface::QueryOutput {
	private:
		/// system-wide log	   
		std::ostream &LOG;
		
		/// Socket for the TCP connection
		int sockfd;
		
		//------------------------------------------------------------
		// Buffer to xfer data from producer to consumer
		//------------------------------------------------------------
		
		/// Size of each buffer = 512 KB
		static const int BUF_SIZE = 512 * (1 << 10);
		
		/// shared buffer
		char shared_buf [BUF_SIZE];		
		
		/// Index into the next read position
		int read_ptr;
		
		/// Index into the next write position in the shared buffer
		int write_ptr;

		/// Indicator for the end of the output stream
		bool bEnd;
		
		//------------------------------------------------------------
		// Synchronization entities
		//------------------------------------------------------------
		
		/// Mutex for critical code
		pthread_mutex_t mutex;
		
		/// Condition variable for producer waiting for a free buffer space
		pthread_cond_t prod_wait_cond;
		
		/// Indicator for whether the producer is waiting or not
		bool b_prod_waiting;
		
		//------------------------------------------------------------
		// Schema information
		//------------------------------------------------------------
		
		/// Number of (data) attributes in the output schema
		int numAttrs;
		
		/// Types of data attributes
		Type attrTypes [MAX_ATTRS];
		
		/// Length of attributes
		int attrLen [MAX_ATTRS];
		
		/// Offsets @ which these are present
		int offsets [MAX_ATTRS];
		
		/// length of tuples we are expecting
		int tupleLen;
		
		/// Offset of the timestamp
		int tstampOffset;
		
		/// Offset of the sign
		int signOffset;
		
		static const int LINE_BUF_SIZE = 1024;
		
		char lineBuf [LINE_BUF_SIZE];

		/// debug
		long totalBytes;
		
	public:
		OutputConnection (ostream& LOG);
		~OutputConnection ();
		
		void run();

		int setSocket (int sockfd);
		int setNumAttrs(unsigned int numAttrs);
		
		int setAttrInfo(unsigned int attrPos, 
						Type attrType,
						unsigned int attrLen);
		
		int start();
		int putNext(const char *tuple, unsigned int len);
		int end();

	private:
		int consumeBuf ();
		int consumeBuf (char *buf, int size);
		bool buf_empty () const;
		bool buf_full () const;
		int writeln (char *line);
		int writechar (char c);
	};
}

#endif
