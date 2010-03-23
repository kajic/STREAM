#ifndef _IN_CONN_
#define _IN_CONN_

/**
 * @file      in_conn.h
 * @date      Nov. 10, 2004
 * @brief     Input connection
 */

#ifndef _THREAD_
#include "thread.h"
#endif

#ifndef _TABLE_SOURCE_
#include "interface/table_source.h"
#endif

#ifndef _TYPES_
#include "common/types.h"
#endif

#ifndef _CONSTANTS_
#include "common/constants.h"
#endif

#ifndef _NET_QUEUE_
#include "queue.h"
#endif

#include <pthread.h>

#include <ostream>
using std::ostream;

namespace Network {
	class NetworkManager;

	/**
	 * An input connection.  An input connection object (typically) runs
	 * as part of two threads.  The first thread which executes the run()
	 * method talks to a socket connection, and writes the tuples input in
	 * this connection into a queue.  The second thread calls start(),
	 * getNext() and end() methods dequeued tuples from the queue.	 
	 */ 
	
	class InputConnection : public Thread, public Interface::TableSource {		
		/// Event log
		std::ostream &LOG;
		
		/// Socket for the TCP connection
		int sockfd;
		
		/// Have we been properly initialized?
		bool bInit;
		
		/// Queue for transferring tuples from one thread to another.
		Queue *queue;
		
		/// Last tuple that we returned in getNext()
		char *lastTuple;

		//----------------------------------------------------------------------
		// Read buffering
		//----------------------------------------------------------------------
		
		/// Size of the read buffer: 32 KB
		static const int READ_BUF_SIZE = 32 * (1 << 10);
		
		/// Buffer used by readline() method to perform buffered net i/o		
		char readBuf[READ_BUF_SIZE];
		
		/// Next unread position in the buffer
		char *curPtr;
		
		/// Number of valid bytes in the buffer currently
		int nbytes;

		/// Have we reached the end of the connection
		bool beof;

		/// Has the end() method been called by server ?
		bool bEnd;

		//----------------------------------------------------------------------
		// Schema Information
		//----------------------------------------------------------------------
		
		/// Types of attributes
		Type attrTypes [MAX_ATTRS];
		
		/// Attr lengthts
		int attrLen [MAX_ATTRS];
		
		/// Offsets of attributes in the tupleBuf
		int offsets [MAX_ATTRS];
		
		/// Number of attributes
		int numAttrs;		
		
		/// Length of the tuples
		int tupleLen;		


		//------------------------------------------------------------
		// Synchronization related ...
		//------------------------------------------------------------
		
		/// Mutex for critical code
		pthread_mutex_t mutex;

		/// Input connection waiting for server to call end()
		pthread_cond_t wait_for_end_cond;
		
		/// are we waiting for end
		bool b_wait_for_end;
		
	public:
		
		InputConnection (ostream& LOG);
		
		int setSocket (int sockfd);
		
		virtual void run();
		
		int start ();
		int getNext (char *&tuple, unsigned int &len, bool& isHeartbeat);
		int end();
		
	private:
		int initialize ();
		int constructTuple (char *tuple, char *line);
		int parseSchema (char *str);
		int computeOffsets ();
		int readline (char *lineBuf, int bufsize);
		int loadBuf ();
	};
}
#endif
