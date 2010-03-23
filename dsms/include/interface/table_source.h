#ifndef _TABLE_SOURCE_
#define _TABLE_SOURCE_

namespace Interface {

/**
 * The interface that the STREAM server uses to get input stream /
 * relation tuples.  Various kinds of inputs can be obtained by extending
 * this class (e.g, read from a file, read from a network etc)
 *
 * The main method is getNext(), which teh server uses to pull the next
 * tuple whenever it desires.  
 *
 * Before  consuming any  input  tuples, the  server  invokes the  start()
 * method,   which   can   be   used   to   perform   various   kinds   of
 * initializations. Similarly, the end() method is called ,when the server
 * is not going to invoke any more getNext()s.
 */
	
	class TableSource {
	public:	
		virtual ~TableSource() {}	
		
		/**
		 * Signal that getNext will be invoked next.
		 *
		 * @return            0 (success), !0 (failure)
		 */
		virtual int start() = 0;
		
		/**
		 * Get the next tuple from the table (stream/relation).		 
		 *
		 * @param   tuple     0 (if the next tuple has not yet available)
		 *                    pointer to the location of the next tuple.
		 *                    A tuple is just a pointer to a memory location
		 *                    containing the encoding of the tuple
		 *                    attribute.   Memory is owned by TableSource
		 *                    is guaranteed to be valid only till the
		 *                    next getNext call.
		 *
		 *                    Modified (Dec 6): you can pass heartbeats
		 *                    by setting isHeartbeat to 'true'. Then
		 *                    the first 4 bytes of the tuple should contains
		 *                    the timestamp as usual.
		 *
		 * @param   len       Length of the returned tuple
		 * @return            0 (success), !0 (failure)
		 */
		
		virtual int getNext(char *& tuple, unsigned int& len,
							bool& isHeartbeat) = 0;
		
		/**
		 * Signal that the server needs no more tuples.
		 * 
		 * @return            0 (success), !0 (failure)
		 */
		virtual int end() = 0;
	};
}

#endif
