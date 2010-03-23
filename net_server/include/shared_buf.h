#ifndef _SHARED_BUF_
#define _SHARED_BUF_

/**
 * @file       shared_buf.h
 * @date       Nov. 9, 2004
 * @brief      A thread safe shared buffer
 */

namespace Network {

	/**
	 * A thread-safe shared buffer.  One writer can write to a buffer and
	 * one reader from possibly a different thread can read from the
	 * buffer.  The buffer implements a FIFO writing/reading, so this is
	 * more like a shared queue at the granularity of bytes.
	 */
	
	class SharedBuf {
	public:
		
		/**
		 * Get a pointer to the next data that can be read.  On return,
		 * the reader can read upto 'len' bytes from 'ptr'.  The reader
		 * has to inform the buffer that he has read a given number of
		 * bytes by using the commitRead method, otherwise, subsequent
		 * calls to getReadPtr will return the same ptr.
		 *
		 * If there is no data to be read, the buffer returns len = 0.
		 */
		
		int getReadPtr (char *&ptr, int &len);
		
		/**
		 * A reader uses this call to inform the buffer that he has read
		 * 'len' bytes starting from 'ptr'.  'ptr' itself should be a
		 * "valid" location.  ptr is a valid location iff:
		 *
		 *   (a) if the previous call by the reader was a getReadPtr(),
		 *       ptr was returned by the method
		 *   (b) if the previous call by the reader was a commitRead() and
		 *       the previous argument was (ptr - len).
		 *
		 * [[ More explanation ]]
		 */		
		int commitRead (char *ptr, int len);
		
		/**
		 * Get a pointer to the next location that can be written.  On
		 * return, the writer can write upto len bytes from 'ptr'.  The
		 * writer has to inform any write he has done using the
		 * commitWrite() method, otherwise the writes will not be visible
		 * to the reader.
		 */ 
		int getWritePtr (char *&ptr, int &len);
		
		/** 
		 * commitWrite:getWritePtr :: commitRead:getReadPtr
		 */ 
		int commitWrite (char *ptr, int len);
	};
}

#endif
