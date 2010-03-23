#ifndef _NET_QUEUE_
#define _NET_QUEUE_

#include <ostream>
using std::ostream;

#include <pthread.h>

namespace Network {

	/**
	 * A queue of fixed length objects.  The queue is thread safe (but it
	 * assumes one reader and one writer).
	 *
	 * This queue is used to exchange information from the input
	 * connection thread and the dsms thread, and from the dsms thread to
	 * an output connection thread
	 */
	
	class Queue {
	private:
		/// Log for system events
		ostream &LOG;
		
		// 64 KB
		static const int BUF_SIZE = (1 << 16);
		
		/// Buffer for the queue objects
		char buffer [BUF_SIZE];
		
		/// Length of each queue object
		int objLen;
		
		/// Pointer to the last slot
		char *lastSlot;
		
		/// Next slot to read from.  Contains valid data iff 
		/// (nextReadSlot != nextWriteSlot) || (!bEmpty)
		char *nextReadSlot;
		
		/// The slot is valid only if the queue is not full:
		/// (nextReadSlot != nextWriteSlot) || (bEmpty)
		char *nextWriteSlot;
		
		/// Indicator whether queue is empty or not
		bool bEmpty;
		
		/// Indicator whether the writer is blocked for the queue to clear
		/// up some slots
		bool bWriterBlocked;
		
		/// Indicator whether the reader is blocked for the queue to
		/// become nonempty
		bool bReaderBlocked;
		
		/// Mutex for critical code
		pthread_mutex_t mutex;
		
		/// Condition variable for waiting/signaling on queue nonempty condition
		pthread_cond_t nonempty_cond;
		
		/// Condition variable for waiting/signaling on queue nonfull condition
		pthread_cond_t nonfull_cond;
		
	public:
		Queue (ostream &LOG, int objLen);
		~Queue ();
		
		/**
		 * Get the next write slot for the writer.  On return 'ptr' points
		 * to the beginning of the slot, and the writer can write one
		 * object to this location (the writer should know the length of
		 * an object).  The written object is not visible to the reader
		 * until the commitWrite() method is called.
		 *
		 * The getNextWriteSlot() method blocks if the queue is full
		 * and remains so until the reader reads one object off from the
		 * queue.  If the queue is not full, this calls  does not even
		 * involve any synchronization calls - so it is never blocking.
		 */ 
		int getNextWriteSlot (char *&ptr);
		
		/**
		 * Commit an object written to the location pointed to by the
		 * pointer.  commitWrite() and getNextWriteSlot() should
		 * strictly interleave and the 'ptr' value used in commitWrite()
		 * should be the same returned by the previous
		 * getNextWriteSlot().
		 *
		 * This call could block *temporarily* since it has some critical
		 * code.  But the blocking occurs only if the reader is in his
		 * critical code, and the blocking ends after the reader is out of
		 * his critical code.
		 */
		
		int commitWrite (char *ptr);
		
		/**
		 * Returns true if the queue is empty and false otherwise.  No
		 * synchronization involved, nonblocking.
		 */
		bool isEmpty () const;
		
		/**
		 * Get the next read slot.  On return 'ptr' points to beginning of
		 * the next object.  The read slot is not dequeued from the queue
		 * until the commitRead() method is called.
		 *
		 * This method blocks if the queue is empty until the writer
		 * writes some objects.  But if the queue is not empty, it does not
		 * involve any synchronization calls and is never blocking.
		 */
		
		int getNextReadSlot (char *&ptr);
		
		/**
		 * commit a read operation.  This dequeues the last read object.
		 * This call could block temporarily since it has some critical
		 * code.  But the blocking is oinly for the duration of the
		 * writers critical code.
		 */
		
		int commitRead (char *ptr);		
	};
}

#endif
