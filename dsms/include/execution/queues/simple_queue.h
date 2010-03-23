#ifndef _SIMPLE_QUEUE_
#define _SIMPLE_QUEUE_

#include <ostream>

#ifndef _QUEUE_
#include "execution/queues/queue.h"
#endif

#ifndef _MEMORY_MGR_
#include "execution/memory/memory_mgr.h"
#endif

/**
 * A simple implementation of a queue.  The queue has the following
 * characteristics:
 *
 * 1. Works with a fixed number of pages (got from memory manager).
 * 2. Allocates all pages @ start time.
 * 3. Does not grow / shrink dynamically (restatement of 1)
 */

namespace Execution {
	class SimpleQueue : public Queue {
	private:
		// System wide unique id assigned to me.
		int id;
		
		// Memory manager for the pages
		MemoryManager *memMgr;
		
		// Size of pages served by memory manager
		unsigned int pageSize;
		
		// Number of pages of memory that I am allowed to have
		unsigned int numPages;

		// Each page contains a pointer to the next page in the circular
		// buffer organization of the page: nextPtrOffset contains the
		// offset of this pointer from the start of the page
		unsigned int nextPtrOffset;

		// offset of next page pointer from the last element in a page: we
		// store this to efficiently move frm the last element of a page
		// to the first elmeent of the next page
		unsigned int nextPtrOffset_le;
		
		// number of elements that I can store in a page
		unsigned int numElementsPerPage;
		
		// Maximum number of elements that I can enqueue:
		// numElementsPerPage x numPages
		unsigned int maxElements;
		
		// Pointer to an element in the queue: contains the true pointer
		// and the position of the element within the page that it is
		// stored.
		struct EPtr {
			Element       *element;
			unsigned int   posInPage;
		};
		
		// The position where the next element is enqueued.
		EPtr nextEnqueue;
		
		// The position pointing to the next element that will be
		// dequeued. 
		EPtr nextDequeue;
		
		// The number of elements that we currently have on the queue.
		unsigned int numElements;
		
		std::ostream& LOG;
		
	public:
		
		SimpleQueue (int id, std::ostream& LOG);
		virtual ~SimpleQueue();
		
		// Initialization routines.
		int setNumPages(unsigned int numPages);
		int setMemoryManager(MemoryManager *memMgr);
		int initialize();
		
		// Inherited from Queue.
		bool enqueue (Element element);
		bool dequeue (Element &element);
		bool peek (Element &element) const;
		bool isFull () const;
		bool isEmpty () const;
		
	private:	  		
		int computePageLayout();
		int initState();		
	};
}
#endif
