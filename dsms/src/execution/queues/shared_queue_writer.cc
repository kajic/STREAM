#ifndef _SHARED_QUEUE_WRITER_
#include "execution/queues/shared_queue_writer.h"
#endif

#ifndef _DEBUG_
#include "common/debug.h"
#endif

using namespace Execution;
using namespace std;

SharedQueueWriter::SharedQueueWriter (unsigned int id, ostream &_LOG)
	: LOG (_LOG)
{
	this -> id = id;
	this -> memMgr = 0;
}

SharedQueueWriter::~SharedQueueWriter () {}

int SharedQueueWriter::setNumPages (unsigned int numPages)
{
	this -> numPages = numPages;
	return 0;
}

int SharedQueueWriter::setMemoryManager (MemoryManager *memMgr)
{
	ASSERT (memMgr);

	this -> memMgr = memMgr;
	return 0;
}

int SharedQueueWriter::setNumReaders (unsigned int numReaders)
{
	this -> numReaders = numReaders;
	return 0;
}

int SharedQueueWriter::setStore (StorageAlloc *store)
{
	ASSERT (store);

	this -> store = store;
	return 0;
}

int SharedQueueWriter::computePageLayout()
{
	unsigned int lastElemOffset;
	
	pageSize = memMgr -> getPageSize();
	
	// next pointer offset: we place this as much to the end of the page
	// as possible.  We want to ensure that we meet the alignment
	// requirement of (char *)
	ASSERT (pageSize > sizeof (char *));
	nextPtrOffset = pageSize - sizeof (char *);
	nextPtrOffset -= (nextPtrOffset % __alignof__ (char *));
	
	// Number of elements we can store per page
	numElementsPerPage = nextPtrOffset / sizeof (Element);
	ASSERT (numElementsPerPage > 0);
	
	// Beginning of the last element
	lastElemOffset = sizeof(Element) * (numElementsPerPage - 1);
	
	// Offset of next pointer from the beginning of the last element
	ASSERT (nextPtrOffset > lastElemOffset);
	nextPtrOffset_le = nextPtrOffset - lastElemOffset;
	
	return 0;
}

#define NEXT(p)   (*((char **)((p) + nextPtrOffset)))

int SharedQueueWriter::initState()
{
	int rc;
	char *firstPage;
	char *page;

	// Allocate the first page
	if ((rc = memMgr -> allocatePage (firstPage)) != 0)
		return rc;
	
	// Allocate the rest of the pages
	page = firstPage;
	for (unsigned int p = 1 ; p < numPages ; p++) {
		
		if ((rc = memMgr -> allocatePage (NEXT(page))) != 0)
			return rc;
		
		page = NEXT (page);
	}
	
	// We want a circular buffer - so we point the last page to the first
	// page 
	NEXT (page) = firstPage;
	
	// The enqueue position is the first position on the first page
	nextEnqueue.element   = (Element*) firstPage;
	nextEnqueue.posInPage = 0;

	// The next dequeue position is the same
	for (unsigned int r = 0 ; r < numReaders ; r++) 
		nextDequeue [r] = nextEnqueue;

	totalEnqueued = 0;
	for (unsigned int r = 0 ; r < numReaders ; r++)
		totalDequeued [r] = 0;

	// arbitrarily break the tie
	slowestReader = 0;
	
	return 0;
}

int SharedQueueWriter::initialize ()
{
	int rc;
	
	// Determine how to layout elements on a page
	if ((rc = computePageLayout()) != 0)
		return rc;
	
	// initialize my active state
	if ((rc = initState ()) != 0)
		return rc;
	
	// maximum number of elements I can process
	maxElements = numElementsPerPage * numPages;

	return 0;
}

#define NEXT_LE(p)  (*((char **)( ((char *)(p)) + nextPtrOffset_le) ))
#define LAST_ELEMENT(p) ((p).posInPage == numElementsPerPage - 1)
#define LINCR(p)    ((p).element++,(p).posInPage++)
#define GINCR(p)    ((p).element = (Element*)NEXT_LE((p).element),(p).posInPage=0)
#define INCR(p)     ((LAST_ELEMENT(p))? GINCR(p) : LINCR (p))

bool SharedQueueWriter::enqueue (Element element)
{
	unsigned int numActiveElements;
	
	numActiveElements = totalEnqueued - totalDequeued [slowestReader];	
	
	ASSERT (numActiveElements <= maxElements);	
	
	// no space
	if (numActiveElements == maxElements)
		return false;
	
	// enqueue
	*nextEnqueue.element = element;
	
	// next enqueue position
	INCR (nextEnqueue);
	
	totalEnqueued ++;
	
	// [[ Explanation ]]
	if (element.kind != E_HEARTBEAT)
		store -> addRef (element.tuple, numReaders - 1);
	
#ifdef _MONITOR_
	logTs (element.timestamp);
	if (element.kind != E_HEARTBEAT)
		logElem ();
#endif
	
	return true;
}

bool SharedQueueWriter::dequeue (Element &element)
{
	ASSERT (0);
	return -1;
}

bool SharedQueueWriter::peek (Element &element) const
{
	ASSERT (0);
	return -1;
}

bool SharedQueueWriter::isFull() const
{
	ASSERT (totalEnqueued - totalDequeued [slowestReader] <= maxElements);	
	return (totalEnqueued - totalDequeued [slowestReader] == maxElements);
}

bool SharedQueueWriter::isEmpty() const
{
	ASSERT (0);
	return -1;
}

bool SharedQueueWriter::dequeue (Element &element, unsigned int reader)
{
	unsigned int totalDequeued_r;
	
	ASSERT (reader < numReaders);
	ASSERT (totalEnqueued >= totalDequeued [reader]);
	
	if (totalEnqueued == totalDequeued [reader])
		return false;
	
	element = *(nextDequeue[reader].element);
	INCR (nextDequeue[reader]);
	
	if (reader == slowestReader) {
		totalDequeued_r = totalDequeued [reader];
		
		for (unsigned int r = 0 ; r < numReaders ; r++) {
			ASSERT (totalDequeued_r <= totalDequeued [r]);
			
			totalDequeued [r] -= totalDequeued_r;			
			if ((totalDequeued [r] == 0) && (r != reader)) {				
				slowestReader = r;
			}						
		}
		totalEnqueued -= totalDequeued_r;
		
		totalDequeued [reader] = 1;
	}
	
	else {
		totalDequeued [reader] ++;
	}
	
	return true;
}

bool SharedQueueWriter::peek (Element &element, unsigned int reader)
	const
{
	ASSERT (reader < numReaders);
	ASSERT (totalEnqueued >= totalDequeued [reader]);
	
	if (totalEnqueued == totalDequeued [reader])
		return false;
	
	element = *(nextDequeue[reader].element);
	
	return true;
}

bool SharedQueueWriter::isFull (unsigned int reader) const
{
	ASSERT (reader < numReaders);
	ASSERT (totalEnqueued >= totalDequeued [reader]);

	return (totalEnqueued - totalDequeued [reader] == maxElements);
}
	
bool SharedQueueWriter::isEmpty (unsigned int reader) const
{
	ASSERT (reader < numReaders);
	ASSERT (totalEnqueued >= totalDequeued [reader]);
	
	return (totalEnqueued == totalDequeued [reader]);
}
