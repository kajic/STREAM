/**
 * @file        simple_queue.cc
 * @date        June 3, 2004
 * @brief       Implementation of a simple queue
 */

#ifndef _SIMPLE_QUEUE_
#include "execution/queues/simple_queue.h"
#endif

#ifndef _DEBUG_
#include "common/debug.h"
#endif

using namespace std;
using namespace Execution;

SimpleQueue::SimpleQueue(int _id, ostream& _LOG)
	: LOG (_LOG)
{
	this -> id          = _id;
	this -> memMgr      = 0;
	this -> numPages    = 0;
	this -> maxElements = 0;
}

SimpleQueue::~SimpleQueue() {}

int SimpleQueue::setNumPages(unsigned int numPages)
{
	ASSERT (numPages > 0);
	
	this -> numPages = numPages;
	return 0;
}

int SimpleQueue::setMemoryManager(MemoryManager *memMgr)
{
	ASSERT (memMgr);

	this -> memMgr   = memMgr;
	this -> pageSize = memMgr -> getPageSize();

	return 0;
}

int SimpleQueue::computePageLayout()
{
	unsigned int lastElemOffset;
	
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

int SimpleQueue::initState()
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
	
	// We initialize the two element pointers to the first position in the
	// first page.
	nextEnqueue.element   = (Element*) firstPage;
	nextEnqueue.posInPage = 0;
	
	nextDequeue = nextEnqueue;
	
	return 0;
}

int SimpleQueue::initialize()
{
	int rc;
	
	// Determine how to layout elements on a page
	if ((rc = computePageLayout()) != 0)
		return rc;

	// Initialize my "active" state - the memory for the queue, and the
	// pointers.
	if ((rc = initState()) != 0)
		return rc;
	
	// Maximum number of elements that I can process:
	maxElements = numElementsPerPage * numPages;
	numElements = 0;
	
	return 0;
}

#define NEXT_LE(p)  (*((char **)( ((char *)(p)) + nextPtrOffset_le) ))
#define LAST_ELEMENT(p) ((p).posInPage == numElementsPerPage - 1)
#define LINCR(p)    ((p).element++,(p).posInPage++)
#define GINCR(p)    ((p).element = (Element*)NEXT_LE((p).element),(p).posInPage=0)
#define INCR(p)     ((LAST_ELEMENT(p))? GINCR(p) : LINCR (p))

bool SimpleQueue::enqueue (Element element)
{
	
	ASSERT (numElements <= maxElements);
	
	if (numElements == maxElements)
		return false;
	
	*nextEnqueue.element = element;
	
	INCR (nextEnqueue);
	numElements++;

#ifdef _MONITOR_
	logTs (element.timestamp);
	if (element.kind != E_HEARTBEAT)
		logElem ();
#endif

	return true;
}

bool SimpleQueue::dequeue (Element& element)
{
	ASSERT (numElements <= maxElements);
	
	if (numElements == 0)
		return false;
	
	element = *nextDequeue.element;
	
	INCR (nextDequeue);
	numElements --;
	
	return true;
}

bool SimpleQueue::peek (Element& element) const
{
	ASSERT (numElements <= maxElements);
	
	if (numElements == 0)
		return false;
	
	element = *nextDequeue.element;
	return true;
}

bool SimpleQueue::isFull() const
{
	ASSERT (numElements <= maxElements);
	
	return (numElements == maxElements);
}

bool SimpleQueue::isEmpty() const
{
	ASSERT (numElements <= maxElements);
	
	return (numElements == 0);
}
