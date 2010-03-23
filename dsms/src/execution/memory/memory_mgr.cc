/**
 * @file        memory_mgr.cc
 * @date        May 29, 2004
 * @brief       System wide memory manager
 */

#include <unistd.h>

#ifndef _MEMORY_MGR_
#include "execution/memory/memory_mgr.h"
#endif

using namespace Execution;
using namespace std;

#define NEXT(page) (*((char **)(page)))
#define PAGENUM(page) ((page - memory) >> LOG_PAGE_SIZE)

MemoryManager::MemoryManager (unsigned int _id, ostream& _LOG)
	: LOG (_LOG)
{
	this -> id = _id;
	this -> memory = 0;
	this -> memorySize = 0;
	this -> numPages = 0;
	this -> emptyPageList = 0;
}

MemoryManager::~MemoryManager () {
	if (memory) {
		free (memory);
	}
}

int MemoryManager::setMemorySize (unsigned int memorySize)
{
	ASSERT (memorySize > 0);
	
	this -> memorySize = memorySize;
	return 0;
}

/**
 * Allocate memory, set page size, compute number pages, and initialize
 * the array used to maintain reference counts.  We set the page size to
 * the system define PAGESIZE - there seems to be no particular rationale
 * for this.
 */
int MemoryManager::initialize ()
{	
	// conservatively round memorySize to be multiples of PAGE_SIZE, which
	// simplifies the management for us.
	numPages = (memorySize + PAGE_SIZE) / PAGE_SIZE;
	memorySize = numPages * PAGE_SIZE;
	memory = (char *)malloc (numPages * PAGE_SIZE);
	
	if (!memory) {
		LOG << "Memory Manager: unable to allocate memory" << endl;
		
		numPages  = 0;
		return -1;
	}
	
	// Add all pages to the empty pages linked list
	char *page = memory;
	for (unsigned int p = 0 ; p < numPages - 1 ; p++, page += PAGE_SIZE)	
		NEXT(page) = page + PAGE_SIZE;	
	NEXT (page) = 0;
	
	LOG << "Memory Manager: started with " << numPages 
		<< " pages of size " << PAGE_SIZE << endl;
	
	emptyPageList = memory;

	return 0;
}


/**
 * Allocate a new page. 
 *
 * The new page is picked from the beginning of empty pages linked list. 
 */ 
int MemoryManager::allocatePage (char *&page)
{	
	// We do not have any more pages.
	if (emptyPageList == 0) {
		LOG << "Memory Manager: out of memory" << endl;		
		return -1;
	}
	
	// first page in the empty pages list
	page = emptyPageList;
		
	// Update the empty pages list.
	emptyPageList = NEXT(emptyPageList);
	
#ifdef _DM_
	//LOG << "Memory Manager: Allocated page " << PAGENUM(page) << endl;
#endif
	
	return 0;
}

/**
 * Deallocate a page
 * 
 * The deallocated page is inserted at the beginning of the empty pages
 * list
 */ 
int MemoryManager::deallocatePage (char *page)
{	
	NEXT(page) = emptyPageList;
	emptyPageList = page;

#ifdef _DM_
	//LOG << "Memory Manager: Deallocated page " << PAGENUM(page) << endl;
#endif
	
	return 0;
}
