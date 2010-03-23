#ifndef _MEMORY_MGR_
#define _MEMORY_MGR_

#include <ostream>

#ifndef _DEBUG_
#include "common/debug.h"
#endif

namespace Execution {
	
	class MemoryManager {
	private:

		/// System-wide unique identifier
		unsigned int id;
		
		/// Memory being managed
		char *memory;
		
		/// Size of each page of memory
		static const unsigned int PAGE_SIZE     = 4096;
		static const unsigned int LOG_PAGE_SIZE = 12;	   
		static const unsigned int PAGE_MASK     = 0xfffff000;
		
		// Amount of memory
		unsigned int memorySize;
		
		/// Number of pages in memory
		unsigned int numPages;
		
		/// System-wide logger
		std::ostream  &LOG;
		
		/// Linked list of empty pages
		char *emptyPageList;
		
	public:
		
		MemoryManager (unsigned int id, std::ostream &LOG);
		~MemoryManager();
		
		//------------------------------------------------------------
		// Initialization routines
		//------------------------------------------------------------
		
		int setMemorySize (unsigned int memorySize);		
		int initialize ();
		
		/**
		 * @return size of pages
		 */
		unsigned int getPageSize () const { 
			return PAGE_SIZE;
		}
		
		/**
		 * Allocate a new page & return.
		 */
		int allocatePage (char *&page);
		
		/**
		 * Convert a pointer into our memory into a unique integer
		 * identifier.  Pointers to different locations get distinct
		 * identifiers. This is used within LinStore.
		 *
		 * @param ptr pointer to the memory location
		 * @return unique id for the memory location
		 */ 
		unsigned int getId (char *ptr) {
			ASSERT (ptr);
			ASSERT (ptr >= memory);
			ASSERT (ptr - memory < (int)memorySize);
			
			return (ptr - memory);
		}
		
		char *getPage (char *ptr) {
			ASSERT (ptr);
			ASSERT (ptr >= memory);
			ASSERT (ptr - memory < (int)memorySize);
			
			return (memory + ((ptr - memory) & PAGE_MASK));
		}

		/**
		 * Deallocate a page
		 */
		int deallocatePage (char *page);
		
	};
}


#endif
