#ifndef _QUERY_MGR_
#define _QUERY_MGR_

/**
 * @file        query_mgr.h
 * @date        Aug. 21, 2004
 * @brief       Query manager interface
 */

#include <ostream>

#ifndef _CONSTANTS_
#include "common/constants.h"
#endif

namespace Metadata {

	/**
	 * Query manager stores all the registered queries and subqueries in
	 * the system.  It also assigns a unique identifier to each query
	 * which is used by the rest of the system.
	 *
	 * Storing the query strings does not serve any useful purpose at
	 * present - but seems in general to be a useful thing to do :)
	 */
	
	class QueryManager {
	private:
		std::ostream &LOG;
		
		/// Space allocated for storing queries
		static const unsigned int QRY_BUF_LEN = 8192;
		
		/// Buffer for storing queries
		char queryBuffer [QRY_BUF_LEN];
		
		/// pointers to query strings within queryBuffer
		char *queries [MAX_QUERIES];
		
		/// number of registered queries
		unsigned int numQueries;

		/// Pointer to the first available space in queryBuffer where we
		/// start writing the next query string
		char *next;
		
	public:
		QueryManager (std::ostream& LOG);
		~QueryManager();
		
		/**
		 * register the query indicated by queryString and return an id
		 * for the query.
		 */
		
		int registerQuery (const char *queryStr, unsigned int queryStrLen,
						   unsigned int &queryId);
	};
}

#endif
