
#ifndef _QUERY_MGR_
#include "metadata/query_mgr.h"
#endif

#ifndef _DEBUG_
#include "common/debug.h"
#endif

using namespace Metadata;
using namespace std;

QueryManager::QueryManager (ostream &_LOG)
	: LOG (_LOG)
{
	this -> numQueries = 0;
	this -> next = queryBuffer;
	
	for (unsigned int q = 0 ; q < MAX_QUERIES ; q++)
		queries [q] = 0;
}

QueryManager::~QueryManager() {}

int QueryManager::registerQuery (const char *qryStr,
								 unsigned int qryStrLen,
								 unsigned int &qryId)
{
	// Sanity checks
	ASSERT (qryStr);
	ASSERT (qryStrLen > 0);
	if (strlen (qryStr) != qryStrLen) {
		LOG << "Query: " << qryStr << endl;
		LOG << "strlen(qryStr) = " << strlen(qryStr) << endl;
		LOG << "qryStrLen = " << qryStrLen << endl;
		ASSERT (0);
	}
	ASSERT (next - queryBuffer <= (int)QRY_BUF_LEN);
	
	// We don't have space?
	if (next - queryBuffer + qryStrLen >= QRY_BUF_LEN) {
		LOG << "QueryManager out of space" << endl;
		return -1;
	}
	
	// Exceeded query number limit
	if (numQueries >= MAX_QUERIES) {
		LOG << "QueryManager: maximum query limit exceeded" << endl;
		return -1;
	}
	
	// queries[queryId] retrieves the string for the query
	qryId = numQueries ++;
	queries [qryId] = next;
	
	strncpy (next, qryStr, qryStrLen + 1);
	next += (qryStrLen + 1);
	
	return 0;
}

