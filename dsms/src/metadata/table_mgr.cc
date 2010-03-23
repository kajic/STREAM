/**
 * @file      tablemgr.cc
 * @date      May 20, 2004
 * @brief     Implementation of the table manager
 */

#include <string.h>
#include <stdlib.h>


#include "common/debug.h"

#ifndef _TABLE_MGR_
#include "metadata/table_mgr.h"
#endif

#ifndef _ERROR_
#include "interface/error.h"
#endif

#ifndef _SYS_STREAM_
#include "common/sys_stream.h"
#endif

using namespace Metadata;

TableManager::TableManager()
{
	numTables = 0;
	
#ifdef _SYS_STR_
	registerSysStr ();	
#endif
}

/**
 * Deallocate space allocated for table & attribute names.
 */
TableManager::~TableManager() 
{		
	for (unsigned int t = 0 ; t < numTables ; t++) {
		for (unsigned int a = 0 ; a < tableList [t].numAttrs ; a++) {
			
			ASSERT(tableList[t].attrList[a].attrName);
			
			free (tableList[t].attrList[a].attrName);
		}
		
		ASSERT(tableList [t].tableName);
		free (tableList [t].tableName);
	}
}

int TableManager::registerStream(const char     *streamName, 
                                 unsigned int&   streamId)

{
	ASSERT(streamName);
	
	if(numTables == MAX_TABLES)
		return -1; //ERR_FULL
	
	// Check for duplicate table name
	for(unsigned int i = 0 ; i < numTables ; i++) 
		if(strcmp (tableList [i].tableName, streamName) == 0)
			return DUPLICATE_TABLE_ERR;
	
    // Internal identifier for the table is its position in the tableList:
	streamId = numTables;	
	
	tableList [numTables].tableName  = strdup(streamName);
	tableList [numTables].isStream   = true;
	tableList [numTables].numAttrs   = 0;	
	
	numTables++;
	
	return 0;
}

int TableManager::registerRelation(const char       *relName,
								   unsigned int&     relId)
{
	ASSERT(relName);
	
	// Too many tables?
	if(numTables == MAX_TABLES)
		return -1; 
	
	// check for duplicate table names
	for(unsigned int i = 0; i < numTables ; i++)
		if(strcmp (tableList[i].tableName, relName) == 0)
			return DUPLICATE_TABLE_ERR; 
	
    // Internal identifier for the table is its position in the tableList:    
	relId = numTables;

	tableList [numTables].tableName  = strdup(relName);
	tableList [numTables].isStream   = false;
	tableList [numTables].numAttrs   = 0;	
	
	numTables++;
	
	return 0;
}

int TableManager::unregisterTable (unsigned int tableId)
{
	// It has to be the last registered table
	ASSERT (tableId == numTables - 1);	
	numTables --;
	return 0;
}

int TableManager::addTableAttribute(unsigned int      tableId, 
                                    const char       *attrName,
                                    Type              attrType,
                                    unsigned int      attrLen)
{
	ASSERT(attrName);
	ASSERT(tableId < numTables);
	
	Table& table = tableList [tableId];
	
	// Too many attributes?
	if(table.numAttrs == MAX_ATTRS) 
		return -1;
	
	// check for duplicates
	for (unsigned int i = 0 ; i < table.numAttrs ; i++)
		if(strcmp (table.attrList [i].attrName, attrName) == 0)
			return DUPLICATE_ATTR_ERR;		
	
	table.attrList [ table.numAttrs ].attrName = strdup(attrName);
	table.attrList [ table.numAttrs ].attrType = attrType;
	table.attrList [ table.numAttrs ].attrLen  = attrLen;
	
	table.numAttrs++;
	
	return 0;
}

//----------------------------------------------------------------------
//
// Lookup methods:
//
//----------------------------------------------------------------------


bool TableManager::getTableId (const char      *tableName,
							   unsigned int&    tableId) const
{
	ASSERT(tableName);
	
	for(tableId = 0 ; tableId < numTables ; tableId++) 
		if (strcmp (tableList[tableId].tableName, tableName) == 0) 
			return true;
	
	return false;	
}

unsigned int TableManager::getNumAttrs (unsigned int tableId) const
{
	ASSERT(tableId < numTables);
	
	return tableList [ tableId ].numAttrs;
}

bool TableManager::getAttrId (unsigned int     tableId, 
							  const char      *attrName,
                              unsigned int&    attrId) const
{
	ASSERT(tableId < numTables);
	ASSERT(attrName);
	
	const Table& table = tableList [ tableId ];
	
	for(attrId = 0 ; attrId < table.numAttrs ; attrId++)
		if(strcmp (table.attrList [attrId].attrName, attrName) == 0)
			return true;
	
	return false;
}


Type TableManager::getAttrType(unsigned int  tableId,
							   unsigned int  attrId) const
{
	ASSERT(tableId < numTables);
	ASSERT(attrId  < tableList [ tableId ].numAttrs);
	
	return tableList [ tableId ].attrList [ attrId ].attrType;
}


unsigned int TableManager::getAttrLen(unsigned int tableId,
                                      unsigned int attrId) const
{
	ASSERT(tableId < numTables);
	ASSERT(attrId < tableList [ tableId ].numAttrs);
	
	return tableList [ tableId ].attrList [ attrId ].attrLen;
}

bool TableManager::isStream(unsigned int tableId) const
{
	ASSERT(tableId < numTables);
	
	return tableList [ tableId ].isStream;
}

const char *TableManager::getTableName (unsigned int tableId) const
{
	ASSERT (tableId < numTables);
	
	return tableList [tableId].tableName;
}

#ifdef _SYS_STR_

void TableManager::registerSysStr ()
{
	int rc;
	unsigned int id;
	
	rc = registerStream (SYS_STREAM, id);
	ASSERT (rc == 0);
	ASSERT (id == SS_ID);
	
	for (unsigned int a = 0 ; a < SS_NUM_ATTRS ; a++) {
		rc = addTableAttribute (SS_ID,
								SS_ATTRS [a],
								SS_ATTR_TYPES [a],
								SS_ATTR_LEN [a]);
		ASSERT (rc == 0);
	}
}

#endif

#if _TEST_
/// deub
static ostream& operator << (ostream& out, Type type)
{
	switch(type) {
	case INT:
		out << "Integer";
		break;
	case FLOAT:
		out << "Float";
		break;
	case BYTE:
		out << "Byte";
		break;
	case CHAR:
		out << "Char";
		break;
	default:
		out << "Unknown";
		break;
	}
	return out;
}


/// debug
void TableManager::printState() const 
{
	
	for (unsigned int t = 0 ; t < numTables ; t++) {
		if (tableList [t].isStream) 
			cout << "Stream: ";
		else
			cout << "Relation: ";
		
		const Table& table = tableList [t];
		
		cout << table.tableName;
		cout << "(";
		
		for (unsigned int a = 0 ; a < table.numAttrs ; a++) { 
			cout << table.attrList [a].attrName << " "
				 << table.attrList [a].attrType;
			
			if(table.attrList [a].attrType == CHAR)
				cout << "(" << table.attrList [a].attrLen << ")";
			
			if(a < table.numAttrs - 1)
				cout << ", ";
		}
		
		cout << ")" << endl;
	}
	
	return;
}
#endif
		
			

			
		
