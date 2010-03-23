#ifndef _SEM_INTERP_
#define _SEM_INTERP_

/**
 * @file       sem_interp.h
 * @date       May 24, 2004
 * @brief      Semantic Interpreter: converts syntactic parse trees to
 *             SemanticQuery objects.
 */

#ifndef _NODES_
#include "parser/nodes.h"
#endif

#ifndef _QUERY_
#include "querygen/query.h"
#endif

#ifndef _TABLE_MGR_
#include "metadata/table_mgr.h"
#endif

/**
 * Semantic Interpreter: 
 * 
 * In this file and sem_interp.cc file, the term "interpret" means
 * converting from "syntactic" structures to "semantic" structures.  For
 * example, a stream is identified by a name (string) externally, which is
 * a syntactic identification for the stream.  Internally, a unique ID is
 * used to identify the stream.  
 * 
 * Semantic Interpreter converts the syntactic parse tree into the
 * corresponding semantic Query object.  This represents the second layer
 * in converting CQL query into the actual execution units:
 * 
 * CQL query --> NODE * ---> Query ---> LogOp tree --> PhyOp tree --->
 * Execution units .
 * 
 */
class SemanticInterpreter {
 public:
	/**
	 * @param tableMgr    System-wide table manager that stores the named
	 *                    tables and their attributes.
	 */	
	SemanticInterpreter (Metadata::TableManager *tableMgr);
	
	/**
	 * Interpret a query: convert a parsed tree (NODE *) into a Query
	 * object. 
	 * 
	 * @param parseTree   Parse tree
	 * @param query       (output) query object
	 *
	 * @return            [[ Usual convention ]]
	 */
	int interpretQuery (NODE *parseTree, Semantic::Query &query);
	
 private:
	Metadata::TableManager     *_tableMgr;	
};

#endif
