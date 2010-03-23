#ifndef _QUERY_DEBUG_
#define _QUERY_DEBUG_

/**
 * @file          query_debug.h
 * @date          May 19, 2004
 * @brief         debug (print) routines for Semantic query
 */

#include <iostream>

#ifndef _QUERY_
#include "querygen/query.h"
#endif

#ifndef _TABLE_MGR_
#include "metadata/table_mgr.h"
#endif

namespace Semantic {
	std::ostream& operator << (std::ostream& out, Attr);
	std::ostream& operator << (std::ostream& out, AggrFn fn);
	std::ostream& operator << (std::ostream& out, ArithOp op);
	std::ostream& operator << (std::ostream& out, CompOp op);
	std::ostream& operator << (std::ostream& out, Expr*);
	std::ostream& operator << (std::ostream& out, BExpr);
	std::ostream& operator << (std::ostream& out, WindowSpec);
	std::ostream& operator << (std::ostream& out, R2SOp);
	
	void printQuery(std::ostream&                 out, 
					const Query&                  query, 
					const Metadata::TableManager *tableMgr);
}

#endif

