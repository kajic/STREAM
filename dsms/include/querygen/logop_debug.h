#ifndef _LOGOP_DEBUG_
#define _LOGOP_DEBUG_

#include <iostream>

#ifndef _LOGOP_
#include "querygen/logop.h"
#endif

namespace Logical {
	std::ostream& operator << (std::ostream& out, Operator *op);
	std::ostream& operator << (std::ostream& out, BExpr bexpr);
	std::ostream& operator << (std::ostream& out, Attr attr);
	std::ostream& operator << (std::ostream& out, Expr *expr);
	
	bool check_operator (const Operator *op);
	bool check_operator_recursive (const Operator *op);	
	bool check_plan (const Operator *op);
}

#endif
