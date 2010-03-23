#ifndef _PHY_OP_DEBUG_
#define _PHY_OP_DEBUG_

#ifndef _CPP_OSTREAM_
#include <ostream>
#endif

#ifndef _PHY_OP_
#include "metadata/phy_op.h"
#endif

namespace Physical {
	std::ostream &operator << (std::ostream &out, Operator *op);
	std::ostream &operator << (std::ostream &out, BExpr *bexpr);
	std::ostream &operator << (std::ostream &out, Expr *expr);
	std::ostream &operator << (std::ostream &out, Attr attr);

	std::ostream &operator << (std::ostream &out, Synopsis *syn);
	std::ostream &operator << (std::ostream &out, Store *store);
}

#endif
