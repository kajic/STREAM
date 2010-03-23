#ifndef _PLAN_MGR_IMPL_
#include "metadata/plan_mgr_impl.h"
#endif

using namespace Metadata;
using namespace Physical;

static TableManager *_tableMgr;

namespace Metadata {
	class BufOut {
		
		char *buf;
		int   bufLen;
		char *curPtr;
		char *last;

	public:
		BufOut (char *buf, int bufLen) {
			this -> buf = buf;
			this -> bufLen = bufLen;
			this -> curPtr = buf;
			this -> last = buf + bufLen;
		}
		
		BufOut& operator << (int i) {
			// buffer full:
			if (curPtr >= last - 1)
				return (*this);
			
			int nused = snprintf (curPtr, last - curPtr, "%d", i);
			ASSERT (nused < last - curPtr);
			
			curPtr += nused;
			
			return (*this);
		}

		BufOut& printFloat (float f) {
			// buffer full:
			if (curPtr >= last - 1)
				return (*this);
			
			int nused = snprintf (curPtr, last - curPtr, "%f", f);
			ASSERT (nused < last - curPtr);
			
			curPtr += nused;
			
			return (*this);
		}
		
		BufOut& operator << (const char *str) {
			// buffer full:
			if (curPtr >= last - 1)
				return (*this);
			
			int nused = snprintf (curPtr, last - curPtr, "%s", str);
			ASSERT (nused < last - curPtr);
			
			curPtr += nused;
			return (*this);
		}
		
		bool good () {
			return (curPtr < last - 1);
		}
	};
}

static int opKindToInt (OperatorKind kind);
static BufOut& operator << (BufOut& bout, Operator *op); 
static BufOut& operator << (BufOut& bout, Store *store);
static BufOut& operator << (BufOut& bout, Synopsis *syn);

static BufOut& operator << (BufOut& bout, Attr attr);
static BufOut& operator << (BufOut& bout, Expr *expr);
static BufOut& operator << (BufOut& bout, BExpr *pred);
static BufOut& operator << (BufOut& bout, AggrFn fn);

static const char *getSynPos (Synopsis *relSyn);
static bool ignoreSyn (Synopsis *syn);
static bool ignoreStore (Store *store);
		
int PlanManagerImpl::getXMLPlan (char *planBuf, unsigned int planBufLen)
{
	BufOut bout (planBuf, planBufLen);
	Operator *op;

	_tableMgr = tableMgr;
		
	// <plan>
	bout << "<plan>\n\n";
	
	// Operators
	op = usedOps;
	for (op = usedOps ; op ; op = op -> next) 
		bout << op << "\n";
	
	// Stores
	for (unsigned int s = 0 ; s < numStores ; s++) 
		bout << (stores + s) << "\n";
	
	// Synopses
	for (unsigned int s = 0 ; s < numSyns ; s++) 
		bout << (syns + s) << "\n";
	
	bout << "</plan>\n";
	
	if (!bout.good ()) {
		LOG << "Plan buffer insufficient" << std::endl;
		return -1;
	}
	
	return 0;
}

/**
 * Map operator kind to an integer between 0 .. 16 to help us access
 * information about operators in a more systematic way
 */ 
static int opKindToInt (OperatorKind kind)
{
	switch (kind) {
	case PO_SELECT:
		return 0;
		
	case PO_PROJECT:
		return 1;
		
	case PO_JOIN:
		return 2;
		
	case PO_STR_JOIN:
		return 3;
		
	case PO_JOIN_PROJECT:
		return 2;

	case PO_STR_JOIN_PROJECT:
		return 3;
		
	case PO_GROUP_AGGR:
		return 4;
		
	case PO_DISTINCT:
		return 5;
		
	case PO_ROW_WIN:
		return 6;
		
	case PO_RANGE_WIN:
		return 7;
				
	case PO_PARTN_WIN:
		return 8;
		
	case PO_ISTREAM:
		return 9;
		
	case PO_DSTREAM:
		return 10;
		
	case PO_RSTREAM:
		return 11;
		
	case PO_UNION:
		return 12;
		
	case PO_EXCEPT:
		return 13;
		
	case PO_STREAM_SOURCE:
		return 14;
		
	case PO_RELN_SOURCE:
		return 15;
		
	case PO_OUTPUT:
		return 16;

	case PO_SINK:
		return 17;

	case PO_SS_GEN:
		return 18;
		
	default:
		ASSERT (0);
		break;
	}
	return 0;
}


/**
 * Strings for operator names, used within plan encoding
 */ 
static const char *opNamesShort[] = {
	"Select",            // 0
	"Project",           // 1
	"Join",              // 2
	"StrJoin",           // 3
	"Aggr",              // 4
	"DupElim",           // 5
	"RowWin",            // 6
	"TimeWin",           // 7
	"PartWin",           // 8
	"Istream",           // 9
	"Dstream",           // 10
	"Rstream",           // 11
	"Union",             // 12
	"Except",            // 13
	"StrSrc",            // 14
	"RelSrc",            // 15
	"Output",            // 16
	"Sink",              // 17
	"SSGen"              // 18
};

/**
 * Long names of the operators
 */
static const char *opNamesLong[] = {
	"Selection",                // 0
	"Projection",               // 1
	"Binary Join",              // 2
	"Stream Relation Join",     // 3
	"Group By Aggregation",     // 4
	"Duplicate Elimination",    // 5
	"Row Window",               // 6
	"Time Based Window",        // 7
	"Partition Window",         // 8
	"Istream",                  // 9
	"Dstream",                  // 10
	"Rstream",                  // 11
	"Union",                    // 12
	"Except",                   // 13
	"Stream Source",            // 14
	"Relation Source",          // 15
	"Output",                   // 16
	"Sink",                     // 17
	"System Stream Generator"   // 18
};

static BufOut& operator << (BufOut& bout, AggrFn fn)
{
	switch (fn) {
	case COUNT:
		bout << "Count";
		break;
		
	case SUM:
		bout << "Sum";
		break;
		
	case AVG:
		bout << "Avg";
		break;

	case MAX:
		bout << "Max";
		break;

	case MIN:
		bout << "Min";
		break;

	default:
		break;
	}
	
	return bout;
}
static BufOut& operator << (BufOut& bout, Operator *op)
{
	int typeId;
	
	ASSERT (op);
	
	bout << "<operator id = \"" << op -> id << "\"";

	if (op -> bStream) {
		bout << " stream = \"1\"";
	}
	else {
		bout << " stream = \"0\""; 
	}

	bout << ">\n";
	
	typeId = opKindToInt(op -> kind);
	bout << "<name> " << opNamesShort[typeId] << " </name>\n";
	bout << "<lname> " << opNamesLong[typeId] << " </lname>\n";
	
	for (unsigned int i = 0 ; i < op -> numInputs ; i++) {
		if (op -> inQueues [i] -> kind == READER_Q) {
			bout << "<input queue = \""
				 << op -> inQueues [i] -> u.READER.writer -> id
				 << "\"> ";			
		}
		else {
			bout << "<input queue = \""
				 << op -> inQueues [i] -> id
				 << "\"> ";
		}
		
		bout << op -> inputs [i] -> id
			 << " </input>\n";
	}
	
	switch (op -> kind) {
	case PO_SELECT:
		ASSERT (op -> u.SELECT.pred);
		bout << "<property name = \"Predicate\" value = \""
			 << op -> u.SELECT.pred
			 << "\"/>\n";
		
		break;
		
	case PO_PROJECT:
		bout << "<property name = \"Project List\" value = \"";
		for (unsigned int p = 0 ; p < op -> numAttrs ; p++) {			
			bout << op -> u.PROJECT.projs [p];
			
			if (p < op -> numAttrs - 1)
				bout << ", ";
		}
		bout << "\"/>\n";

		break;
		
	case PO_JOIN:
		bout << "<property name = \"Join Predicate\" value = \""
			 << op -> u.JOIN.pred
			 << "\"/>\n";		
		
		break;
		
	case PO_STR_JOIN:
		bout << "<property name = \"Join Predicate\" value = \""
			 << op -> u.STR_JOIN.pred
			 << "\"/>\n";		
		
		break;
		
	case PO_JOIN_PROJECT:
		bout << "<property name = \"Join Predicate\" value = \""
			 << op -> u.JOIN_PROJECT.pred
			 << "\"/>\n";		

		bout << "<property name = \"Project List\" value = \"";
		for (unsigned int p = 0 ; p < op -> numAttrs ; p++) {			
			bout << op -> u.JOIN_PROJECT.projs [p];
			
			if (p < op -> numAttrs - 1)
				bout << ", ";
		}
		bout << "\"/>\n";
		
		break;

	case PO_STR_JOIN_PROJECT:
		bout << "<property name = \"Join Predicate\" value = \""
			 << op -> u.STR_JOIN_PROJECT.pred
			 << "\"/>\n";		
		
		bout << "<property name = \"Project List\" value = \"";
		for (unsigned int p = 0 ; p < op -> numAttrs ; p++) {			
			bout << op -> u.STR_JOIN_PROJECT.projs [p];
			
			if (p < op -> numAttrs - 1)
				bout << ", ";
		}
		bout << "\"/>\n";
		
		break;
		
	case PO_GROUP_AGGR:
		bout << "<property name = \"Grouping Attrs\" value = \"";
		for (unsigned int a = 0 ; a < op -> u.GROUP_AGGR.numGroupAttrs ; a++) {
			bout << op -> u.GROUP_AGGR.groupAttrs [a];
			if (a < op -> u.GROUP_AGGR.numGroupAttrs - 1)
				bout << ", ";
		}
		bout << "\"/>\n";

		bout << "<property name = \"Aggrs\" value = \"";
		for (unsigned int a = 0 ; a < op -> u.GROUP_AGGR.numAggrAttrs ; a++) {
			
			bout << op -> u.GROUP_AGGR.fn [a]
				 << "("
				 << op -> u.GROUP_AGGR.aggrAttrs [a]
				 << ")";
			if (a < op -> u.GROUP_AGGR.numAggrAttrs - 1)
				bout << ", ";
		}
		bout << "\"/>\n";
		
		break;
		
	case PO_DISTINCT:
		break;
		
	case PO_ROW_WIN:
		bout << "<property name = \"Num Rows\" value = \""
			 << op -> u.ROW_WIN.numRows
			 << "\"/>\n";
		
		break;
		
	case PO_RANGE_WIN:
		bout << "<property name = \"Range\" value = \""
			 << op -> u.RANGE_WIN.timeUnits
             << "\" stride = \""
             << op -> u.RANGE_WIN.strideUnits
			 << "\"/>\n";
		
		break;
				
	case PO_PARTN_WIN:
		bout << "<property name = \"Partn Attrs\" value = \"";
		for (unsigned int a = 0 ; a < op -> u.PARTN_WIN.numPartnAttrs ; a++) {
			bout << op -> u.PARTN_WIN.partnAttrs [a];
			if (a < op -> u.PARTN_WIN.numPartnAttrs - 1)
				bout << ", ";
		}
		bout << "\"/>\n";
		
		break;
		
	case PO_ISTREAM:
		break;
		
	case PO_DSTREAM:
		break;
		
	case PO_RSTREAM:
		break;
		
	case PO_UNION:
		break;
		
	case PO_EXCEPT:
		break;
		
	case PO_STREAM_SOURCE:
		bout << "<property name = \"Stream\" value = \""
			 << _tableMgr -> getTableName (op -> u.STREAM_SOURCE.strId)
			 << "\"/>\n";
		
		break;
		
	case PO_RELN_SOURCE:
		bout << "<property name = \"Relation\" value = \""
			 << _tableMgr -> getTableName (op -> u.RELN_SOURCE.relId)
			 << "\"/>\n";
		
		break;
		
	case PO_OUTPUT:		
		bout << "<query>" << op -> u.OUTPUT.queryId << "</query>\n";
		bout << "<property name = \"Query\" value = \""
			 << op -> u.OUTPUT.queryId
			 << "\"/>\n";
		
		break;
		
	case PO_SINK:
		break;

	case PO_SS_GEN:
		break;
		
	default:
		ASSERT (0);
		break;
	}
	
	bout << "</operator>\n";
	
	return bout;	
}

static BufOut& operator << (BufOut& bout, Attr attr)
{
	bout << "[" << attr.input << "," << attr.pos << "]";
	return bout;
}

static BufOut& operator << (BufOut& bout, ArithOp op) {
	switch (op) {
	case ADD:
		bout << " + ";
		break;
		
	case SUB:
		bout << " - ";
		break;
		
	case MUL:
		bout << " * ";
		break;
			
	case DIV:
		bout << " / ";
		break;
		
	default:
		ASSERT (0);
		break;
	}
	return bout;
}

static BufOut& operator << (BufOut& bout, Expr *expr)
{
	switch (expr -> kind) {
	case CONST_VAL:
		
		switch (expr -> type) {
		case INT:
			bout << expr -> u.ival;
			break;
		case FLOAT:
			bout.printFloat (expr -> u.fval);
			break;
		case CHAR:
			bout << expr -> u.sval;
			break;
		case BYTE:
			bout << expr -> u.bval;
			break;
		default:
			ASSERT (0);
			break;
		}
		break;
		
	case ATTR_REF:
		bout << expr -> u.attr;
		break;

	case COMP_EXPR:
		bout << expr -> u.COMP_EXPR.left
			 << expr -> u.COMP_EXPR.op
			 << expr -> u.COMP_EXPR.right;
		break;
		
	default:
		ASSERT (0);
		break;
	}

	return bout;
}

static BufOut& operator << (BufOut& bout, CompOp op)
{
	switch (op) {
	case LT:
		bout << " &lt; ";
		break;
		
	case LE:
		bout << " &lt;= ";
		break;
		
	case GT:
		bout << " &gt; ";
		break;
		
	case GE:
		bout << " &gt;= ";
		break;
		
	case EQ:
		bout << " = ";
		break;
		
	case NE:
		bout << " != ";
		break;
		
	default:
		ASSERT (0);
		break;
	}
	
	return bout;
}

static BufOut& operator << (BufOut& bout, BExpr *pred)
{
	if (!pred) {
		bout << "(null)";
		return bout;
	}
	
	bout << pred -> left << pred -> op << pred -> right;

	if (pred -> next)
		bout << ", " << pred -> next;
	return bout;
}

static BufOut& operator << (BufOut& bout, Store *store)
{
	// We don't want to print some "internal" stores since
	// these clutter up the screen space
	if (ignoreStore (store))
		return bout;	
	
	bout << "<store id = \"" << store -> id << "\">\n";
	bout << "<owner> " << store -> ownOp -> id << " </owner>\n";
	
	switch (store -> kind) {
	case SIMPLE_STORE:
		bout << "<name> Simple Store </name>\n";
		break;
		
	case REL_STORE:
		bout << "<name> Relation Store </name>\n";
		break;
		
	case WIN_STORE:
		bout << "<name> Window Store </name>\n";
		break;
		
	case LIN_STORE:
		bout << "<name> Lineage Store </name>\n";
		break;
		
	case PARTN_WIN_STORE:
		bout << "<name> Partition Window Store </name>\n";
		break;
		
	default:
		ASSERT (0);
		break;
	}
	bout << "</store>\n";
	
	return bout;
}

static BufOut& operator << (BufOut& bout, Synopsis *syn)
{
	if (ignoreSyn (syn))
		return bout;
	
	bout << "<synopsis id = \"" << syn -> id << "\">\n";
	bout << "<owner> " << syn -> ownOp -> id << " </owner>\n";
	bout << "<source> " << syn -> store -> id << " </source>\n";

	switch (syn -> kind) {
	case REL_SYN:
		bout << "<name> Relation Synopsis </name>\n";
		bout << "<pos> " << getSynPos (syn) << " </pos>\n";
		break;
		
	case WIN_SYN:
		bout << "<name> Window Synopsis </name>\n";
		bout << "<pos> center </pos>\n";
		break;
		
	case PARTN_WIN_SYN:
		bout << "<name> Partition Window Synopsis </name>\n";
		bout << "<pos> output </pos>\n";
		break;
		
	case LIN_SYN:
		bout << "<name> Lineage Synopsis </name>\n";
		bout << "<pos> output </pos>\n";
		break;
		
	default:
		ASSERT (0);
		break;
	}
	
	bout << "</synopsis>\n";
	
	return bout;
}

static const char *getSynPos (Synopsis *relSyn)
{
	Operator *op = relSyn -> ownOp;

	switch (op -> kind) {
	case PO_SELECT:
	case PO_PROJECT:
	case PO_ROW_WIN:
	case PO_RANGE_WIN:
	case PO_PARTN_WIN:
	case PO_ISTREAM:
	case PO_DSTREAM:
	case PO_UNION:
	case PO_EXCEPT:
	case PO_STREAM_SOURCE:
	case PO_OUTPUT:
	case PO_SINK:
	case PO_SS_GEN:
		ASSERT (0);
		break;
		
	case PO_JOIN:
		ASSERT (op -> u.JOIN.innerSyn == relSyn ||
				op -> u.JOIN.outerSyn == relSyn);
		if (op -> u.JOIN.innerSyn == relSyn)
			return "right";
		else
			return "left";
		
	case PO_STR_JOIN:
		ASSERT (op -> u.STR_JOIN.innerSyn == relSyn);
		return "right";
		
	case PO_JOIN_PROJECT:
		ASSERT (op -> u.JOIN_PROJECT.innerSyn == relSyn ||
				op -> u.JOIN_PROJECT.outerSyn == relSyn);
		if (op -> u.JOIN_PROJECT.innerSyn == relSyn)
			return "right";
		else
			return "left";
		
	case PO_STR_JOIN_PROJECT:
		ASSERT (op -> u.STR_JOIN_PROJECT.innerSyn == relSyn);
		return "right";		
		
	case PO_GROUP_AGGR:
		ASSERT (op -> u.GROUP_AGGR.inSyn == relSyn ||
				op -> u.GROUP_AGGR.outSyn == relSyn);
		if (op -> u.GROUP_AGGR.inSyn == relSyn)
			return "center";
		else
			return "output";
		
	case PO_DISTINCT:
		ASSERT (op -> u.DISTINCT.outSyn == relSyn);
		return "output";		
		
	case PO_RSTREAM:
		ASSERT (op -> u.RSTREAM.inSyn == relSyn);
		return "center";
		
	case PO_RELN_SOURCE:
		ASSERT (op -> u.RELN_SOURCE.outSyn == relSyn);
		return "output";		

	default:
		ASSERT (0);
		break;
	}
	
	return "center";
}

static bool ignoreSyn (Synopsis *syn)
{
	// Only relation synopses are ignored
	if (syn -> kind != REL_SYN)
		return false;
	
	Operator *owner = syn -> ownOp;
	
	if (owner -> kind == PO_ISTREAM) {
		ASSERT (owner -> u.ISTREAM.nowSyn == syn);
		return true;
	}

	if (owner -> kind == PO_DSTREAM) {
		ASSERT (owner -> u.DSTREAM.nowSyn == syn);
		return true;
	}
	
	if (owner -> kind == PO_EXCEPT && owner -> u.EXCEPT.countSyn == syn) {
		return true;
	}

	return false;
}

static bool ignoreStore (Store *store)
{
	return (store -> ownOp -> store != store &&
			store -> ownOp -> kind != PO_SS_GEN);
}

