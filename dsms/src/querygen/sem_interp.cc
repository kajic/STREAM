/**
 * @file            sem_interp.cc
 * @date            May 11, 2004
 * @brief           Routines that transform a query from CQL string to
 *                  functional units that compute the query
 */

/// debug
#include <iostream>
using namespace std;

#include <string.h>

#ifndef _SEM_INTERP_
#include "querygen/sem_interp.h"
#endif

#ifndef _CONSTANTS_
#include "common/constants.h"
#endif

#ifndef _DEBUG_
#include "common/debug.h"
#endif

#ifndef _ERROR_
#include "interface/error.h"
#endif

using namespace Semantic;
using namespace Parser;
using namespace Metadata;

/**
 *
 * The system-wide table manager, cached here during interpreting: set
 * before start of a new semantic interpretation.
 */
static TableManager *tableMgr;

/**
 * The symbol table.
 *
 * The  symbol  table  is   used  to  interpret  various  names  (symbols)
 * referenced in  the query.  For example,  using the symbol  table we can
 * convert named attributes in the  query to an internal representation of
 * the attribute <attrId, tableId>.
 *
 * Each instance of  a (stream/relation) that appears in  the query gets a
 * symbol table entry.   A symbol table entry contains  a variable-name (a
 * string) and  a table-id, which identifies the  stream/relation that the
 * variable  name  corresponds   to.   Recall  that  each  stream/relation
 * occurring   in  a  query   either  implicitly   or  explicitly   has  a
 * variable-name associated.
 *
 */

// maximum size of the symbol table
static const unsigned int MAX_SYMBOLS = MAX_TABLES;

// Each entry of a symbol table
struct SymbolTableEntry {
	const char   *varName;
	unsigned int  tableId;
};

// The symbol table
static SymbolTableEntry symbolTable[MAX_SYMBOLS];

// Number of entries currently in the symbol table
static unsigned int numSymbolTableEntries;

// Reset symbol table: called before start of a new interpretation.
static void resetSymbolTable()
{
	numSymbolTableEntries = 0;
}

/**
 * Space Mgmt. for expressions used within a Query: This is similar to
 * that for NODEs in nodes.cc
 */

// Maximum number of expressions that we can handle
static const unsigned int NUM_EXPRS = 1000;

// Space for expressions
static Expr exprs[NUM_EXPRS];

// Number of expressions currently allocated: 0 .. numExprs - 1
static unsigned int numExprs;

// Reclaim all space
static void resetExprs() 
{
	numExprs = 0;
	return;
}

// Allocate a new expression of a specified type
Expr *newExpr(ExprType type) 
{
	Expr *e;
	
	// No more space
	if(numExprs == NUM_EXPRS) {
		return 0;
	}  
	
	e = exprs + numExprs++;
	e -> exprType = type;
	
	return e;
}

/**
 * Add a new symbol table entry to the symbol table.  This involves the
 * following steps:
 * 
 * 1. check if the table is registered.
 * 2. If so, get its tableId
 * 3. Check if the varName already is stored in symbol table.  It is an
 *    error if it is.
 * 4. Otherwise, store <tableId, varName> in the symbol table.
 *
 * @param  tableName   name of the table (stream/relation)
 *                     corresponding to this entry
 * @param  varName     variable name by which this instance of the
 *                     stream/relation is referred in the query.
 * 
 * @return             0 (success) !0 (failure)
 */

static int addSymbolTableEntry(const char *tableName, const char *varName)
{
	unsigned int  tableId;  
  
	ASSERT(tableName);
	ASSERT(varName);
	  
	// Table not registered?
	if(!tableMgr -> getTableId(tableName, tableId)) {
		return UNKNOWN_TABLE_ERR;
	}
	
	// varName already in symbol table?
	for(unsigned int s = 0 ; s < numSymbolTableEntries ; s++) 
		if(strcmp(symbolTable[s].varName, varName) == 0)
			return AMBIGUOUS_TABLE_ERR;
  
	// symbol table full?
	if(numSymbolTableEntries == MAX_SYMBOLS)
		return -1;
	
	symbolTable[numSymbolTableEntries].varName = varName;
	symbolTable[numSymbolTableEntries].tableId = tableId;
  
	numSymbolTableEntries++;
  
	return 0;
}

/**
 * Get the tables referenced in an SFW query.  Build a symbol table for
 * these tables.  Also store the information in the passed query object.
 *
 * Currently, the  only class of  queries considered is SFW  queries.  For
 * these queries the referenced tables are the tables in the FROM clause.
 */
static int getSFWReferencedTables (NODE *parseTree, Query& query)
{
	int        rc;
	NODE       *sfw_block;
	NODE       *table_list;	
	NODE       *table;
	const char *tableName;
	const char *varName;
	
	// Sanity check:
	ASSERT(parseTree -> u.CONT_QUERY.sfw_block);
	
	// Select-from-where block
	sfw_block = parseTree -> u.CONT_QUERY.sfw_block;
	
	ASSERT(sfw_block -> kind == N_SFW_BLOCK);
	ASSERT(sfw_block -> u.SFW_BLOCK.rel_list);
	
	// list of tables in the from-clause.
	table_list = sfw_block -> u.SFW_BLOCK.rel_list;
	
	// Iterate through all the tables referenced
	while(table_list) {
		ASSERT(table_list -> kind == N_LIST);
		
		table = table_list -> u.LIST.curr;
		
		ASSERT(table);
		ASSERT(table -> kind == N_REL_VAR);
		ASSERT(table -> u.REL_VAR.rel_name);

		// name of the table
		tableName = table -> u.REL_VAR.rel_name;

		// variable name of this instance of the table
		if(table -> u.REL_VAR.var_name)
			varName = table -> u.REL_VAR.var_name;
		else
			varName = tableName;
		
		// insert <varName, tableName> into the symbol table
		if((rc = addSymbolTableEntry(tableName, varName)) != 0)
			return rc;		
		
		table_list = table_list -> u.LIST.next;
	}
	
	query.numRefTables = numSymbolTableEntries;
	for(unsigned int t = 0 ; t < numSymbolTableEntries ; t++)
		query.refTables [t] = symbolTable[t].tableId;
	
	return 0;
}

static int getBinOpReferencedTables (NODE *parseTree, Query &query)
{
	NODE         *bin_op;
	char         *left_table;
	char         *right_table;
	unsigned int  leftTableId;
	unsigned int  rightTableId;
	
	ASSERT (parseTree -> u.CONT_QUERY.bin_op);	
	bin_op = parseTree -> u.CONT_QUERY.bin_op;
	
	ASSERT (bin_op -> kind == N_UNION ||
			bin_op -> kind == N_EXCEPT);

	if (bin_op -> kind == N_UNION) {				
		left_table = bin_op -> u.UNION.left_table;
		right_table = bin_op -> u.UNION.right_table;
	}
	
	else {		
		left_table = bin_op -> u.EXCEPT.left_table;
		right_table = bin_op -> u.EXCEPT.right_table;
	}	
	ASSERT (left_table && right_table);
	
	if(!tableMgr -> getTableId(left_table, leftTableId)) 
		return UNKNOWN_TABLE_ERR;
	
	if(!tableMgr -> getTableId(right_table, rightTableId)) 
		return UNKNOWN_TABLE_ERR;
	
	query.numRefTables = 2;
	query.refTables [0] = leftTableId;
	query.refTables [1] = rightTableId;
	
	return 0;
}

static int getReferencedTables (NODE *parseTree, Query &query)
{
	// Sanity check
	ASSERT (parseTree);
	ASSERT (parseTree -> kind == N_CONT_QUERY);

	// Query has to be a SFW query or a binary operator query 
	ASSERT (parseTree -> u.CONT_QUERY.sfw_block &&
			!parseTree -> u.CONT_QUERY.bin_op
			||			
			parseTree -> u.CONT_QUERY.bin_op &&
			!parseTree -> u.CONT_QUERY.sfw_block);
	
	if (parseTree -> u.CONT_QUERY.sfw_block)
		return getSFWReferencedTables (parseTree, query);
	
	// else
	return getBinOpReferencedTables (parseTree, query);	
}	

/**
 * Convert the syntactic variable name (string) to the "semantic" variable
 * id (unsigned int).  The variable id of a variable name is just its
 * position in the symbol table of variable names.
 *
 */
static int interpretVarName(const char *varName, unsigned int& varId)
{    
	ASSERT(varName);
	
	for(varId = 0 ; varId < numSymbolTableEntries ; varId++)
		if(strcmp(varName, symbolTable[varId].varName) == 0)
			return 0;
	return UNKNOWN_VAR_ERR;
}


/**
 * Convert  a syntactic attribute  to a  semantic attribute.   A syntactic
 * attribute (e.g.,  "R.A" or  "A") has an  optional variable  name string
 * ("R") and a non-optional  attribute name ("A").  The semantic attribute
 * is of the  form <varId, attrId> is the unique variable  id of the table
 * instance to  which "A" belongs, and  attrId is the  identifier for this
 * attribute within the table.
 *
 * @param synAttr     parse tree node encoding a syntactic attribute
 * @param semAttr     output semantic attribute
 *
 * @return            0 (success), !0 (failure)       
 */
static int interpretAttr(NODE *synAttr, Attr& semAttr)
     
{
	int             rc;
	const char     *varName;
	const char     *attrName;	
	unsigned int    varId;
	unsigned int    attrId, a;
	unsigned int    tableId;
	    
	// Sanity check
	ASSERT(synAttr);
	ASSERT(synAttr -> kind == N_ATTR_REF);
	
	varName = synAttr -> u.ATTR_REF.rel_var_name;
	attrName = synAttr -> u.ATTR_REF.attr_name;
	
	// Variable name explicit (e.g., "R.A")
	if(varName) {
		
		// varName -> varId
		if((rc = interpretVarName(varName, varId)) != 0)
			return rc;

		tableId = symbolTable[varId].tableId;
		
		// <tableId,attrName> -> attrId
		if(!tableMgr -> getAttrId(tableId, attrName, attrId))
			return UNKNOWN_ATTR_ERR;
	}
	
	// Variable name implicit: for all possible variable id's check if the
	// attribute belongs to the table corr. to the variable id.
	else {      
		bool bFound = false;
    
		for(unsigned int v = 0 ; v < numSymbolTableEntries ; v++) {
			
			tableId = symbolTable[v].tableId;
			
			// <tableId,attrName> -> attrId
			if(tableMgr -> getAttrId(tableId, attrName, a)) {
				// Ambiguous attribute
				if(bFound) {
					return AMBIGUOUS_ATTR_ERR;
				}
				
				bFound = true;
				varId = v;
				attrId = a;
			}
		}
		
		// Nope I could not find the variable to which this attribute
		// corrs.		
		if(!bFound)
			return UNKNOWN_ATTR_ERR;
	}
	
	semAttr.varId = varId;
	semAttr.attrId = attrId;
	
	return 0;
}

/**
 * Convert an expression from its syntactic form to its semantic form.
 * This involves replacing all "syntactic" attributes to "semantic
 * attributes" using the interpretAttr function.
 *
 * A syntactic expression could be an arithmetic expression, a constant
 * value, a (syntactic) attribute, or a aggregation expression.
 */
static int interpretExpr(NODE *synExpr, Expr *& semExpr)
{   
	Expr          *leftSemExpr, *rightSemExpr;
	Attr           semAttr;
	Type           semAttrType;
	Type           aggrOutputType;
	AggrFn         aggrFn;
	int    rc;
  
	ASSERT(synExpr);
	
	switch(synExpr -> kind) {
    
	case N_ARITH_EXPR:
		semExpr = newExpr (E_COMP_EXPR);    
    
		if((rc = interpretExpr(synExpr -> u.ARITH_EXPR.left_expr,
							   leftSemExpr)) != 0)
			return rc;
		
		if((rc = interpretExpr(synExpr -> u.ARITH_EXPR.right_expr,
							   rightSemExpr)) != 0)
			return rc;
		
		// Type incompatible operators
		if(leftSemExpr -> type != rightSemExpr -> type)
			return TYPE_ERR;
    
		semExpr -> type              = leftSemExpr -> type;
		semExpr -> u.COMP_EXPR.op    = synExpr -> u.ARITH_EXPR.op;
		semExpr -> u.COMP_EXPR.left  = leftSemExpr;
		semExpr -> u.COMP_EXPR.right = rightSemExpr;
    
		break;
    
	case N_AGGR_EXPR:
		semExpr = newExpr (E_AGGR_EXPR);

		aggrFn = synExpr -> u.AGGR_EXPR.fn;

		// Count aggr. function is attribute independent.  So we make it
		// the count over first table, first attribute without changing
		// query semantics
		if (aggrFn == COUNT) {
			semAttr.varId = 0;   
			semAttr.attrId = 0;  
		}
		else {
			ASSERT (synExpr -> u.AGGR_EXPR.attr);
			
			if((rc = interpretAttr(synExpr -> u.AGGR_EXPR.attr,
								   semAttr))  != 0) {				
				return rc;
			}
		}
		
		semAttrType = tableMgr ->
			getAttrType(symbolTable[semAttr.varId].tableId, semAttr.attrId);
		
		aggrOutputType = getOutputType(aggrFn, semAttrType);
		
		semExpr -> u.AGGR_EXPR.fn   = aggrFn;
		semExpr -> u.AGGR_EXPR.attr = semAttr;
		semExpr -> type             = aggrOutputType;
		
		break;
		
	case N_CONST_VAL:
		semExpr = newExpr (E_CONST_VAL);
		
		semExpr -> type = synExpr -> u.CONST_VAL.type;
		
		switch (semExpr -> type) {
		case INT:   
			semExpr -> u.ival = synExpr -> u.CONST_VAL.ival; break;			
			
		case FLOAT: 
			semExpr -> u.fval = synExpr -> u.CONST_VAL.fval; break;
			
		case CHAR:
			semExpr -> u.sval = synExpr -> u.CONST_VAL.sval; break;
			
		case BYTE:
			semExpr -> u.bval = synExpr -> u.CONST_VAL.bval; break;
			
		default:
			return -1;
		}
		break;
		
	case N_ATTR_REF:
		
		semExpr = newExpr (E_ATTR_REF);
		
		if((rc = interpretAttr(synExpr, semAttr)) != 0)
			return rc;
		
		semAttrType = tableMgr ->
			getAttrType(symbolTable[semAttr.varId].tableId, semAttr.attrId);
		
		semExpr -> type            = semAttrType;
		semExpr -> u.attr          = semAttr; 
		
		break;
    
	default:        
		return -1;
	}
  
	return 0;
}

/**
 * Convert a syntactic condition predicate into a "semantic" boolean
 * expression.  This involves converting the syntactic left and right
 * (syntactic) expressions into semantic expressions using interpretExpr
 * function. 
 */
static int interpretCondn (NODE *condn, BExpr& bexpr) 
{  
	int    rc;
	Expr  *leftSemExpr, *rightSemExpr;

	ASSERT(condn);
	ASSERT(condn -> kind == N_CONDN);
	
	// Interpret left expression
	if((rc = interpretExpr(condn -> u.CONDN.left_expr, 
						   leftSemExpr)) != 0)
		return rc;
	
	// Interpret right expression
	if((rc = interpretExpr(condn -> u.CONDN.right_expr,
						   rightSemExpr)) != 0)
		return rc;
	
	
	bexpr.op       = condn -> u.CONDN.op;
	bexpr.left     = leftSemExpr;
	bexpr.right    = rightSemExpr;

	// Left & right are type incompatible
	if (bexpr.left -> type != bexpr.right -> type)
		return TYPE_ERR;
	
	return 0;
}

/**
 * Convert a syntactic time-window node in a parse tree to the semantic
 * form.  This involves converting all window lengths to seconds.
 * Conversion will be done for the window range and for the window slide
 * if it was set. 
 */
static int interpretRangeWin(NODE *synWin, WindowSpec& semWin) 
{
	
	NODE         *timeSpec;
	unsigned int  numSeconds[2];
    	
	// Canonicalize the timespecification
    for (unsigned int i = 0 ; i<2 ; i++) {
        // First convert the window range, then the window slide
        if (i == 0) {
            timeSpec = synWin -> u.WINDOW_SPEC.time_spec;
        } else {
            timeSpec = synWin -> u.WINDOW_SPEC.slide_spec;
            // Continue if the window slide is set
            if (timeSpec == 0) {
                numSeconds[i] = 0;
                continue;
            }
        }

        ASSERT(timeSpec);
        ASSERT(timeSpec -> kind == N_TIME_SPEC);
	
        switch (timeSpec -> u.TIME_SPEC.unit) {
            
        case Parser::NOTIMEUNIT:
        case Parser::SECOND:
            
            numSeconds[i] = timeSpec -> u.TIME_SPEC.len;
            break;
            
        case Parser::MINUTE:
            
            numSeconds[i] = timeSpec -> u.TIME_SPEC.len * 60;
            break;
            
        case Parser::HOUR:
            
            numSeconds[i] = timeSpec -> u.TIME_SPEC.len * 60 * 60;
            break;
            
        case Parser::DAY:
            
            numSeconds[i] = timeSpec -> u.TIME_SPEC.len * 60 * 60 * 24;
            break;
            
        default:
            return -1;
        }
    }
	
	semWin.type = RANGE;
	semWin.u.RANGE.timeUnits = numSeconds[0];
	semWin.u.RANGE.slideUnits = numSeconds[1];

	return 0;
}

/**
 * Convert a syntactic row-based window in a parse tree to the semantic
 * form.  Hardly anything to do ...
 */
static int interpretRowWin(NODE *synWin, WindowSpec& semWin)
{
	semWin.type          = ROW;
	semWin.u.ROW.numRows = synWin -> u.WINDOW_SPEC.num_rows;
	
	return 0;
}

/**
 * Convert a partition window in a parse tree to the semantic form.
 * Essentially involves converting all attributes from syntactic to
 * semantic form.
 */
static int interpretPartnWin(NODE *synWin, WindowSpec& semWin)
{
	unsigned int  numPartnAttrs;
	NODE         *synPartnAttrList;
	NODE         *synPartnAttr;
	Attr          semPartnAttr;
	int           rc;
	
	semWin.type  = PARTITION;
	semWin.u.PARTITION.numRows = synWin -> u.WINDOW_SPEC.num_rows;
	
	numPartnAttrs = 0;
	synPartnAttrList = synWin -> u.WINDOW_SPEC.part_win_attr_list;
	
	while (synPartnAttrList) {
		
		// Interpret the partition attribute
		synPartnAttr = synPartnAttrList -> u.LIST.curr;		
		if((rc = interpretAttr(synPartnAttr, semPartnAttr)) != 0)
			return rc;
		
		// Too many partition attributes ?
		if(numPartnAttrs >= MAX_PARTN_ATTRS)
			return -1;
		
		semWin.u.PARTITION.attrs [numPartnAttrs]
			= semPartnAttr;
		numPartnAttrs++;
		
		synPartnAttrList = synPartnAttrList -> u.LIST.next;
	}
	
	semWin.u.PARTITION.numPartnAttrs = numPartnAttrs;
	
	return 0;
}

/**
 * Convert a now window in a parse tree to the semantic form: no work
 */
static int interpretNowWin(NODE *synWin, WindowSpec& semWin) 
{
	semWin.type = NOW;
	return 0;
}

/**
 * Convert an UNBOUNDED window in a parse tree ot the semantic form: no
 * work
 */
static int interpretUnboundedWin(NODE *synWin, WindowSpec& semWin)
{
	semWin.type = UNBOUNDED;
	return 0;
}


/**
 * Convert a syntactic window specification node in the parse tree of a
 * query to a semantic form.  Based on the window type call the appropriate
 * "interpret" function.
 */ 
int interpretWin(NODE *synWin, WindowSpec& semWin)
{  
	
  	// Window explicitly specified:
	if(synWin) {
		
		ASSERT(synWin -> kind == N_WINDOW_SPEC);
				
		switch(synWin -> u.WINDOW_SPEC.type) {
			
		case ROW:
			return interpretRowWin(synWin, semWin);
      
		case RANGE:
			return interpretRangeWin(synWin, semWin);
			
		case NOW:
			return interpretNowWin(synWin, semWin);
      
		case PARTITION:
			return interpretPartnWin(synWin, semWin);
			
		case UNBOUNDED:
			return interpretUnboundedWin(synWin, semWin);
			
		default:
			return -1;
		}
	}
	
	// Window not specified: default unbounded window
	else {    
		semWin.type = UNBOUNDED;
	}
	
	return 0;
}

/**
 * Interpret the Rel-to-stream operator.  Not much work: just record if
 * the operator is present or not, and the type of the operator if
 * present.
 */
static int interpretR2SOp(NODE *r2snode, Query& query)
{
	// relation to stream operator present
	if(r2snode) {
		ASSERT(r2snode -> kind == N_XSTREAM);
		
		query.bRelToStr = true;
		
		switch(r2snode -> u.XSTREAM.op) {
		case Parser::ISTREAM: query.r2sOp = Semantic::ISTREAM;
			break;	
		case Parser::DSTREAM: query.r2sOp = Semantic::DSTREAM;
			break;
		case Parser::RSTREAM: query.r2sOp = Semantic::RSTREAM;
			break;
		default:
			return -1;
		}
	}
	
	// relation to stream operator absent
	else {
		query.bRelToStr = false;
	}
	
	return 0;
}


/**
 * Interpret the from clause of a query: [[ Comment ]] 
 */
static int interpretFromClause(NODE *fromClause, Query& query)
{
	int           rc;
	
	// Each listing in FROM clause is a relation expression: either a
	// standalone relation or a stream with a (explicit/implicit) window.	
	NODE         *relExpr;         // Relation expression in from clause	
	const char   *varName;         // Variable name of the table in relExpr
	unsigned int  varId;           // Variable id corresponding to varName
	unsigned int  tableId;         // Table id of the table in relExpr	
	NODE         *synWin;          // Window node in relExpr if exists
	WindowSpec    semWin;          // Semantic window ("interpreted" synWin)
		
	query.numFromClauseTables = 0;
	
	while(fromClause) {
		
		ASSERT(fromClause -> kind == N_LIST);
		
		// relExpr
		relExpr = fromClause -> u.LIST.curr;
		ASSERT(relExpr);
		ASSERT(relExpr -> kind == N_REL_VAR);
		
		// varName
		if(relExpr -> u.REL_VAR.var_name)
			varName = relExpr -> u.REL_VAR.var_name;
		else 
			varName = relExpr -> u.REL_VAR.rel_name;
		ASSERT(varName);
		
		// varId (from varName)
		if((rc = interpretVarName(varName, varId)) != 0)
			return rc;
		
		// tableId
		tableId = symbolTable[varId].tableId;
		
		// synWin
		synWin = relExpr -> u.REL_VAR.window_spec;
		
		// tableId corresponds to a stream
		if(tableMgr -> isStream(tableId)) {
			
			// Interpret semWin (synWin can be 0 in which case the default
			// UNBOUNDED window is produced
			if((rc = interpretWin(synWin, semWin)) != 0)
				return rc;
		}
		
		// tableId corresponds to a relation: should not contain a window
		else if(synWin)
			return WINDOW_OVER_REL_ERR;			
		
		// Error: too many tables?		
		if(query.numFromClauseTables == MAX_TABLES) 
			return -1;
		
		// Update query .........
		
		query.fromClauseTables [ query.numFromClauseTables ] = varId;
		query.winSpec          [ query.numFromClauseTables ] = semWin;
		query.numFromClauseTables ++;
		
		// Next rel expr.
		fromClause = fromClause -> u.LIST.next;
	}
  
	return 0;
}

/**
 * Converts the list of parsed conditions in the where clause to the
 * semantic forms: 
 */
static int interpretWhereClause(NODE* whereClause, Query& query) 
{
	int    rc;
	NODE  *synCond;
	BExpr  semBExpr;

	query.numPreds = 0;
	
	while(whereClause) {
		ASSERT(whereClause -> kind == N_LIST);
		
		synCond = whereClause -> u.LIST.curr;
		ASSERT(synCond);
		
		// synCond -> semBExpr
		if((rc = interpretCondn(synCond, semBExpr)) != 0)
			return rc;
		
		// too many predicates?
		if(query.numPreds >= MAX_PRED)
			return -1;
		
		query.preds [ query.numPreds ] = semBExpr;
		query.numPreds ++;

		whereClause = whereClause -> u.LIST.next;
	}
	
	return 0;
}

static int interpretGroupByClause(NODE *gbyClause, Query& query)
{
	int     rc;
	NODE   *synAttr;
	Attr    semAttr;
	
	query.numGbyAttrs = 0;
	
	while(gbyClause) {
		ASSERT(gbyClause -> kind == N_LIST);
		
		synAttr = gbyClause -> u.LIST.curr;
		ASSERT(synAttr);
		
		// SynAttr -> semAttr
		if((rc = interpretAttr(synAttr, semAttr)) != 0)
			return rc;
		
		// Too many group by attributes?
		if(query.numGbyAttrs >= MAX_GROUP_ATTRS)
			return -1;
		
		query.gbyAttrs [ query.numGbyAttrs ] = semAttr;
		query.numGbyAttrs ++;
		
		gbyClause = gbyClause -> u.LIST.next;
	}
	
	return 0;
}

/**
 * converts the "*" in SELECT * to the correct list of project
 * attributes. This list is the just the concatenation of all the
 * attributes of all the tables occurring in the FROM clause.
 */
static int interpretStarInSelect(Query& query) {
	unsigned int  numProjExprs;
	unsigned int  varId;
	unsigned int  tableId;	
	unsigned int  numAttrs;
	Type          attrType;
	Expr         *expr;
	
	numProjExprs = 0;
	
	for(unsigned int t = 0 ; t < query.numFromClauseTables ; t++) {
		varId    = query.fromClauseTables [t];
		tableId  = query.refTables [varId];
		
		numAttrs = tableMgr -> getNumAttrs (tableId);
		
		for(unsigned int attrId = 0 ; attrId < numAttrs ; attrId++) {
			
			// Too many project expressions?
			if(numProjExprs >= MAX_PROJ)
				return -1;
			
			attrType = tableMgr -> getAttrType(tableId, attrId);
			
			expr = newExpr(E_ATTR_REF);
			
			expr -> exprType      = E_ATTR_REF;
			expr -> type          = attrType;
			expr -> u.attr.varId  = varId;
			expr -> u.attr.attrId = attrId;
			
			query.projExprs [ numProjExprs ] = expr;			
			numProjExprs ++;
		}
	}

	query.numProjExprs = numProjExprs;

	return 0;
}

/**
 * converts a syntactic parsed form of the select clause to the sematnic
 * representation of the select classi n the query object
 */
static int interpretSelectClause (NODE *selectClause, Query& query)
{
	int            rc;
	NODE          *synProjExprList;
	unsigned int  numProjExprs;
	NODE          *synProjExpr;
	Expr          *semProjExpr;
	
	
	ASSERT(selectClause);
	ASSERT(selectClause -> kind == N_SELECT_CLAUSE);
	
	// Distinct flag:
	query.bDistinct = selectClause -> u.SELECT_CLAUSE.b_distinct;
	
	synProjExprList = selectClause -> u.SELECT_CLAUSE.proj_term_list;

	// The select clause does not contain a project expression ==> the
	// original non-parsed query was of the fomr SELECT *.  
	if(!synProjExprList) {
		return interpretStarInSelect(query);
	}
	
	// The select clause does not have a *:
	numProjExprs = 0;
	while(synProjExprList) {
		ASSERT(synProjExprList -> kind == N_LIST);
		
		synProjExpr = synProjExprList -> u.LIST.curr;
		ASSERT(synProjExpr);
		
		// synProjExpr -> semProjExpr
		if((rc = interpretExpr(synProjExpr, semProjExpr)) != 0)
			return rc;
		
		// Too many project expressions
		if(numProjExprs >= MAX_PROJ)
			return -1;
		
		query.projExprs [ numProjExprs ] = semProjExpr;
		numProjExprs ++;
		
		synProjExprList = synProjExprList -> u.LIST.next;
	}
	
	query.numProjExprs = numProjExprs;
	
	return 0;
}


SemanticInterpreter::SemanticInterpreter(TableManager *_tableMgr) 
{
	this -> _tableMgr = _tableMgr;
}

static bool schemaCompatible (unsigned int table1, unsigned int table2)
{
	unsigned int numAttrs;
	
	if (tableMgr -> getNumAttrs (table1) !=
		tableMgr -> getNumAttrs (table2))
		return false;
	
	numAttrs = tableMgr -> getNumAttrs (table1);

	// Note: strings of different lengths are compatible
	for (unsigned int a = 0 ; a < numAttrs ; a++) {
		if (tableMgr -> getAttrType (table1, a) !=
			tableMgr -> getAttrType (table2, a)) {
			
			return false;
		}
	}
	
	return true;
}
	
/**
 * Converts a syntactic parsed form of a query (NODE object) into a "semantic"
 * representation (Query object).  See the comments about struct Query to
 * find out what this conversion involves.
 */

int SemanticInterpreter::interpretQuery(NODE  *parseTree, Query& query)
{	
	int         rc;
	NODE       *sfw_block;
	
	// Sanity checks: parse tree should correspond to a continuous query,
	// the query should contain a select-from-where block.
	ASSERT(parseTree);
	ASSERT(parseTree -> kind == N_CONT_QUERY);
	
	tableMgr = this -> _tableMgr;
	resetExprs();
	resetSymbolTable();
	
	// Get the list of tables referenced bu this query
	if((rc = getReferencedTables (parseTree, query)) != 0)
		return rc;	

	// Query type
	ASSERT (parseTree -> u.CONT_QUERY.sfw_block &&
			!parseTree -> u.CONT_QUERY.bin_op
			||			
			parseTree -> u.CONT_QUERY.bin_op &&
			!parseTree -> u.CONT_QUERY.sfw_block);
	
	if (parseTree -> u.CONT_QUERY.sfw_block)
		query.kind = SFW_QUERY;
	else
		query.kind = BINARY_OP;

	// Interpret the (optional) Rel-to-str operator
	if((rc = interpretR2SOp (parseTree -> u.CONT_QUERY.reltostr_op,
							 query)) != 0)
		return rc;
		
	if (query.kind == SFW_QUERY) {
		
		sfw_block = parseTree -> u.CONT_QUERY.sfw_block;
		
		ASSERT(sfw_block);
		ASSERT(sfw_block -> kind == N_SFW_BLOCK);
		ASSERT(sfw_block -> u.SFW_BLOCK.rel_list);
		
		// Interpret the from clause.
		if((rc = interpretFromClause(sfw_block -> u.SFW_BLOCK.rel_list,
									 query)) != 0)
			return rc;
		
		// Interpret the (optional) where clause.
		if((rc = interpretWhereClause(sfw_block -> u.SFW_BLOCK.condition_list,
									  query)) != 0)
			return rc;
		
		// Interpret the (optional) group by clause.
		if((rc = interpretGroupByClause(sfw_block -> u.SFW_BLOCK.group_list,
										query)) != 0)
			return rc;
		
		// Interpret the select clause.  We do it last because it references
		// FROM clause and GROUP by clause.
		ASSERT(sfw_block -> u.SFW_BLOCK.select_clause);
		if((rc = interpretSelectClause(sfw_block -> u.SFW_BLOCK.select_clause,
									   query)) != 0)
			return rc;
	}
	
	else {
		query.leftTable = 0;
		query.rightTable = 1;

		if (parseTree -> u.CONT_QUERY.bin_op -> kind == N_UNION) {
			query.binOp = UNION;
		}

		else {
			query.binOp = EXCEPT;
		}
		
		// Check if the schemas of the two tables match
		if (!schemaCompatible (query.refTables [0],
							   query.refTables [1]))
			return SCHEMA_MISMATCH_ERR;
	}
	
	return 0;
}
