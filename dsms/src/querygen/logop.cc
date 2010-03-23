/**
 * @file       logop.cc
 * @date       May 26, 2004
 * @brief      Routines to manage operators used by logical plan generator
 *             (Logical namespace)
 */

#include <iostream>
using namespace std;

#ifndef _DEBUG_
#include "common/debug.h"
#endif

#ifndef _LOGOP_
#include "querygen/logop.h"
#endif

using namespace std;
using namespace Logical;


// Ugly .... :(
static Metadata::TableManager *tableMgr;

void Logical::set_table_mgr (Metadata::TableManager *_tableMgr)
{
	tableMgr = _tableMgr;
}

//----------------------------------------------------------------------
// 
// Routines for managing space used by operators (very similar to Redbase
// NODEs)
//
//----------------------------------------------------------------------

#define MAXOPERATOR  500
static Operator operator_pool[MAXOPERATOR];
static int operator_ptr = 0;

/**
 * Reset the pool of operators.  All previously constructed (logical)
 * operators are deallocated.
 */

void Logical::reset_op_pool()
{
	operator_ptr = 0;
}

/**
 * Allocate a new operator from the pool of operators
 */
static Operator *newop(OperatorKind kind)
{
	Operator *op;
	
	if(operator_ptr == MAXOPERATOR) {
		return 0;
	}
	
	op = operator_pool + operator_ptr;

#ifdef _DM_
	op -> id = operator_ptr;
#endif

	operator_ptr++;	
	
	op -> kind = kind;
	op -> output = 0;
	
	return op;
}

//----------------------------------------------------------------------
// Help routines
//----------------------------------------------------------------------
static int append_schema (Operator *dest, const Operator *src)
{
	ASSERT (dest);
	ASSERT (src);
	
	if (dest -> numOutAttrs + src -> numOutAttrs > MAX_ATTRS)
		return -1;
	
	for (unsigned int a = 0 ; a < src -> numOutAttrs ; a++)
		dest -> outAttrs [ dest -> numOutAttrs ++]
			= src -> outAttrs [a];
	return 0;
}

static int copy_schema (Operator *dest, const Operator *src)
{
	ASSERT (dest);
	ASSERT (src);
	
	dest -> numOutAttrs = src -> numOutAttrs;
	
	for (unsigned int a = 0 ; a < dest -> numOutAttrs ; a++)
		dest -> outAttrs [a] = src -> outAttrs [a];
	
	return 0;
}



//----------------------------------------------------------------------
// 
// Basic operator constructors
//
//----------------------------------------------------------------------

// Stream source
Operator *Logical::mk_stream_source(unsigned int varId, unsigned int strId)
{
	unsigned int numOutAttrs;
	
	Operator *op = newop(LO_STREAM_SOURCE);
	
	if(!op) return 0;
	
	// Number of attributes in the stream schema
	numOutAttrs = tableMgr->getNumAttrs(strId);
	ASSERT(numOutAttrs > 0);
	
	// Determine the output schema
	op -> numOutAttrs = numOutAttrs;
	for (unsigned int a = 0 ; a < numOutAttrs ; a++) {
		op -> outAttrs[a].kind            = NAMED;
		op -> outAttrs[a].u.NAMED.varId   = varId;
		op -> outAttrs[a].u.NAMED.tableId = strId;
		op -> outAttrs[a].u.NAMED.attrId  = a;
	}
	
	op -> bStream = true;
	op -> numInputs = 0;
	
	op -> u.STREAM_SOURCE.varId = varId;
	op -> u.STREAM_SOURCE.strId = strId;
	
	return op;
}

// Relation source
Operator *Logical::mk_reln_source(unsigned int varId, unsigned int relnId)
{
	unsigned int numOutAttrs;
	
	Operator *op = newop(LO_RELN_SOURCE);
	
	if(!op) return 0;
	
	// Number of attributes in the relatin schema
	numOutAttrs = tableMgr->getNumAttrs(relnId);
	ASSERT(numOutAttrs > 0);
	
	// Determine output schema
	op -> numOutAttrs = numOutAttrs;
	for(unsigned int a = 0 ; a < numOutAttrs ; a++) {
		op -> outAttrs[a].kind            = NAMED;
		op -> outAttrs[a].u.NAMED.varId   = varId;
		op -> outAttrs[a].u.NAMED.tableId = relnId;
		op -> outAttrs[a].u.NAMED.attrId  = a;
	}
	
	op -> bStream = false;
	op -> numInputs = 0;

	op -> u.RELN_SOURCE.varId = varId;
	op -> u.RELN_SOURCE.relId = relnId;
	
	return op;
}

// Range window
Operator *Logical::mk_range_window(Operator *input, unsigned int timeUnits, 
  unsigned int strideUnits)
{
	Operator *op = newop(LO_RANGE_WIN);
	
	if(!op) return 0;
	
	// Input should be a stream
	ASSERT(input);
	ASSERT(input -> bStream);
	
	copy_schema (op, input);

	op -> numInputs = 1;
	op -> inputs [0] = input;
	op -> bStream = false;	
	
	op -> u.RANGE_WIN.timeUnits = timeUnits;
    op -> u.RANGE_WIN.strideUnits = strideUnits;
	
	input -> output = op;
	
	return op;
}

// Row window
Operator *Logical::mk_row_window(Operator *input, unsigned int numRows)
{
	Operator *op = newop(LO_ROW_WIN);

	if(!op) return 0;	

	// Input should be a stream
	ASSERT(input);
	ASSERT(input -> bStream);

	// Output schema = input schema
	copy_schema (op, input);
	
	op -> numInputs = 1;
	op -> inputs [0] = input;
	op -> bStream = false;
	op -> u.ROW_WIN.numRows = numRows;
	
	input -> output = op;
	
	return op;
}

// Now window
Operator *Logical::mk_now_window(Operator *input)
{
	Operator *op = newop(LO_NOW_WIN);

	if(!op) return 0;	

	// Input should be a stream
	ASSERT(input);
	ASSERT(input -> bStream);
	
	// Output schema = input schema
	copy_schema (op, input);
	
	op -> numInputs = 1;
	op -> inputs [0] = input;
	op -> bStream = false;
	
	input -> output = op;
	
	return op;
}

// Partition window
Operator *Logical::mk_partn_window(Operator *input, unsigned int numRows)
{
	Operator *op = newop(LO_PARTN_WIN);
	
	if(!op) return 0;

	// Input should be a stream
	ASSERT(input);
	ASSERT(input -> bStream);
	
	// Output schema = input schema
	copy_schema (op, input);
	
	op -> numInputs = 1;
	op -> inputs [0] = input;
	op -> bStream = false;
	op -> u.PARTN_WIN.numPartnAttrs = 0;
	op -> u.PARTN_WIN.numRows = numRows;
	
	input -> output = op;
	
	return op;
}

// Partition window (continued)
Operator *Logical::partn_window_add_attr(Operator *pwin, Attr attr)
{
	// Has to be a partition window
	ASSERT(pwin -> kind == LO_PARTN_WIN);
	
	// Too many attributes?
	if(pwin -> u.PARTN_WIN.numPartnAttrs == MAX_GROUP_ATTRS)
		return 0;
	
	pwin -> u.PARTN_WIN.partnAttrs
		[pwin -> u.PARTN_WIN.numPartnAttrs++] = attr;
		
	return pwin;
}

// Select
Operator *Logical::mk_select(Operator *input, BExpr bexpr)
{
	Operator *op = newop(LO_SELECT);
	
	if(!op) return 0;
	
	ASSERT(input);
	
	// Outputschema = input schema
	copy_schema (op, input);
	
	// Does not affect +/- nature of input
	op -> bStream = input -> bStream;
	
	op -> numInputs = 1;
	op -> inputs [0] = input;
	op -> u.SELECT.bexpr = bexpr;
		
	input -> output = op;
	
	return op;
}

// Project
Operator *Logical::mk_project(Operator *input)
{
	Operator *op = newop(LO_PROJECT);
	
	if(!op) return 0;
	
	ASSERT(input);
	
	// No project expressions added yet
	op -> numOutAttrs = 0;
	
	// Retains +/- effect of input
	op -> bStream = input -> bStream;
	
	op -> numInputs = 1;
	op -> inputs [0] = input;
	
	input -> output = op;
	
	return op;
}

// Project (continued)
Operator *Logical::add_project_expr(Operator *proj, Expr *expr)
{
	unsigned int numOutAttrs;

	ASSERT(proj -> kind == LO_PROJECT);
	ASSERT(expr);

	numOutAttrs = proj -> numOutAttrs;

	// Too many project attributes
	if(numOutAttrs == MAX_ATTRS)
		return 0;
	
	proj -> u.PROJECT.projExprs [ numOutAttrs ] = expr;
	
	// Type of the (new) output attribute
	if(expr -> kind == ATTR_REF) {
		proj -> outAttrs [ numOutAttrs ] = expr -> u.attr;	
	}
	else {
		proj -> outAttrs [ numOutAttrs ].kind = UNNAMED;		
		proj -> outAttrs [ numOutAttrs ].u.UNNAMED.type
			= expr -> type;
	}
		
	proj -> numOutAttrs ++;
	
	return proj;
}

// Cross product
Operator *Logical::mk_cross()
{
	Operator *op = newop(LO_CROSS);
	
	if(!op) return 0;
	
	op -> numOutAttrs = 0;
	op -> bStream = true;
	op -> numInputs = 0;
	
	return op;
}

// Cross product (continued)
Operator *Logical::cross_add_input(Operator *cross, Operator *input)
{
	ASSERT(cross -> kind == LO_CROSS);
	ASSERT(input);
	
	// Too many attributes in the output of cross?
	if(cross -> numOutAttrs + input -> numOutAttrs > MAX_ATTRS)
		return 0;
	
	// too many inputs?
	if(cross -> numInputs == MAX_INPUT_OPS)
		return 0;
	
	cross -> inputs [ cross -> numInputs++ ] = input;
	
	// Append the schema of the new input to the existing schema
	for(unsigned int a = 0 ; a < input -> numOutAttrs ; a++)
		cross -> outAttrs
			[ cross -> numOutAttrs ++ ] = input -> outAttrs [a];
	
	// Even one input that has '-' makes the output of cross contain '-'
	if(!input -> bStream)
		cross -> bStream = false;
	
	input -> output = cross;
	
	return cross;
}

// Group by Aggregate
Operator *Logical::mk_group_aggr(Operator *input)
{
	Operator *op = newop(LO_GROUP_AGGR);
	
	if(!op) return 0;
	
	ASSERT(input);
	
	op -> numOutAttrs = 0;
	op -> bStream = false;
	op -> u.GROUP_AGGR.numGroupAttrs = 0;
	op -> u.GROUP_AGGR.numAggrAttrs = 0;
	
	op -> numInputs = 1;
	op -> inputs [0] = input;
	
	input -> output = op;
	
	return op;
}

// Group by Aggregate (continued)
Operator *Logical::add_group_attr(Operator *op, Attr attr)
{
	ASSERT(op -> kind == LO_GROUP_AGGR);
	ASSERT(op -> numOutAttrs ==
		   op -> u.GROUP_AGGR.numGroupAttrs +
		   op -> u.GROUP_AGGR.numAggrAttrs);
	
	// Too many grouping attributes?
	if(op -> u.GROUP_AGGR.numGroupAttrs == MAX_GROUP_ATTRS)
		return 0;
	
	op -> u.GROUP_AGGR.groupAttrs
		[ op -> u.GROUP_AGGR.numGroupAttrs ++] = attr;
	
	op -> outAttrs
		[ op -> numOutAttrs ++] = attr;	
	
	return op;
}

// Group by Aggregate (continued)
Operator *Logical::add_aggr (Operator *op, AggrFn fn, Attr aggrAttr)
{
	ASSERT(op -> kind == LO_GROUP_AGGR);
	ASSERT(op -> numOutAttrs ==
		   op -> u.GROUP_AGGR.numGroupAttrs +
		   op -> u.GROUP_AGGR.numAggrAttrs);

	// too many aggregation attributes?
	if(op -> u.GROUP_AGGR.numAggrAttrs == MAX_AGGR_ATTRS)
		return 0;
	
	op -> u.GROUP_AGGR.aggrAttrs
		[ op -> u.GROUP_AGGR.numAggrAttrs ] = aggrAttr;
	op -> u.GROUP_AGGR.fn
		[ op -> u.GROUP_AGGR.numAggrAttrs ] = fn;
	op -> u.GROUP_AGGR.numAggrAttrs ++;
	
	Attr &newOutAttr = op -> outAttrs [ op -> numOutAttrs ];
	
	if(aggrAttr.kind == NAMED) {
		newOutAttr.kind           = AGGR;
		newOutAttr.u.AGGR.varId   = aggrAttr.u.NAMED.varId;
		newOutAttr.u.AGGR.tableId = aggrAttr.u.NAMED.tableId;
		newOutAttr.u.AGGR.attrId  = aggrAttr.u.NAMED.attrId;
		newOutAttr.u.AGGR.fn      = fn;
	}
	
	else {
		newOutAttr.kind = UNNAMED;
		newOutAttr.u.UNNAMED.type
			= getOutputType(fn, aggrAttr.u.UNNAMED.type);
	}
	
	op -> numOutAttrs ++;
	
	return op;
}

// Distinct operator
Operator *Logical::mk_distinct (Operator *input)
{
	Operator *op = newop (LO_DISTINCT);
	
	if (!op) return 0;
	
	ASSERT (input);
	
	op -> bStream = input -> bStream;
	
	// Outputschema = input schema
	copy_schema (op, input);

	op -> numInputs = 1;
	op -> inputs [0] = input;
	
	input -> output = op;	

	return op;
}

// Istream
Operator *Logical::mk_istream(Operator *input)
{
	Operator *op = newop(LO_ISTREAM);
	
	if(!op) return 0;
	
	ASSERT(input);
	
	// Output is a stream by defn.
	op -> bStream = true;
	
	// Outputschema = input schema
	copy_schema (op, input);
	
	op -> numInputs = 1;
	op -> inputs [0] = input;
	
	input -> output = op;	

	return op;
}

// Dstream
Operator *Logical::mk_dstream(Operator *input)
{
	Operator *op = newop(LO_DSTREAM);

	if(!op) return 0;	

	// Output is a stream by defn.
	op -> bStream = true;
	
	// Output schema = input schema
	copy_schema (op, input);
	
	op -> numInputs = 1;
	op -> inputs [0] = input;
	
	input -> output = op;	
	
	return op;
}

// Rstream
Operator *Logical::mk_rstream(Operator *input)
{
	Operator *op = newop(LO_RSTREAM);
	
	if(!op) return 0;
	
	// Output is a stram by defn.
	op -> bStream = true;
	
	// Output schema = input schema
	copy_schema (op, input);
	
	op -> numInputs = 1;
	op -> inputs [0] = input;
	
	input -> output = op;	
	
	return op;
}

// Stream Cross product
Operator *Logical::mk_stream_cross (Operator *streamInput)
{
	Operator *op = newop(LO_STREAM_CROSS);
	
	if (!op) return 0;
	
	// We want an input producing a stream.
	ASSERT (streamInput);
	ASSERT (streamInput -> bStream);
	
	// Output is a stream by defn.
	op -> bStream = true;
	
	// Currently the is the schema of the streamInput.  This will get
	// expanded in the stream_cross_add_rel_input function.
	op -> numOutAttrs = streamInput -> numOutAttrs;
	for (unsigned int a = 0 ; a < op -> numOutAttrs ; a++) 
		op -> outAttrs [a] = streamInput -> outAttrs [a];
	
	op -> numInputs = 1;
	op -> inputs [0] = streamInput;
	
	streamInput -> output = op;
	
	return op;
}

// Stream Cross product (continued)
Operator *Logical::stream_cross_add_input (Operator *op, 
										   Operator *input)
{	
	ASSERT (op -> kind == LO_STREAM_CROSS);
	
	// Too many inputs
	if (op -> numInputs == MAX_INPUT_OPS)
		return 0;
	
	// Add the new input.
	op -> inputs [ op -> numInputs ++] = input;	
	input -> output = op;
	
	// Too many attributes?
	if (op -> numOutAttrs + input -> numOutAttrs > MAX_ATTRS)
		return 0;
	
	// Update the schema.
	for (unsigned int a = 0 ; a < input -> numOutAttrs ; a++) {
		op -> outAttrs [ op -> numOutAttrs ++ ] 
			= input -> outAttrs [a];
	}
	
	return op;
}

static Type getAttrType (Attr attr)
{
	Type type;
	Type aggrAttrType;
	switch (attr.kind) {
	case NAMED:
		type = tableMgr -> getAttrType (attr.u.NAMED.tableId,
										attr.u.NAMED.attrId);
		break;
		
	case AGGR:
		aggrAttrType = tableMgr -> getAttrType (attr.u.AGGR.tableId,
												attr.u.AGGR.attrId);
		
		type = getOutputType (attr.u.AGGR.fn,
							  aggrAttrType);
		break;
		
	case UNNAMED:
		type = attr.u.UNNAMED.type;
		break;
		
	default:
		ASSERT (0);
		break;
	}
	
	return type;
}

// Union
Operator *Logical::mk_union (Operator *left, Operator *right)
{
	Operator *op = newop (LO_UNION);
	
	if (!op) return 0;

	// Output is a stream only if both inputs are streams
	op -> bStream = (left -> bStream && right -> bStream);
	
	// Schema is the schema of the left input (whcih should be identical
	// to the schema of the right input
	op -> numOutAttrs = left -> numOutAttrs;
	for (unsigned int a = 0 ; a < op -> numOutAttrs ; a++) {
		op -> outAttrs [a].kind = UNNAMED;
		op -> outAttrs [a].u.UNNAMED.type =
			getAttrType (left -> outAttrs [a]);
	}
	
	op -> numInputs = 2;
	op -> inputs [0] = left;
	op -> inputs [1] = right;
	
	op -> output = 0;
	
	left -> output = op;
	right -> output = op;
	
	return op;
}

// Union
Operator *Logical::mk_except (Operator *left, Operator *right)
{
	Operator *op = newop (LO_EXCEPT);
	
	if (!op) return 0;
	
	// Output is a always a relation (well, unless the right input only
	// produces minuses)
	op -> bStream = false;
	
	// Schema is the schema of the left input (whcih should be identical
	// to the schema of the right input
	op -> numOutAttrs = left -> numOutAttrs;
	for (unsigned int a = 0 ; a < op -> numOutAttrs ; a++) {
		op -> outAttrs [a].kind = UNNAMED;
		op -> outAttrs [a].u.UNNAMED.type =
			getAttrType (left -> outAttrs [a]);
	}
	
	op -> numInputs = 2;
	op -> inputs [0] = left;
	op -> inputs [1] = right;
	
	op -> output = 0;
	
	left -> output = op;
	right -> output = op;
	
	return op;
}

int Logical::update_schema_recursive (Operator *op) 
{
	ASSERT (op);
	
	while (op) {
		switch (op -> kind) {
		case LO_PROJECT:
		case LO_GROUP_AGGR:
		case LO_STREAM_SOURCE:
		case LO_RELN_SOURCE:
			return 0;
			
		case LO_ROW_WIN:
		case LO_RANGE_WIN:
		case LO_NOW_WIN:
		case LO_PARTN_WIN:
		case LO_SELECT:
		case LO_DISTINCT:
		case LO_ISTREAM:
		case LO_DSTREAM:
		case LO_RSTREAM:				
		case LO_CROSS:
		case LO_STREAM_CROSS:
			op -> numOutAttrs = 0;
			for (unsigned int i = 0 ; i < op -> numInputs ; i++) {
				ASSERT (op -> inputs [i]);
				append_schema (op, op -> inputs [i]);
			}
			break;

		case LO_UNION:
		case LO_EXCEPT:
			// We cannot change the schema of just one input
			return -1;
			
		default:
			return -1;
		}
		
		op = op -> output;
	}
	
	return 0;
}

int Logical::update_stream_property_recursive (Operator *op)
{
	ASSERT (op);

	while (op) {
		switch (op -> kind) {
		case LO_GROUP_AGGR:
		case LO_STREAM_SOURCE:
		case LO_RELN_SOURCE:
		case LO_ISTREAM:
		case LO_DSTREAM:
		case LO_RSTREAM:
		case LO_ROW_WIN:
		case LO_RANGE_WIN:
		case LO_NOW_WIN:
		case LO_PARTN_WIN:
		case LO_STREAM_CROSS:
		case LO_EXCEPT:						
			return 0;
			
		case LO_PROJECT:			
		case LO_SELECT:
		case LO_DISTINCT:
			ASSERT (op -> inputs [0]);
			op -> bStream = op -> inputs [0] -> bStream;
			break;
			
		case LO_CROSS:
			op -> bStream = true;
			for (unsigned int i = 0 ; i < op -> numInputs ; i++) {
				ASSERT (op -> inputs [i]);
				if (!op -> inputs [i] -> bStream) {
					op -> bStream = false;
					break;
				}
			}
			break;
			
		case LO_UNION:
			op -> bStream = (op -> inputs [0] -> bStream &&
							 op -> inputs [1] -> bStream);
			break;			
			
		default:
			return -1;
		}
		
		op = op -> output;
	}
	
	return 0;
}

static bool operator == (const Attr& attr1, const Attr& attr2)
{
	if (attr1.kind != attr2.kind)
		return false;
	
	switch (attr1.kind) {
	case NAMED:
		
		return ((attr1.u.NAMED.varId == attr2.u.NAMED.varId)     &&
				(attr1.u.NAMED.tableId == attr2.u.NAMED.tableId) &&
				(attr1.u.NAMED.attrId == attr2.u.NAMED.attrId));
		
	case AGGR:
		
		return ((attr1.u.AGGR.varId == attr2.u.AGGR.varId)       &&
				(attr1.u.AGGR.tableId == attr2.u.AGGR.tableId)   &&
				(attr1.u.AGGR.attrId == attr2.u.AGGR.attrId)     &&
				(attr1.u.AGGR.fn == attr2.u.AGGR.fn));
		
	case UNNAMED:
	default:
		return false;		
	}

	// never comes
	return false;
}


bool Logical::check_reference (const Attr &attr, const Operator *op)
{		
	for (unsigned int a = 0 ; a < op -> numOutAttrs ; a++)
		if (attr == op -> outAttrs [a])
			return true;
	
	return false;	
}

bool Logical::check_reference (const Expr *expr, const Operator *op)
{
	switch (expr -> kind) {
	case CONST_VAL:
		
		return true;
		
	case ATTR_REF:
		
		return check_reference (expr -> u.attr, op);
		
	case COMP_EXPR:
		
		if (!check_reference (expr -> u.COMP_EXPR.left, op))
			return false;
		
		return check_reference (expr -> u.COMP_EXPR.right, op);
		
	default:
		
		break;
	}
	
	return false;
}
