#ifndef _PHY_OP_
#define _PHY_OP_

/**
 * @file     phy_op.h
 * @date     Aug. 21, 2004
 * @brief    Representation of physical operators in the system.
 */ 

#ifndef _CONSTANTS_
#include "common/constants.h"
#endif

#ifndef _TYPES_
#include "common/types.h"
#endif

#ifndef _AGGR_
#include "common/aggr.h"
#endif

#ifndef _OP_
#include "common/op.h"
#endif

#ifndef _OPERATOR_
#include "execution/operators/operator.h"
#endif



namespace Physical {
	
	/**
	 * Representation of an attribute in a physical operator.  The member
	 * "input" identifies the input operator that produces the attribute,
	 * and "pos" identifies the position of the attribute in the output
	 * schema of the (input) operator.
	 */
	
	struct Attr {
		unsigned int input;
		unsigned int pos;
	};		
	
	/**
	 * Three kinds of expressions: constant values, attribute references,
	 * and complex expressions built using arithmetic operators.  Same
	 * as Logical::ExprKind.
	 */
	
	enum ExprKind {
		CONST_VAL, ATTR_REF, COMP_EXPR
	};
	
	/**
	 * Same as Logical::Expression, but now the definition of an attribute
	 * is different.
	 */
	struct Expr {
		ExprKind   kind;
		
		// output type of the expression
		Type       type; 
		
		union {
			int         ival; // type == INT && kind == CONST_VAL
			float       fval; // type == FLOAT && kind == CONST_VAL
			const char *sval; // type == CHAR && kind == CONST_VAL
			char        bval; // type == BYTE && kind == CONST_VAL
			
			struct {
				ArithOp  op;
				Expr    *left;
				Expr    *right;
			} COMP_EXPR;
			
			Attr   attr;
		} u;
	};
	
	struct BExpr {
		CompOp      op;
		Expr       *left;
		Expr       *right;
		BExpr      *next;  // next predicate in the conjunction
	};
	
	enum OperatorKind {
		// Selection
		PO_SELECT,
		
		// Projection
		PO_PROJECT,
		
		// Relation-Relation join
		PO_JOIN,
		
		// Stream-relation join
		PO_STR_JOIN,
		
		// Combinatin of a join followed by project
		PO_JOIN_PROJECT,

		// Combination of a str-join followed by project
		PO_STR_JOIN_PROJECT,
		
		// Group-by aggregation
		PO_GROUP_AGGR,
		
		// distinct 
		PO_DISTINCT,
		
		// row window operator
		PO_ROW_WIN,
		
		// range window op
		PO_RANGE_WIN,
				
		// partition window
		PO_PARTN_WIN,
		
		// Istream
		PO_ISTREAM,
		
		// Dstream
		PO_DSTREAM,
		
		// Rstream
		PO_RSTREAM,

		// Union
		PO_UNION,

		// Anti-semijoin
		PO_EXCEPT,
		
		// Source for a base stream
		PO_STREAM_SOURCE,
		
		// Base relation source
		PO_RELN_SOURCE,
		
		// "No-op" operator that sits on top of query views - other
		// queries that use the output of this query read from the query
		// source operator.  This is a dummy operator found at the
		// metadata level - when we generate the actual exec. operators we
		// will bypass it.
		PO_QUERY_SOURCE,
		
		// Output op that interfaces with the external world
		PO_OUTPUT,
		
		// Sink operator to consume tuples from unused ops
		PO_SINK,

		// System stream generator
		PO_SS_GEN
	};	
	
	// Maximum number of inputs to an operator: we only consider unary and
	// binary operators now ...
	static const unsigned int MAX_IN_BRANCHING = 2;
	static const unsigned int MAX_GROUP_ATTRS = 10;
	static const unsigned int MAX_AGGR_ATTRS  = 10;

	// forward decl.
	struct Synopsis;
	struct Store;
	struct Queue;
	
	struct Operator {
		
		/// indexes array PlanManagerImpl.ops
		unsigned int id;
		
		/// next & prev pointers to arrange the operators as a linked list
		/// for resource management purposes
		Operator *next;
		Operator *prev;
		
		/// Type of operator
		OperatorKind  kind;
		
		/// Number of attributes in the output schema of the operator
		unsigned int numAttrs;
		
		/// Types of the various attributes in the output schema
		Type  attrTypes [MAX_ATTRS];
		
		/// Length of string attributes in the output schema
		unsigned int attrLen [MAX_ATTRS];
		
		/// Does the operator produce a stream
		bool bStream;
		
		/// Operators reading off from this operator
		Operator *outputs [MAX_OUT_BRANCHING];
		
		/// number of output operators
		unsigned int numOutputs;
		
		/// Operators which form the input to this operator
		Operator *inputs  [MAX_IN_BRANCHING];
		
		/// number of input operators
		unsigned int numInputs;
		
		/// Stores that allocate tuples in various inputs
		Store *inStores [MAX_IN_BRANCHING];		
		
		/// Store for the allocation of space for the output tuples.
		Store *store;
		
		/// Input queues for each input
		Queue *inQueues [MAX_IN_BRANCHING];
		
		/// The (single) output queue
		Queue *outQueue;		
		
		/// Instantiated operator
		Execution::Operator *instOp;
		
		union {
			
			struct {
				// Id assigned by tableMgr
				unsigned int strId; 
				
				// Id of the tableSource operator
				unsigned int srcId;
			} STREAM_SOURCE;
			
			struct {
				// Id assigned by tableMgr
				unsigned int relId; 
				
				// Id of the tableSource operator
				unsigned int srcId;
				
				// Synopsis of the relation (used to generate MINUS tuples)
				Synopsis *outSyn;
				
			} RELN_SOURCE;
			
			struct {
				unsigned int outputId; // [[ Explanation ]]
				unsigned int queryId;
			} OUTPUT;
			
			struct {
				BExpr       *pred;
			} SELECT;
			
			struct {
				// Synopsis for out: required if project produces MINUS
				// tuples: (bStream == false)
				Synopsis *outSyn;
				
				// project expressions
				Expr  *projs [MAX_ATTRS];
			} PROJECT;
			
			struct {
				
				// My   output   schema    is   concatenation   of   first
				// numOuterAttrs  from left  input and  numInnerAttrs from
				// right  input. (Assert(numOuterAttrs +  numInnerAttrs ==
				// numAttrs).  We  store this information  at construction
				// time since the schemas of the input operators can later
				// "expand"
				
				unsigned int numOuterAttrs;				
				unsigned int numInnerAttrs;
				
				// Join predicate
				BExpr *pred;
				
				// Synopsis for the inner relation
				Synopsis *innerSyn;
				
				// Synopsis for the outer relation
				Synopsis *outerSyn;
				
				// Synopsis for output (required to generate MINUS elements)				
				Synopsis *joinSyn;
			} JOIN;
			
			struct {
				// My output schema is concatenation of first
				// numOuterAttrs from left input and numInnerAttrs from
				// right input. (Assert(numOuterAttrs + numInnerAttrs ==
				// numAttrs)
				
				unsigned int numOuterAttrs;				
				unsigned int numInnerAttrs;
				
				// Join predicate
				BExpr *pred;
				
				// Synopsis for the inner relation
				Synopsis *innerSyn;
			} STR_JOIN;
			
			struct {
				// Project expressions
				Expr *projs [MAX_ATTRS];
				
				// Join predicate
				BExpr *pred;
				
				// Synopsis for the inner relation
				Synopsis *innerSyn;
				
				// Synopsis for the outer relation
				Synopsis *outerSyn;
				
				// Synopsis for output (required to generate MINUS elements)
				Synopsis *joinSyn;
			} JOIN_PROJECT;
			
			struct {
				// Output projections
				Expr *projs [MAX_ATTRS];
				
				// Join predicate
				BExpr *pred;
				
				// Synopsis for inner
				Synopsis *innerSyn;
			} STR_JOIN_PROJECT;
			
			struct {
				// grouping attributes
				Attr         groupAttrs [MAX_GROUP_ATTRS];
				
				// aggregated attributes
				Attr         aggrAttrs [MAX_AGGR_ATTRS];
				
				// aggregation function
				AggrFn       fn [MAX_AGGR_ATTRS];
				
				// ...
				unsigned int numGroupAttrs;
				
				// ...
				unsigned int numAggrAttrs;
				
				// Synopsis for input
			    Synopsis *inSyn;
				
				// Synopsis for output
				Synopsis *outSyn;
			} GROUP_AGGR;
			
			struct {
				
				Synopsis *outSyn;
				
			} DISTINCT;
			
			struct {
				// Window size
				unsigned int numRows;
				
				// Synopsis for the window
				Synopsis *winSyn;
			} ROW_WIN;
			
			struct {
                // window stride
                unsigned int strideUnits;
                
				// window size
				unsigned int timeUnits;
				
				// Synopsis for the window
				Synopsis *winSyn;
			} RANGE_WIN;
			
			struct {
				// Partitioning attributes
				Attr partnAttrs [MAX_GROUP_ATTRS];
				
				// ...
				unsigned int numPartnAttrs;
				
				// Window size
				unsigned int numRows;
				
				// Synopsis for the window
			    Synopsis *winSyn;
				
			} PARTN_WIN;
			
			struct {
				// Synopsis for elements with current timestamp [[Explain]]
				Synopsis *nowSyn;

				// Store for the tuples in nowsyn
				Store *nowStore;
			} ISTREAM;
			
			struct {
				// Synopsis for elements with current timestamp [[Explain]]
				Synopsis *nowSyn;

				// Store for the tuples in nowsyn
				Store *nowStore;
			} DSTREAM;
			
			struct {
				// Synopsis of the input relation
				Synopsis *inSyn;
			} RSTREAM;

			struct {
				// Optional synopsis if one of the inputs is a relation.
				Synopsis *outSyn;
			} UNION;
			
			struct {
				// Internal Synopsis
				Synopsis *countSyn;
				
				// Output lineage synopsis
				Synopsis *outSyn;
				
				// Store for countSyn
				Store *countStore;
			} EXCEPT;
			
			struct {				
				Store *outStores [MAX_OUT_BRANCHING];
				Queue *outQueues [MAX_OUT_BRANCHING];
				unsigned int numOutput;
			} SS_GEN;
		} u;
	};
}

#endif
