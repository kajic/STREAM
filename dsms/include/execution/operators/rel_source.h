#ifndef _REL_SOURCE_
#define _REL_SOURCE_

/**
 * @file         rel_source.h
 * @date         Sept. 8, 2004
 * @brief        Relation source
 */

#include <ostream>

#ifndef _CONSTANTS_
#include "common/constants.h"
#endif

#ifndef _OPERATOR_
#include "execution/operators/operator.h"
#endif

#ifndef _QUEUE_
#include "execution/queues/queue.h"
#endif

#ifndef _TABLE_SOURCE_
#include "interface/table_source.h"
#endif

#ifndef _STORE_ALLOC_
#include "execution/stores/store_alloc.h"
#endif

#ifndef _REL_SYN_
#include "execution/synopses/rel_syn.h"
#endif

#ifndef _EVAL_CONTEXT_
#include "execution/internals/eval_context.h"
#endif

namespace Execution {
	class RelSource : public Operator {
	private:

		/// System-wide unique id
		unsigned int id;
		
		/// System-wide log
		std::ostream &LOG;
		
		/// Output queue
		Queue *outputQueue;
		
		/// Source who is feeding us the input tuples
		Interface::TableSource *source;
		
		/// Storage allocator for the output tuples
		StorageAlloc *store;
		
		/// The current state of the relation.  This is used to generate
		/// MINUS elements for the relation
		RelationSynopsis *rel;
		
		/// A scan that returns all tuples identical in all attributes to
		/// a given tuple
		unsigned int scanId;
		
		/// Evaluation context for performing the computations
		EvalContext *evalContext;
		
		/// Timestamp of the last output element
		Timestamp lastOutputTs;

		/// Timestamp of the last input element
		Timestamp lastInputTs;
		
		/// [[ Explanation ]]
		Tuple minusTuple;
		
		//--------------------------------------------------------------------
		// Schema related ...
		//--------------------------------------------------------------------
		
		/// Schema of the input tuples
		struct {
			Type type;
			unsigned int len;
		} attrs [MAX_ATTRS];
		
		/// Number of attributes in the input schema
		unsigned int numAttrs;
		
		/// Mapping of "external" attributes to "internal" columns.
		Column outCols [MAX_ATTRS];
		
		//---------------------------------------------------------------------
		// Encoding related ...
		//---------------------------------------------------------------------
		
		/// Offsets of various attributes in the input tuples
		unsigned int offsets [MAX_ATTRS];
		
		/// Offset at which the timestamp is encoded 
		static const unsigned int TIMESTAMP_OFFSET = 0;
		
		/// Offset at which "sign" (PLUS/MINUS) is encoded
		static const unsigned int SIGN_OFFSET = TIMESTAMP_SIZE;
		
		/// Offset at which the data is encoded (sign is 1 byte)
		static const unsigned int DATA_OFFSET = TIMESTAMP_SIZE + 1;
		
		/// Char that represents a PLUS in the encoding of an input tuple
		static const char PLUS = '+';
		
		/// Char that represents a MINUS in the encoding of an input tuple
		static const char MINUS = '-';			

		static const unsigned int INPUT_ROLE = 2;		
		
	public:
		
		RelSource (unsigned int id, std::ostream &LOG);
		virtual ~RelSource ();
		
		//---------------------------------------------------------------------
		// Initialization routines
		//---------------------------------------------------------------------
		
		int setOutputQueue (Queue *outputQueue);
		int setSource (Interface::TableSource *source);
		int setStore (StorageAlloc *store);
		int setSynopsis (RelationSynopsis *syn);
		int setScan (unsigned int scanId);
		int setEvalContext (EvalContext *evalContext);
		int setMinusTuple (Tuple tuple);
		int addAttr (Type type, unsigned int len, Column outCol);
		int initialize ();
		int run (TimeSlice timeSlice);

	private:

		int decodeAttrs (char *inTuple, Tuple outTuple);
		int getSynTuple (Tuple inTuple, Tuple &outTuple);
	};
}


#endif
