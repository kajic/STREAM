#ifndef _GEN_OUTPUT_
#define _GEN_OUTPUT_

#ifndef _QUERY_OUTPUT_
#include "interface/query_output.h"
#endif

#ifndef _TYPES_
#include "common/types.h"
#endif

#ifndef _CONSTANTS_
#include "common/constants.h"
#endif

#include <ostream>

using Interface::QueryOutput;
using std::ostream;

namespace Client {
	
	class GenOutput : public QueryOutput {
	private:
		ostream &out;
		
		/// Number of (data) attributes in the output schema
		int numAttrs;
		
		/// Types of data attributes
		Type attrTypes [MAX_ATTRS];
		
		/// Length of attributes
		int attrLen [MAX_ATTRS];
		
		/// Offsets @ which these are present
		int offsets [MAX_ATTRS];
		
		/// length of tuples we are expecting
		int tupleLen;
		
		/// Offset of the timestamp
		int tstampOffset;
		
		/// Offset of the sign
		int signOffset;
		
	public:
		GenOutput (ostream &out);
		~GenOutput ();
		
		int setNumAttrs (unsigned int numAttrs);
		
		int setAttrInfo (unsigned int attrPos, Type attrType,
						 unsigned attrLen);
		
		int start ();
		int putNext (const char *tuple, unsigned int len);
		int end ();
	};
}

#endif
