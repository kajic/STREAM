#ifndef _FILE_SOURCE_
#define _FILE_SOURCE_

#ifndef _TABLE_SOURCE_
#include "interface/table_source.h"
#endif

#ifndef _CONSTANTS_
#include "common/constants.h"
#endif

#include <fstream>

using Interface::TableSource;

namespace Client {
	
	class FileSource : public TableSource {
	private:
		
		/// Source file input
		std::fstream input;
		
		/// Maximum size of tuples we support
		static const unsigned int MAX_TUPLE_SIZE = 256;
		
		/// Buffer for tuples
		char tupleBuf [MAX_TUPLE_SIZE];
		
		enum Type {
			INT, FLOAT, CHAR, BYTE
		};
		
		/// Types of attributes
		Type attrTypes [MAX_ATTRS];
		
		/// Attr lengthts
		int attrLen [MAX_ATTRS];
		
		/// Offsets of attributes in the tupleBuf
		int offsets [MAX_ATTRS];
		
		/// Number of attributes
		int numAttrs;		

		/// Length of tuples
		int tupleLen;
		
	public:
		FileSource (const char *fileName);
		~FileSource ();
		
		int start ();
		int getNext (char *&tuple, unsigned int &len, bool &isHeartbeta);
		int end ();
		
	private:
		int parseTuple (char *lineBuffer);
		int parseSchema (char *lineBuffer);
		int computeOffsets ();
	};
}

#endif
