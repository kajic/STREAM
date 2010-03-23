#ifndef _QUERY_OUTPUT_
#define _QUERY_OUTPUT_

/**
 * @file      query_output.h
 * @date      May 22, 2004
 * @brief     Defines the abstract class which is used to output query
 *            results by the STREAM server.
 */

#ifndef _TYPES_
#include "common/types.h"
#endif

namespace Interface {

/**
 * Interface  that  the  STREAM  server  uses  to  produce  query  output.
 * Different kinds of outputs (e.g., write  to a file, write to a network)
 * can be obtained by extending this class in appropriate ways.
 * 
 * QueryOutput supports the method putNext(), which is used by teh server
 * to push the next output tuple to the output.
 *
 * Before pushing any tuples to the output, the server first indicates the
 * schema of the output tuples using setNumAttrs & setAttrInfo method
 * calls.  The method start() is called before the first putNext call:
 * this cen be used by the object to perform various initializations,
 * resources allocation etc.  
 */
	class QueryOutput {
	public:
		virtual ~QueryOutput() {}
		
		/**
		 * Set the number of attributes in the schema of the tuples flowing
		 * through this object.  Along with setAttrInfo, used to specify the
		 * schema of the tuples flowing through.
		 *
		 * @param       numAttrs      number of attributes in the schema
		 * @return      0 (success), !0 (otherwise)
		 */	
		virtual int setNumAttrs(unsigned int numAttrs) = 0;
		
		/**
		 * Set the information about one particular attribute in the schema of
		 * the output tuples.
		 *
		 * @param   attrPos           position of the attribute among
		 *                            attributes in the schema
		 * @param   attrType          data type of the attribute
		 * @param   attrLen           Length of the attribute (for strings)
		 * @return                    0 (success), !0 (otherwise)
		 */	
		virtual int setAttrInfo(unsigned int attrPos, 
								Type         attrType,
								unsigned int attrLen) = 0;
		
		/**
		 * Signal that putNext method will be invoked ...
		 *
		 * @return                   0 (everything alright), !0 otherwise
		 */ 
		
		virtual int start() = 0;
	
		/**
		 * The next output in the output, encoded in the input/output tuple
		 * format:  
		 *
		 *  <tuple>       ---> <timestamp> <effect> <dataportion>
		 *  <dataportion> ---> <1st attr> <2nd attr> ...
		 *  <int attr>    ---> encoded in 4 bytes in net order 
		 *  <float attr>  ---> encoded in 4 bytes in net order 
		 *  <byte attr>   ---> 1 byte character
		 *  <string attr> ---> null terminated string [[ Explain padding
		 *  ...]]
		 *  <effect>      ---> integer: 1 (PLUS), 2 (MINUS)
		 *
		 * @param  tuple            encoding of the tuple
		 * @param  len              Length of the encoding
		 * @return                  0 (success), !0 (otherwise)
		 */	
		virtual int putNext(const char *tuple, unsigned int len) = 0;

		/**
		 * signal that no more putNext calls will be invoked
		 *
		 * @retur                  [[ usual convention ]]
		 */	
		virtual int end() = 0;
	};
}

#endif
