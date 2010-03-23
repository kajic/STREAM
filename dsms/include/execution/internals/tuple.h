#ifndef _TUPLE_
#define _TUPLE_

namespace Execution {
	
	/**
	 * A Column is an attribute within a tuple.  In the implementation a
	 * column is just an unsigned integer.  See the column-access macros
	 * below to understand the semantics of column values.  An integer
	 * column with value 1 points to a location 4 bytes from the beginning
	 * of the tupl, while a byte column with value 1 points to a location
	 * 1 byte from the beginning of the tuple.
	 */	
	typedef unsigned int Column;
	
	/**
	 * A tuple - just a char * to the location storing the contents.
	 */
	typedef char *Tuple;
}

// Routines to access columns within tuples

#define ICOL(t,c)   ( ( (int *)   (t) ) [c] )
#define FCOL(t,c)   ( ( (float *) (t) ) [c] )
#define BCOL(t,c)   ( (t) [c] )
#define CCOL(t,c)   (t + c)

#endif
