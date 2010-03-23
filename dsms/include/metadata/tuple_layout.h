#ifndef _TUPLE_LAYOUT_
#define _TUPLE_LAYOUT_

/**
 * @file        tuple_layout.h
 * @date        Sept. 16, 2004
 * @brief       Determine the layout of tuples
 */

#ifndef _PHY_OP_
#include "metadata/phy_op.h"
#endif

namespace Metadata {

	/**
	 * TupleLayout class.  This class is used during instantiation of
	 * operators, stores, and other executable entities.  It determines
	 * the actual layout of tuples given a schema for the tuples.
	 *
	 * The tuple layout produced by this class has the *prefix* property:
	 * if one schema is a prefix of another, then the common attributes
	 * all have the same position in the layout.
	 *
	 * A tuple layout object can be used in an online fashion - adding new
	 * attributes (add* methods) and determining the positions (columns)
	 * of these attributes, or in an offline fashion - constructing the
	 * tuple layout object for the schema produced by an operator, and
	 * then determining the columns of each attribute of the operator
	 * (getColumn method).
	 */
	
	class TupleLayout {
	private:
		
		/// Tuple lengths can only be multiple of TUPLE_ALIGN
		static unsigned int TUPLE_ALIGN;
		
		/// Columns of attributes
		unsigned int cols [MAX_ATTRS];

		/// Number of attributes in the current schema
		unsigned int numAttrs;
		
		/// Number of bytes used by the current set of attributes
		unsigned int numBytes;
		
	public:
		
		/**
		 * Tuple layout object with an empty schema.
		 */ 
		TupleLayout ();		
		
		/**
		 * Determine the tuple layout for the schema produced by operator
		 * op.		
		 */ 		
		TupleLayout (Physical::Operator *op);

		~TupleLayout();
		
		/**
		 * Add an attribute to the existing schema of the tuple layout
		 * object.  Return the column of the newly added attribute
		 */		
		int addAttr (Type type, unsigned int len, unsigned int &col);
		
		/**
		 * Add a fixed length attribute to the existing schema.  Return
		 * the column of the newly added attribute
		 */		
		int addFixedLenAttr (Type type, unsigned int &col);
		
		/**
		 * Add a char pointer attribute.  Note that the char pointer is
		 * not a string - it refers to the actual (4 byte) pointer.
		 */
		int addCharPtrAttr (unsigned int &col);
		
		/**
		 * Add a timestamp attribute to the current schema & return the
		 * column of the attribute 
		 */		
		int addTimestampAttr (unsigned int &col);
		
		/**
		 * Get the length of tuples of the current schema
		 */		
		unsigned int getTupleLen() const;
		
		/**
		 * Get the column of a particular attribute in the current schema
		 */		
		unsigned int getColumn (unsigned int pos) const;
		
		/**
		 * Get the column of a particular attribute of an operator
		 */
		static int getOpColumn (Physical::Operator *op, unsigned int pos,
								unsigned int &col);
		/**
		 * Get the tuple length of the output tuples of an operator
		 */		
		static int getOpTupleLen (Physical::Operator *op,
								  unsigned int &len);								  
		
	private:		
		static unsigned int getTupleAlign ();
	};
	
	class ConstTupleLayout {
	private:

		unsigned int numAttrs;
		
		union {
			int   ival;
			float fval;
			char  bval;
			const char *sval;			
		} val [MAX_ATTRS];
		
		Type attrTypes [MAX_ATTRS];
		
		TupleLayout *tupleLayout;
		
	public:
		ConstTupleLayout ();
		~ConstTupleLayout ();				
		
		int addInt (int ival, unsigned int &col);
		int addFloat (float fval, unsigned int &col);
		int addChar (const char *sval, unsigned int &col);
		int addByte (char bval, unsigned int &col);
		
		unsigned int getTupleLen () const;
		int genTuple (char *tuple) const;
	};
}

#endif
