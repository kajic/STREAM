 #ifndef _TUPLE_LAYOUT_
#include "metadata/tuple_layout.h"
#endif

#ifndef _DEBUG_
#include "common/debug.h"
#endif

#ifndef _TUPLE_
#include "execution/internals/tuple.h"
#endif

#include <string.h>

using namespace Metadata;

TupleLayout::TupleLayout ()
{
	numAttrs = 0;
	numBytes = 0;
}

TupleLayout::TupleLayout (Physical::Operator *op)
{
	int rc;

	numAttrs = 0;
	numBytes = 0;
	
	ASSERT (op);	
	for (unsigned int a = 0 ; a < op -> numAttrs ; a++) {
		rc = addAttr (op -> attrTypes [a], op -> attrLen [a], cols [a]);
		ASSERT (rc == 0);
	}
}
					  
TupleLayout::~TupleLayout () {}

int TupleLayout::addAttr (Type type, unsigned int len, unsigned int &col)
{
	ASSERT (numAttrs <= MAX_ATTRS);

	// We are out of resources
	if (numAttrs == MAX_ATTRS)
		return -1;

	if (type == CHAR) {
		cols [numAttrs ++] = col = numBytes;
		numBytes += len;
	}

	else {
		return addFixedLenAttr (type, col);
	}

	return 0;
}

int TupleLayout::addFixedLenAttr (Type type, unsigned int &col)								  
{
	ASSERT (numAttrs <= MAX_ATTRS);

	// We are out of resources
	if (numAttrs == MAX_ATTRS)
		return -1;	
	
	switch (type) {
	case INT:
		
		if (numBytes % sizeof (int) == 0)
			col = numBytes / sizeof (int);
		else
			col = (numBytes / sizeof (int)) + 1;
		numBytes = (col + 1) * sizeof (int);		
		break;
		
	case FLOAT:
		
		if (numBytes % sizeof (float) == 0)
			col = numBytes / sizeof (float);
		else
			col = (numBytes / sizeof (float)) + 1;
		numBytes = (col + 1) * sizeof (float);
		break;
		
		
	case BYTE:
		col = numBytes;
		numBytes ++;
		break;
		
#ifdef _DM_				
	default:
		ASSERT (0);
		break;
#endif
	}
	
	cols [numAttrs ++] = col;
	
	return 0;
}
	
int TupleLayout::addCharPtrAttr (unsigned int &col)
{
	ASSERT (numAttrs <= MAX_ATTRS);

	// We are out of resources
	if (numAttrs == MAX_ATTRS)
		return -1;
	
	if (numBytes % sizeof (char *) == 0)
		col = numBytes / sizeof (char *);
	else
		col = numBytes / sizeof (char *) + 1;
	numBytes = (col + 1) * sizeof (char *);
	
	return 0;	
}

int TupleLayout::addTimestampAttr (unsigned int &col)
{
	ASSERT (numAttrs <= MAX_ATTRS);
	
	// We are out of resources
	if (numAttrs == MAX_ATTRS)
		return -1;
	
	if (numBytes % sizeof (Timestamp) == 0)
		col = numBytes / sizeof (Timestamp);
	else
		col = numBytes / sizeof (Timestamp) + 1;
	numBytes = (col + 1) * sizeof (Timestamp);
	
	return 0;	
}

unsigned int TupleLayout::getTupleLen () const
{	
	if (numBytes % TUPLE_ALIGN == 0)
		return numBytes;
	else
		return numBytes + TUPLE_ALIGN - numBytes % TUPLE_ALIGN;
}

unsigned int TupleLayout::getColumn (unsigned int pos) const
{
	ASSERT (pos < numAttrs);

	return cols [pos];
}

int TupleLayout::getOpColumn (Physical::Operator *op, unsigned int pos,
							  unsigned int &col)
{
	ASSERT (op);
	
	TupleLayout *tupleLayout = new TupleLayout (op);
	col = tupleLayout -> getColumn (pos);
	delete tupleLayout;

	return 0;
}

int TupleLayout::getOpTupleLen (Physical::Operator *op, unsigned int &len)
{
	ASSERT (op);
	TupleLayout *tupleLayout = new TupleLayout (op);
	len = tupleLayout -> getTupleLen ();
	delete tupleLayout;

	return 0;
}

ConstTupleLayout::ConstTupleLayout ()
{
	numAttrs = 0;
	tupleLayout = new TupleLayout ();
}

ConstTupleLayout::~ConstTupleLayout ()
{
	delete tupleLayout;
}

int ConstTupleLayout::addInt (int ival, unsigned int &col)
{
	ASSERT (numAttrs <= MAX_ATTRS);

	if (numAttrs == MAX_ATTRS)
		return -1;

	val [ numAttrs ].ival = ival;
	attrTypes [ numAttrs ] = INT;
	numAttrs++;
	
	return tupleLayout -> addFixedLenAttr (INT, col);
}

int ConstTupleLayout::addFloat (float fval, unsigned int &col)
{
	ASSERT (numAttrs <= MAX_ATTRS);

	if (numAttrs == MAX_ATTRS)
		return -1;

	val [numAttrs].fval = fval;
	attrTypes [numAttrs] = FLOAT;
	numAttrs ++;
	
	return tupleLayout -> addFixedLenAttr (FLOAT, col);
}

int ConstTupleLayout::addChar (const char *sval, unsigned int &col)
{
   	ASSERT (numAttrs <= MAX_ATTRS);

	if (numAttrs == MAX_ATTRS)
		return -1;

	val [numAttrs].sval = sval;
	attrTypes [numAttrs] = CHAR;
	numAttrs ++;
	
	return tupleLayout -> addAttr (CHAR, strlen(sval)+1, col);
}

int ConstTupleLayout::addByte (char bval, unsigned int &col)
{
   	ASSERT (numAttrs <= MAX_ATTRS);

	if (numAttrs == MAX_ATTRS)
		return -1;

	val [numAttrs].bval = bval;
	attrTypes [numAttrs] = BYTE;
	numAttrs++;
	
	return tupleLayout -> addFixedLenAttr (BYTE, col);
}

unsigned int ConstTupleLayout::getTupleLen () const
{
	return tupleLayout -> getTupleLen ();
}

int ConstTupleLayout::genTuple (char *tuple) const
{
	ASSERT (tuple);
	
	for (unsigned int a = 0 ; a < numAttrs ; a++) {		
		switch (attrTypes [a]) {
		case INT:
			ICOL (tuple, tupleLayout -> getColumn (a)) =
				val [a].ival;
			break;

		case FLOAT:
			FCOL (tuple, tupleLayout -> getColumn (a)) =
				val [a].fval;
			break;

		case CHAR:
			strcpy (CCOL (tuple, tupleLayout -> getColumn(a)),
					val [a].sval);
			break;
			
		case BYTE:
			BCOL(tuple, tupleLayout -> getColumn (a)) = val [a].bval;
			break;
			
#ifdef _DM_
		default:
			ASSERT (0);
			break;
#endif
		}
	}
	
	return 0;
}

unsigned int TupleLayout::TUPLE_ALIGN = getTupleAlign ();

// Quick & dirty
unsigned int TupleLayout::getTupleAlign ()
{
	unsigned int a;
	
	for (a = 1 ; a  < 20 ; a++) {
		if ((a % sizeof (int) == 0) &&
			(a % sizeof (float) == 0) &&
			(a % sizeof (char *) == 0) &&
			(a % sizeof (Timestamp) == 0))
			return a;
	}
	
	ASSERT (0);
	return 8;
}
