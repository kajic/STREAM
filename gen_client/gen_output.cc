#ifndef _GEN_OUTPUT_
#include "gen_output.h"
#endif

#ifndef _DEBUG_
#include "common/debug.h"
#endif

using Client::GenOutput;

GenOutput::GenOutput (ostream &_out)
	: out (_out)
{
	numAttrs = 0;
}

GenOutput::~GenOutput () {}

int GenOutput::setNumAttrs (unsigned int numAttrs)
{
	if (numAttrs > MAX_ATTRS)
		return -1;
	
	this -> numAttrs = numAttrs;
	return 0;
}

int GenOutput::setAttrInfo (unsigned int attrPos,
							Type attrType, unsigned len)
{
	if ((int)attrPos >= numAttrs)
		return -1;

	attrTypes [attrPos] = attrType;
	attrLen [attrPos] = len;

	return 0;
}

int GenOutput::start ()
{
	int offset = 0;
	
	if (numAttrs == 0)
		return -1;


	tstampOffset = offset;
	offset += TIMESTAMP_SIZE;

	signOffset = offset;
	offset += 1;
	
	for (int a = 0 ; a < numAttrs ; a++) {

		offsets [a] = offset;

		switch (attrTypes [a]) {
		case INT:
			offset += INT_SIZE;
			break;

		case FLOAT:
			offset += FLOAT_SIZE;
			break;

		case CHAR:
			offset += attrLen [a];
			break;

		case BYTE:
			offset ++;
			break;

		default:
			return -1;
		}
	}

	tupleLen = offset;
	
	return 0;
}

int GenOutput::putNext (const char *tuple, unsigned int len)
{
	Timestamp timestamp;
	int ival;
	float fval;
	
	if ((int)len < tupleLen)
		return -1;
	
	memcpy (&timestamp, tuple + tstampOffset, TIMESTAMP_SIZE);
	out << "[" << timestamp << "]";
	out << ":" << tuple [signOffset] << ":";
	
	for (int a = 0 ; a < numAttrs ; a++) {
		switch (attrTypes [a]) {
		case INT:
			memcpy (&ival, tuple + offsets [a], sizeof(int));
			out << ival;
			break;
			
		case FLOAT:
			memcpy (&fval, tuple + offsets [a], sizeof(float));
			out << fval;
			break;
			
		case CHAR:
			out << tuple + offsets [a];
			break;
			
		case BYTE:
			out << tuple [offsets [a]];
			break;
			
		default:
			ASSERT (0);
			break;
		}
		
		if (a < numAttrs - 1)
			out << ", ";
	}
	
	out << std::endl;
	
	return 0;
}
	
int GenOutput::end () {
	return 0;
}
