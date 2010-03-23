#ifndef _FILE_SOURCE_
#include "file_source.h"
#endif

#ifdef _DM_
#include <assert.h>
#define ASSERT(x) assert(x)
#else
#define ASSERT(x) {}
#endif


#include <iostream>
using namespace std;

static const unsigned int MAX_LINE_SIZE =  1024;
static char lineBuffer [MAX_LINE_SIZE];

using Client::FileSource;

FileSource::FileSource (const char *fileName)
	: input (fileName, std::ios_base::in)
{
	return;
}

FileSource::~FileSource () {}

int FileSource::start ()
{
	int rc;
	
	// Input file not properly opened or something wrong
	if (!input.is_open () || input.bad ()) 
		return -1;

	// Read the schema line & parse it
	input.getline (lineBuffer, MAX_LINE_SIZE);
	if ((rc = parseSchema (lineBuffer)) != 0)
		return rc;

	// compute offsets used in the tuple encodings
	if ((rc = computeOffsets ()) != 0)
		return rc;
	
	return 0;
}

static bool emptyLine (const char *line)
{
	for (; *line && isspace (*line); line++);
	
	return (*line == '\0');
}

int FileSource::getNext (char *&tuple, unsigned int &len, bool& isHeartbeat)
{
	int rc;
	
	isHeartbeat = false;
	
	len = tupleLen;
	
	// If there is a line parse it into a tuple & return
	if (input.good ()) {
		
		input.getline (lineBuffer, MAX_LINE_SIZE);
		
		if (emptyLine (lineBuffer)) {
			tuple = 0;
			len = 0;
		}
		
		else {
			if ((rc = parseTuple (lineBuffer)) != 0)
				return rc;
		
			tuple = tupleBuf;
		}
	}
	
	// EOF
	else if (input.eof ()) {		
		tuple = 0;
		len = 0;
	}
	
	// Some error
	else {
		ASSERT (input.bad());
		return -1;
	}
	
	return 0;
}

int FileSource::end ()
{
	return 0;
}

int FileSource::parseTuple (char *lineBuffer)
{
	char *begin, *end;
	int ival;
	float fval;	
	
	begin = end = lineBuffer;
	
	for (int a = 0 ; a < numAttrs ; a++) {

		// Seek a comma
		for (; *end && *end != ',' ; end++)
			;
		
		// Empty attribute
		if (begin == end)
			return -1;

		if (*end == '\0' && a != numAttrs - 1)
			return -1;
		
		*end = '\0';
		
		switch (attrTypes [a]) {
			
		case INT:
			
			ival = atoi (begin);
			memcpy (tupleBuf + offsets [a], &ival, sizeof (int));			
			break;
			
		case FLOAT:
			
			fval = atof (begin);
			memcpy (tupleBuf + offsets [a], &fval, sizeof (float));
			break;
			
		case CHAR:
			
			strncpy (tupleBuf + offsets [a], begin, attrLen [a]);
			tupleBuf [offsets [a] + attrLen [a] - 1] = '\0';
			break;
			
		case BYTE:
			
			tupleBuf [offsets [a]] = *begin;
			break;
			
		default:
			
			ASSERT (0);
			break;
		}
		
		begin = ++end;
	}
	
	return 0;
}
	
int FileSource::parseSchema (char *ptr)
{
	
	numAttrs = 0;	
	while (*ptr && numAttrs < MAX_ATTRS) {

		switch (*ptr++) {
		case 'i':
			
			attrTypes [numAttrs++] = INT;
			break;
			
		case 'f':
			
			attrTypes [numAttrs++] = FLOAT;
			break;
			
		case 'b':
			
			attrTypes [numAttrs++] = BYTE;
			break;
			
		case 'c':
			
			attrTypes [numAttrs] = CHAR;
			attrLen [numAttrs] = strtol (ptr, &ptr, 10);
			if (attrLen [numAttrs] <= 0)
				return -1;
			numAttrs ++;
			break;
			
		default:
			return -1;
		}
		
		if (*ptr && *ptr != ',')
			return -1;
		
		if (*ptr == ',')
			ptr ++;		
	}
	
	return 0;
}
	
int FileSource::computeOffsets ()
{
	int offset = 0;

	for (int a = 0 ; a < numAttrs ; a++) {
		offsets [a] = offset;
		
		switch (attrTypes [a]) {
		case INT:
			offset += sizeof(int);
			break;

		case FLOAT:
			offset += sizeof(float);
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
		

