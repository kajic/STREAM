/**
 * @file      script_file_reader.cc
 * @date      May 21, 2004
 * @brief     routines to read a script file
 */

#include <iostream>
#include <fstream>
#include <ctype.h>

#include "script_file_reader.h"

using namespace Client;
using namespace std;

// Open the input file
ScriptFileReader::ScriptFileReader(const char *fileName) 
	: inputFile (fileName, ios_base::in) 
{
	lineNo = 0;
}

// Close the input file
ScriptFileReader::~ScriptFileReader() 
{
	inputFile.close();
}

/**
 * Determine if all hte characters in a line are space characters.
 */
bool ScriptFileReader::isEmptyLine (const char   *lineBuffer) const
{	
	for(; *lineBuffer ; lineBuffer++) {
		if(!isspace (*lineBuffer)) {
			return false;
		}
	}
	return true;
}

/**
 * Determine if the line is a comment line
 */
bool ScriptFileReader::isCommentLine (const char    *lineBuffer) const
{
	if(*lineBuffer && *lineBuffer == '#')
		return true;
	return false;
}

// String constants used in parsing
static const char *TABLE_COMMAND   = "table";
static const char *SOURCE_COMMAND  = "source";
static const char *QUERY_COMMAND   = "query";
static const char *DEST_COMMAND    = "dest";
static const char *VQUERY_COMMAND  = "vquery";
static const char *VTABLE_COMMAND  = "vtable";

/**
 * Examples:
 * 
 * "table   : <command> "
 * "source  : <command> "
 * "query   : <command  "
 * "vquery  : <command> "
 * "vtable  : <command> "
 */
int ScriptFileReader::parseLine (char *lineBuffer, Command &command)
{
	char         *ptr;
	char         *begin;
	
	ptr   = lineBuffer;
	
	// eat white space
	for (; *ptr && isspace(*ptr) ; ptr++);
	
	// First word: command type specifier
	begin = ptr;
	for (; *ptr && isalpha(*ptr) ; ptr++);
	
	// No command indicator: parse error
	if(ptr == begin) {
		return -1;
	}
	
	// Catalog command
	if (strncmp(begin, TABLE_COMMAND, ptr - begin) == 0) {
		command.type = TABLE;
	}
	
	else if (strncmp (begin, SOURCE_COMMAND, ptr - begin) == 0) {
		command.type = SOURCE;
	}
	
	else if (strncmp (begin, QUERY_COMMAND, ptr - begin) == 0) {
		command.type = QUERY;
	}
	
	else if (strncmp (begin, VQUERY_COMMAND, ptr - begin) == 0) {
		command.type = VQUERY;
	}
	
	else if (strncmp (begin, VTABLE_COMMAND, ptr - begin) == 0) {
		command.type = VTABLE;
	}
	else if (strncmp (begin, DEST_COMMAND, ptr - begin) == 0) {
		command.type = DEST;
	}
	else {
		return -1;
	}
	
	// eat white space
	for(; *ptr && isspace(*ptr) ; ptr++);
	
	// I am expecting a ':'
	if(*ptr != ':')  return -1;
	
	// eat ':'
	ptr++;
		
	// eat white space
	for(; *ptr && isspace(*ptr) ; ptr++);
	
	command.lineNo      = lineNo;
	command.desc        = ptr;
	command.len         = strlen(ptr);
	
	return 0;
}

/**
 * get the next comamnd from the input file.  
 *
 * Each command is in a line of its own.  Lines that are empty and lines
 * that start with '#' (comment lines) should be ignored.
 */

int ScriptFileReader::getNextCommand(ScriptFileReader::Command& command)
{	
	while(inputFile.good()) {
		
		// Next input line
		inputFile.getline (lineBuffer, LINE_BUFFER_SIZE);
		lineNo ++;
		
		// Empty line or a comment line
		if(isEmptyLine   (lineBuffer)    ||
		   isCommentLine (lineBuffer))
			continue;
		
		// Peek into the line to determine the type of the command
		return parseLine (lineBuffer, command);
	}
	
	// We have reached the end of the file
	if(inputFile.eof()) {
		command.lineNo      = lineNo;
		command.desc        = 0;
		command.len         = 0;
		return 0;
	}
	
	// some other error reading the file	
	cerr << "Error reading the file" << endl;
	return -10;
}
		
