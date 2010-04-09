/*
 * scanhelp.c: help functions for lexer
 *
 * Authors: Jan Jannink 
 *          Jason McHugh
 *          
 * originally by: Mark McAuliffe, University of Wisconsin - Madison, 1991
 * 
 * 1997 Changes: "print", "buffer", "reset" and "io" added.
 * 1998 Changes: "resize", "queryplans", "on" and "off" added.
 *
 *
 * This file is not compiled separately; it is #included into lex.yy.c .
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "parser/scan.h"
#include "parser/nodes.h"
#include "parse.h"

/*
 * size of buffer of strings
 */
#define MAXCHAR		  5000
#define MAXSTRINGLEN   256

/*
 * buffer for string allocation
 */
static char charpool[MAXCHAR];
static int charptr = 0;

static int lower(char *dst, char *src, int max);
static char *mk_string(char *s, int len);

/*
 * string_alloc: returns a pointer to a string of length len if possible
 */
static char *string_alloc(int len)
{
	char *s;

	if(charptr + len > MAXCHAR){
		fprintf(stderr, "out of memory\n");
		exit(1);
	}

	s = charpool + charptr;
	charptr += len;

	return s;
}

/*
 * reset_charptr: releases all memory allocated in preparation for the
 * next query.
 *
 * No return value.
 */
void Parser::reset_charptr(void)
{
    charptr = 0;
}

/*
 * get_id: determines whether s is a reserved word, and returns the
 * appropriate token value if it is.  Otherwise, it returns the token
 * value corresponding to a string.  If s is longer than the maximum token
 * length (MAXSTRINGLEN) then it returns NOTOKEN, so that the parser will
 * flag an error (this is a stupid kludge).
 */
int Parser::get_id(char *s)
{
	static char string[MAXSTRINGLEN];
	int len;
	
	if((len = lower(string, s, MAXSTRINGLEN)) == MAXSTRINGLEN)
		return NOTOKEN;
	
	if(!strcmp(string, "register"))
		return RW_REGISTER;    
	if(!strcmp(string, "stream"))
		return RW_STREAM; 
	if(!strcmp(string, "relation"))
		return RW_RELATION; 
	
	if(!strcmp(string, "integer")) 
		return RW_INTEGER;	
	if(!strcmp(string, "int")) 
		return RW_INTEGER;	
	if(!strcmp(string, "float")) 
		return RW_FLOAT;
	if(!strcmp(string, "real"))
		return RW_FLOAT;	
	if(!strcmp(string, "char")) 
		return RW_CHAR;
	if(!strcmp(string, "byte")) 
		return RW_BYTE;
	
	// Xstream(Select from Where clause)
	if(!strcmp(string, "istream")) 
		return RW_ISTREAM;
	if(!strcmp(string, "dstream"))
		return RW_DSTREAM;
	if(!strcmp(string, "rstream"))
		return RW_RSTREAM;   
	if(!strcmp(string, "select")) {
		return RW_SELECT;
	}
	if(!strcmp(string, "distinct"))
		return RW_DISTINCT;
	if(!strcmp(string, "from"))
		return RW_FROM;
	if(!strcmp(string, "where"))
		return RW_WHERE;   
	if(!strcmp(string, "group"))
		return RW_GROUP;
	if(!strcmp(string, "by"))
		return RW_BY;   
	if(!strcmp(string, "and"))
		return RW_AND;
	if(!strcmp(string, "as"))
		return RW_AS;   
	if(!strcmp(string, "union"))
		return RW_UNION;
	if(!strcmp(string, "except"))
		return RW_EXCEPT;
	
	// aggregation operators 
	if(!strcmp(string, "avg"))
		return RW_AVG;
	if(!strcmp(string, "min"))
		return RW_MIN;
	if(!strcmp(string, "max"))
		return RW_MAX;
	if(!strcmp(string, "count"))
		return RW_COUNT;
	if(!strcmp(string, "sum"))
		return RW_SUM;
      
	// window clause
	if(!strcmp(string, "rows"))
		return RW_ROWS;   
	if(!strcmp(string, "range"))
		return RW_RANGE;    
	if(!strcmp(string, "slide"))
		return RW_SLIDE;        
	if(!strcmp(string, "now"))
		return RW_NOW;
	if(!strcmp(string, "partition"))
		return RW_PARTITION;
	if(!strcmp(string, "unbounded"))
		return RW_UNBOUNDED;
	
	// time expressions
	if (!strcmp(string, "second") || !strcmp(string, "seconds"))
		return RW_SECOND;
	if (!strcmp(string, "minute") || !strcmp(string, "minutes"))
		return RW_MINUTE;
	if (!strcmp(string, "hour") || !strcmp(string, "hours"))
		return RW_HOUR;
	if (!strcmp(string, "day") || !strcmp(string, "days"))
		return RW_DAY;
	
	/*  unresolved lexemes are strings */
	yylval.sval = mk_string(s, len);
	return T_STRING;
}

/*
 * lower: copies src to dst, converting it to lowercase, stopping at the
 * end of src or after max characters.
 *
 * Returns:
 * 	the length of dst (which may be less than the length of src, if
 * 	    src is too long).
 */
static int lower(char *dst, char *src, int max)
{
	int len;

	for(len = 0; len < max && src[len] != '\0'; ++len){
		dst[len] = src[len];
		if(src[len] >= 'A' && src[len] <= 'Z')
			dst[len] += 'a' - 'A';
	}
	dst[len] = '\0';

	return len;
}

/*
 * get_qstring: removes the quotes from a quoted string, allocates
 * space for the resulting string.
 *
 * Returns:
 * 	a pointer to the new string
 */
char *Parser::get_qstring(char *qstring, int len)
{
	/* replace ending quote with \0 */
	qstring[len - 1] = '\0';

	/* copy everything following beginning quote */
	return mk_string(qstring + 1, len - 2);
}

/*
 * mk_string: allocates space for a string of length len and copies s into
 * it.
 *
 * Returns:
 * 	a pointer to the new string
 */
static char *mk_string(char *s, int len)
{
	char *copy;

	/* allocate space for new string */
	if((copy = string_alloc(len + 1)) == NULL){
		printf("out of string space\n");
		exit(1);
	}
   
	/* copy the string */
	strncpy(copy, s, len + 1);
	return copy;
}

