/**
 * @file scan.h
 *
 */

#ifndef _SCAN_
#define _SCAN_

namespace Parser {
	void init_scanner(const char *parseBuf, unsigned int parseBufLen);
	void reset_charptr();
	void reset_scanner();
	int  get_id(char *s);
	char *get_qstring(char *qstring, int len);
}

void yyerror(char const *s);
int yylex(void);
#endif
