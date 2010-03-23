%{
/*
 * scan.l: lex spec for CQL
 *
 * Authors: Shivnath Babu, Arvind Arasu
 *           
 * Originally by: Mark McAuliffe, University of Wisconsin - Madison, 1991
 *                Dallan Quass, Jan Jannink (Stanford University)
 */

#include <string.h>
#include "parser/nodes.h"
#include "parse.h"
#include "parser/scan.h"

using namespace Parser;

/**
 * Flex uses the macro YY_INPUT to read input data. YY_INPUT needs to be
 * redefined for input redirection.  For more information please see pages
 * 156-157 of:   
 *
 * Lex & Yacc
 * by John R. Levine, Tony Mason, Doug Brown
 * Reilly & Associates; ISBN: 1565920007
 *
 */

int my_yyinput(char * buf, int max_size);

#undef YY_INPUT
#define YY_INPUT(buffer, result, max_size) (result = my_yyinput(buffer, max_size))

const char *scanBuf;
int         scanBufLen;
int         scanBufPos;

%}

letter               [A-Za-z]
digit                [0-9]
num                  {digit}+
s_num                {num}

%option              noyywrap

%%
[ \n\t]              {/* ignore spaces, tabs, and newlines */}

{s_num}              {
                       sscanf(yytext, "%d", &yylval.ival);
                       return T_INT;
                     }


{s_num}\.{num}       {
                       sscanf(yytext, "%f", &yylval.rval);
                       return T_REAL;
                     }

{s_num}\.{num}[Ee]{s_num} {
                       sscanf(yytext, "%f", &yylval.rval);
                       return T_REAL;
                     }


\"([^\"\n]|(\"\"))*\" {
                     yylval.sval = Parser::get_qstring(yytext, yyleng);
                     return T_QSTRING;
                    }

\"([^\"\n]|(\"\"))*\n {
                     printf("newline in string constant\n");
                    }

{letter}({letter}|{digit}|_)*   {
                     return Parser::get_id(yylval.sval = yytext);
                     }

"<"                  {return T_LT;}
"<="                 {return T_LE;}
">"                  {return T_GT;}
">="                 {return T_GE;}
"="                  {return T_EQ;}
"!="                 {return T_NE;}
"<>"                 {return T_NE;}

[*/+\-=<>':;,.|&()]  {return yytext[0];}
"["                  {return yytext[0];}
"]"                  {return yytext[0];}
"$"                  {return yytext[0];}
.                    {
                       /* ignore '\0' */ 
                       if(yytext[0] > 0)
                          printf("illegal character [%c]\n", yytext[0]);
                     }
%%

/**
 * Initialize the scanner: 
 */
void Parser::init_scanner(const char *_scanBuf, unsigned int _scanBufLen) {
  scanBuf = _scanBuf;
  scanBufLen = _scanBufLen;
  scanBufPos = 0;
  reset_charptr();
  yyrestart(yyin);
}

/*
 * reset_scanner: resets the scanner after a syntax error
 *
 * No return value.
 */
void Parser::reset_scanner(void)
{
  reset_charptr();
  yyrestart(yyin);
}

// Copy characters from the input string into the buffer
int my_yyinput(char* buf, int max_size) {
  int n = scanBufLen - scanBufPos;
  
  // we are limited by the length of lex's buffer
  if(n > max_size)
    n = max_size;
  
  if(n > 0) {
    memcpy(buf, scanBuf + scanBufPos, n);
    scanBufPos += n;
  }
  
  // the number of characters read
  return n;
}
