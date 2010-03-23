#ifndef _PARSER_
#define _PARSER_

#include "parser/nodes.h"
namespace Parser {
	NODE *parseCommand(const char *queryBuffer, unsigned int queryLen);
}

#endif
