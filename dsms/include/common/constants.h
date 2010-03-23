#ifndef _CONSTANTS_
#define _CONSTANTS_

/**
 * @file        constants.h
 * @date        May 20, 2004
 * @brief       Various constants assumed in the system
 */

// Maximum number of registered (named) tables (streams/relations) in the system.
#define MAX_TABLES          50

// Maximum number of queries that can be registered
#define MAX_QUERIES         50

// Maximum number of attributes per (registered) table.
#define MAX_ATTRS           50

// Maximum number of operators reading from an operator
#define MAX_OUT_BRANCHING   10

#endif
