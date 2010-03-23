/**
 * @file        types.h
 * @date        May 9, 2004
 * @brief       Data types recognized by the system
 */

#ifndef _TYPES_
#define _TYPES_

typedef unsigned int Timestamp;
typedef unsigned int TimeDuration;
typedef unsigned int Hash;

/**
 * Data types recognized by the STREAM system.  
 */

enum Type {
	INT,                // 4 byte integer
	FLOAT,              // 4 byte floating point
	BYTE,               // 1 byte characters
	CHAR                // fixed length strings.
};

// Lengths of the data types
static const unsigned int INT_SIZE   = sizeof(int);
static const unsigned int FLOAT_SIZE = sizeof(float);
static const unsigned int BYTE_SIZE  = sizeof(char);
static const unsigned int TIMESTAMP_SIZE = sizeof (Timestamp);

#endif
