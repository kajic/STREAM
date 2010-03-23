#ifndef _ERROR_
#define _ERROR_

/**
 * The error codes returned by the server.
 */

/// Generic parse error
static const int PARSE_ERR = 101;

/// Duplicate table name
static const int DUPLICATE_TABLE_ERR = 102;

/// Duplicate attr. in the table
static const int DUPLICATE_ATTR_ERR = 103;

/// Unknown table (non-registered table) in a query
static const int UNKNOWN_TABLE_ERR = 104;

/// Unknown variable in query
static const int UNKNOWN_VAR_ERR = 105;

/// Ambiguous attribute
static const int AMBIGUOUS_ATTR_ERR = 106;

/// Unknown attribute
static const int UNKNOWN_ATTR_ERR = 107;

/// Window over relation err
static const int WINDOW_OVER_REL_ERR = 108;

/// Operations over incompatible types
static const int TYPE_ERR = 109;

static const int SCHEMA_MISMATCH_ERR = 110;

static const int AMBIGUOUS_TABLE_ERR = 111;

enum ErrorCode {
	INVALID_USE_ERR = 100,
	INVALID_PARAM_ERR,
	INTERNAL_ERR,
};

/**
 * The messages corresponding to the error codes.
 */

#endif
