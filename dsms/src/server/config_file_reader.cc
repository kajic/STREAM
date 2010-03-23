#ifndef _CONFIG_FILE_READER_
#include "server/config_file_reader.h"
#endif

#ifndef _DEBUG_
#include "common/debug.h"
#endif

#include <ctype.h>

using std::endl;

ConfigFileReader::ConfigFileReader (ostream &_LOG)
	: LOG (_LOG) {}

ConfigFileReader::~ConfigFileReader () {}

int ConfigFileReader::setConfigFile (const char *fileName)
{
	ASSERT (fileName);
	
	inputFile.open (fileName, std::ios_base::in);
	lineNo = 0;
	return 0;
}

static const int MAX_LINE = 1024;
static char lineBuf [MAX_LINE+1];

int ConfigFileReader::getNextParam (Param &param, ParamVal &val,
									bool &bFound)
{
	int rc;
	
	bFound = false;
	
	while (!bFound) {
		
		// Reached the end of file
		if (inputFile.eof ())
			return 0;
		
		// Some other error in the input stream
		if (!inputFile.good ()) {
			LOG << "ConfigFileReader: error reading config file" << endl;
			return -1;
		}
		
		// Get the next input line
		inputFile.getline (lineBuf, MAX_LINE);
		lineBuf [MAX_LINE] = '\0';
		
		// An empty line with just ws
		if (isEmpty (lineBuf))
			continue;
		
		// Comment line
		if (isCommentLine (lineBuf))
			continue;
		
		if ((rc = parseLine (lineBuf, param, val)) != 0) {
			LOG << "ConfigFileReader: error in config file at line = " 
				<< lineNo << endl;
			return rc;
		}
		
		bFound = true;
	}
	
	return 0;
}

bool ConfigFileReader::isEmpty (const char *line)
{
	for (; *line ; line++)
		if (!isspace(*line))
			return false;
	return true;
}

bool ConfigFileReader::isCommentLine (const char *line)
{
	return (*line == '#');
}

static const char *MEMORY_SIZE_P       = "MEMORY_SIZE";
static const char *QUEUE_SIZE_P        = "QUEUE_SIZE";
static const char *SHARED_QUEUE_SIZE_P = "SHARED_QUEUE_SIZE";
static const char *INDEX_THRESHOLD_P   = "INDEX_THRESHOLD";
static const char *RUN_TIME_P          = "RUN_TIME";
static const char *CPU_SPEED_P         = "CPU_SPEED";

int ConfigFileReader::parseLine (const char *line,
								 Param      &param,
								 ParamVal   &val)
{
	const char *ptr, *begin;
	
	ptr = line;
	
	// eat white space
	for (; *ptr && isspace (*ptr) ; ptr++);
	
	// First word: parameter specifier
	begin = ptr;
	for (; *ptr && (isalpha (*ptr) || *ptr == '_') ; ptr++);
	
	if ((ptr - begin == 11) &&
		(strncmp(begin, MEMORY_SIZE_P, 11) == 0)) {
		param = MEMORY_SIZE;
	}
	
	else if ((ptr - begin == 10) &&
			 (strncmp(begin, QUEUE_SIZE_P, 10) == 0)) {
		param = QUEUE_SIZE;
	}
	
	else if ((ptr - begin == 17) &&
			 (strncmp(begin, SHARED_QUEUE_SIZE_P, 17) == 0)) {		
		param = SHARED_QUEUE_SIZE;
	}
	
	else if ((ptr - begin == 15) &&
			 (strncmp(begin, INDEX_THRESHOLD_P, 15) == 0)) {		
		param = INDEX_THRESHOLD;
	}
	
	else if ((ptr - begin == 8) &&
			 (strncmp (begin, RUN_TIME_P, 8) == 0)) {
		param = RUN_TIME;
	}

	else if ((ptr - begin == 9) &&
			 (strncmp(begin, CPU_SPEED_P, 9) == 0)) {
		param = CPU_SPEED;
	}
	
	else {
		LOG << "ConfigFileReader: unknown parameter in line no "
			<< lineNo
			<< endl;
		return -1;
	}
	
	// eat white space
	for (; *ptr && isspace (*ptr) ; ptr++);

	// Should read an '='
	if (*ptr != '=') {
		LOG << "ConfigFileReader: error in line no "
			<< lineNo
			<< endl;
		return -1;
	}

	// eat off '='
	ptr++;
	
	// eat white space
	for (; *ptr && isspace(*ptr) ; ptr++);

	if (*ptr == '\0') {
		LOG << "ConfigFileReader: error in line no "
			<< lineNo
			<< endl;		
		return -1;
	}
	
	if (param == MEMORY_SIZE        ||
		param == QUEUE_SIZE         ||
		param == SHARED_QUEUE_SIZE  ||
		param == CPU_SPEED) {
		
		val.ival = atoi (ptr);
	}
	
	else if (param == RUN_TIME) {
		val.lval = atoll(ptr);		
	}
	
	else {
		val.dval = atof (ptr);
	}
	
	return 0;
}
	
