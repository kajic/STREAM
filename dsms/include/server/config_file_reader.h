#ifndef _CONFIG_FILE_READER_
#define _CONFIG_FILE_READER_

#include <ostream>
#include <fstream>

using std::ostream;
using std::ifstream;

/**
 * Configuration file reader.  
 */

class ConfigFileReader {
 private:
	/// System log
	ostream &LOG;
	
	/// Input file stream
	ifstream inputFile;
	
	/// Line no which we are currently processing
	unsigned int lineNo;
	
 public:

	/**
	 * Parameters used to configure the server
	 */
	enum Param {
		MEMORY_SIZE,
		QUEUE_SIZE,
		SHARED_QUEUE_SIZE,
		INDEX_THRESHOLD,
		RUN_TIME,
		CPU_SPEED
	};
	
	/**
	 * The value of a parameter: could be an integer or a double
	 */ 
	union ParamVal {
		int ival;
		double dval;
		long long int lval;
	};

	ConfigFileReader (ostream &LOG);
	~ConfigFileReader ();
	
	/**
	 * Set the configuration file
	 */
	int setConfigFile (const char *fileName);
	
	/**
	 * Get the next parameter and its value from the config file
	 */ 	
	int getNextParam (Param &param, ParamVal &val, bool &bFound);
	
 private:
	
	bool isEmpty (const char *line);
	bool isCommentLine (const char *line);
	
	int parseLine (const char  *lineBuf,
				   Param       &param,
				   ParamVal    &val);
};

#endif
