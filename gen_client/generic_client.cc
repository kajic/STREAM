/**
 * @file       generic_client.cc
 * @date       May 22, 2004
 * @brief      Generic test client for the STREAM server. 
 */

#include <iostream>
#include <stdlib.h>
#include <unistd.h>

#include "script_file_reader.h"
#include "file_source.h"
#include "gen_output.h"
#include "interface/server.h"

using namespace std;
using namespace Client;

static const unsigned int MAX_TABLE_SPEC = 256;
static char tableSpecBuf [MAX_TABLE_SPEC + 1];
static unsigned int tableSpecLen;

static const unsigned int MAX_QUERY = 512;
static char query [MAX_QUERY + 1];
static unsigned int queryLen;

static const int MAX_OUTPUT = 10;
static fstream queryOutput [MAX_OUTPUT];
static int numOutput;

static const unsigned int MAX_SOURCES = 20;
static FileSource *sources [MAX_SOURCES];
static unsigned int numSources;

static GenOutput *outputs [MAX_OUTPUT];

static ofstream logStr;

/**
 *  Error codes used by handle_error:
 */
static const int ERR_INCORRECT_USAGE            = -1001;
static const int ERR_UNABLE_TO_START_SERVER     = -1002;

/**
 * Handle errors: print the error code & exit.
 */
static void handle_error(int rc) 
{
	cerr << endl << "Error: ";
	
	if(rc == ERR_INCORRECT_USAGE) 
		cerr << "Incorrect Usage";
	else if(rc == ERR_UNABLE_TO_START_SERVER)
		cerr << "Unable to start the server";
	else 
		cerr << "Unknown error: " << rc;
	
	cerr << endl;
	
	exit(1);
}

/**
 * Start a new instance of the server
 */
static int start_server(Server *& server) 
{
	server = Server::newServer(logStr);
	
	if(!server)
		return ERR_UNABLE_TO_START_SERVER;
	
	return 0;
}

/**
 * Register an application with the server
 */
static int register_appln (Server       *server, 
						   const char   *applnScriptFile) 
{
	int rc;
	
	ScriptFileReader           *scriptReader;
	ScriptFileReader::Command   command;
	unsigned int                queryId;
	bool                        bQueryValid;
	bool                        bQueryIdValid;
	bool                        bTableSpecValid;
	FileSource                 *source;
	GenOutput                  *output;

	// Reader to interpret the script file
	scriptReader = new ScriptFileReader(applnScriptFile);
	
	// Get the server ready for the barge of commands
	if((rc = server -> beginAppSpecification()) != 0)
		return rc;	
	
	bQueryIdValid = false;
	bTableSpecValid = false;
	bQueryValid = false;
	numOutput = 0;
	numSources = 0;
	
	// Process all commands
	while (true) {
		
		if ((rc = scriptReader -> getNextCommand(command)) != 0)
			return rc;
		
		// No more commands
		if(!command.desc) 
			break;		
		
		switch (command.type) {
			
		case ScriptFileReader::TABLE:
			
			// Copy the table specification to my local state
			tableSpecLen = command.len;			
			if (tableSpecLen > MAX_TABLE_SPEC)
				return -1;			
			strncpy (tableSpecBuf, command.desc, tableSpecLen+1);
			bTableSpecValid = true;
			
			break;
			
		case ScriptFileReader::SOURCE:
			
			// Previous command should have been a table specification. 
			if (!bTableSpecValid)
				return -1;
			
			// Create a source for this table
			if (numSources >= MAX_SOURCES)
				return -1;
			
			source = sources[numSources++] = new FileSource (command.desc);
			
			// register the table
			if((rc = server -> registerBaseTable(tableSpecBuf,
												 tableSpecLen,
												 source)) != 0) {
				cout << "Error registering base table" << endl;
				return rc;
			}
				
			// Reset the table specification buffer
			bTableSpecValid = false;
			
			break;
			
		case ScriptFileReader::QUERY:

			queryLen = command.len;
			if (queryLen > MAX_QUERY)
				return -1;
			
			strncpy (query, command.desc, queryLen+1);
			bQueryValid = true;
			break;

		case ScriptFileReader::DEST:

			if (!bQueryValid)
				return -1;
			
			if (numOutput >= MAX_OUTPUT)
				return -1;

			queryOutput[numOutput].open (command.desc, std::ios_base::out);
			
			// Create a generic query output
			output = outputs[numOutput] =
				new GenOutput (queryOutput[numOutput]);
			numOutput++;
			
			// Register the query
			if((rc = server -> registerQuery(query,
											 queryLen,
											 output,
											 queryId)) != 0){
				cout << "Error registering query" << endl;
				cout << "Query: " << command.desc << endl;
				return rc;
			}
			
			bQueryValid = false;
			
			break;
			
		case ScriptFileReader::VQUERY:			
			
			// Register query
			if ((rc = server -> registerQuery (command.desc,
											   command.len,
											   0,
											   queryId)) != 0) {
				cout << "Error registering query" << endl;
				cout << "Query: " << command.desc << endl;
				return rc;
			}

			bQueryIdValid = true;
			
			break;
			
		case ScriptFileReader::VTABLE:
			
			if (!bQueryIdValid)
				return -1;
			
			if ((rc = server -> registerView (queryId,
											  command.desc,
											  command.len)) != 0) {
				cout << "Error registering view" << endl;
				cout << "View: " << command.desc << endl;
				return rc;
			}
			
			bQueryIdValid = false;
			break;
			
		default:
			
			// unknown command type
			return -1;
		}			
	}

	if (bQueryValid || bQueryIdValid || bTableSpecValid)
		return -1;
	
	if((rc = server -> endAppSpecification()) != 0)
		return rc;

	delete scriptReader;
	
	return 0;
}

/**
 * Start executing the server.
 */
static int start_execution(Server *server)
{
	return server -> beginExecution();
}

void closeOutputFiles ()
{
	for (int o = 0 ; o < numOutput ; o++)
		queryOutput[o].close();
}

static const char *opt_string = "l:c:";

extern char *optarg;
extern int optind;
static int getOpts (int argc,
					char *argv[],
					char *&logFile, char *&configFile,
					char *&scriptFile)
{
	int c;
	
	logFile = configFile = scriptFile = 0;
	while ((c = getopt (argc, argv, opt_string)) != -1) {
		
		if (c == 'l') {

			if (logFile) {
				cout << "Usage: "
					 << argv [0]
					 << " -l[logFile] -c[configFile] [scriptFile]"
					 << endl;
				return -1;				
			}
			
			logFile = strdup (optarg);
		}
		else if (c == 'c') {
			if (configFile) {
				cout << "Usage: "
					 << argv [0]
					 << " -l[logFile] -c[configFile] [scriptFile]"
					 << endl;
				return -1;
			}
			
			configFile = strdup (optarg);
		}
		
		else {			
 			cout << "Usage: "
				 << argv [0]
				 << " -l[logFile] -c[configFile] [scriptFile]"
				 << endl;
			
			return -1;
		}
	}

	if (!logFile || !configFile) {
		cout << "Usage: "
			 << argv [0]
			 << " -l[logFile] -c[configFile] [scriptFile]"
			 << endl;
		return -1;
	}	
	
	if (optind != argc - 1) {
		cout << "Usage: "
			 << argv [0]
			 << " -l[logFile] -c[configFile] [scriptFile]"
			 << endl;
		return -1;
	}		
	
	scriptFile = strdup (argv [optind]);
	
	return 0;
}

int main(int argc, char *argv[])
{	
	Server     *server;
	char       *applnScriptFile;
	char       *configFile;
	char       *logFile;
	int         rc;

	// Options
	if ((rc = getOpts (argc, argv,
					   logFile, configFile,
					   applnScriptFile)) != 0)
		return 1;

	// Log file
	logStr.open (logFile, ofstream::out);
	
	// Fire up the server 
	if((rc = start_server (server)) != 0)  
		handle_error (rc);

	// Register the config file
	if ((rc = server -> setConfigFile (configFile)) != 0)
		handle_error (rc);
	
	// Register application
	if((rc = register_appln (server, applnScriptFile)) != 0)
		handle_error (rc);
	
	// Start execution
	if((rc = start_execution (server)) != 0)
		handle_error (rc);
	
	// Close all the files
	closeOutputFiles ();
	
	free(logFile);
	free(configFile);
	free(applnScriptFile);

	delete server;
	for (int s = 0 ; s < numSources ; s++)
		delete sources[s];
	for (int o = 0 ; o < numOutput ; o++)
		delete outputs[o];
	return 0;
}
