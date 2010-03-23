#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

/// DEBUG
#include <iostream>
using namespace std;

#define cp(x) cout << "Checkpoint: " << x << endl;

#ifndef _NET_MGR_
#include "net_mgr.h"
#endif

#ifndef _GEN_CONN_
#include "gen_conn.h"
#endif

#ifndef _DEBUG_
#include "debug.h"
#endif

#ifndef _NET_ERROR_
#include "net_error.h"
#endif

#ifndef _IN_CONN_
#include "in_conn.h"
#endif

#ifndef _OUT_CONN_
#include "out_conn.h"
#endif

static const int MAX_FILE_NAME = 128;
static char logFile [MAX_FILE_NAME];

using namespace Network;
using namespace std;

NetworkManager::NetworkManager(int   portNo,
							   char *logFilePref,
							   char *configFile)
{
	this -> portNo      = portNo;
	this -> logFilePref = strdup (logFilePref);
	this -> configFile  = strdup (configFile);
	this -> serverId    = 0;
	
	// Log for the network manager and all the connections
	sprintf (logFile, "%s", logFilePref);
	LOG.open (logFile, ofstream::app);
	
	// Initialize the thread pool
	for (int t = 0 ; t < NUM_THREADS ; t++)
		b_thread_in_use [t] = false;

	// Initialize input/output connections
	for (int i = 0 ; i < MAX_INPUT_CONN ; i++)
		input_conns [i] = 0;

	for (int o = 0 ; o < MAX_OUTPUT_CONN ; o++)
		output_conns [o] = 0;
	
	num_conns = 0;	
}

void NetworkManager::run()
{
	int sockfd, cli_sockfd, cli_len;
	struct sockaddr_in serv_addr, cli_addr;	
	
	LOG << "NetMgr: starting up" << endl;
	
	//-------------------------------------------------------------------
	// Open a socket, bind the address, and wait for new connections
	//-------------------------------------------------------------------
	
	if ((sockfd = socket (PF_INET, SOCK_STREAM, 0)) < 0) {
		LOG << "NetMgr: Can't open a stream socket" << endl;
		return;
	}
	
	bzero((char *) &serv_addr, sizeof(serv_addr));
	serv_addr.sin_family = PF_INET;
	serv_addr.sin_addr.s_addr = htonl(INADDR_ANY);
	serv_addr.sin_port = htons(portNo);
	
	if(bind(sockfd,
			(struct sockaddr *) &serv_addr,
			sizeof(serv_addr)) < 0) {
		LOG << "NetMgr: Can't bind to port " << portNo << endl;
		return;
	}
	
	listen(sockfd, MAX_WAIT_CONN);
	
	while (true) {
		
		cli_len = sizeof(cli_addr);
		cli_sockfd = accept(sockfd, (struct sockaddr *) &cli_addr,							 
							(socklen_t *) &cli_len);
		
		if (cli_sockfd < 0) {
			LOG << "NetMgr: Error accepting a new connection" << endl;
			return;
		}
		
		if (handleNewConn (cli_sockfd) != 0) {
			LOG << "NetMgr: Error handling new connection" << endl;
			return;
		}
	}
	
	// never comes
	return;
}

// Start off a generic connection thread
int NetworkManager::handleNewConn (int cli_sockfd)
{
	int free_thread_id;
	int rc;
	int conn_id;
	GenericConnection *gen_conn;
	
	// Get a free thread to do the work for us
	free_thread_id = -1;
	for (int t = 0 ; t < (int) NUM_THREADS ; t++) {
		if (!b_thread_in_use [t]) {
			free_thread_id = t;
			break;
		}
	}
	
	// If we do not have a free thread, we just don't accept the
	// connection.  We write this situation to the log ...
	if (free_thread_id == -1) {
		
		LOG << "NetMgr: A request not handled due to lack of resources"
			<< endl;
		
		close (cli_sockfd);
		return 0;
	}
	
	// Create a new generic connection object
	ASSERT (num_conns <= MAX_CONN);
	
	// We do not have space to store connection related information, we
	// have to be rebooted :)
	if (num_conns == MAX_CONN) {
		LOG << "NetMgr: No space left on connection table" << endl;
		return -1;		
	}
	
	conn_id = num_conns;	
	gen_conn = new GenericConnection (conn_id, this,									  
									  cli_sockfd, LOG);									  
	connTable [conn_id].type = UNKNOWN;
	num_conns ++;
	
	// Start off the thread ...
	rc = pthread_create (&conn_threads [free_thread_id], NULL, &start_thread,
						 (void*)gen_conn);
	if (rc != 0) {
		LOG << "NetMgr: Error creating a new connection thread" << endl;
		return -1;
	}
	
	return 0;
}

char *NetworkManager::getLogFilePref () const {
	return logFilePref;
}

int NetworkManager::getNewServerId ()
{
	return serverId++;
}

char *NetworkManager::getConfigFile () const {
	return configFile;
}

int NetworkManager::registerCommandConn (int id) {
	ASSERT (id >= 0 && id < num_conns);
	
	connTable [id].type = COMMAND_CONN;
	return 0;
}

int NetworkManager::createInputConn (int &conn_id, InputConnection *&conn)
{
	conn_id = -1;
	for (int i = 0 ; i < MAX_INPUT_CONN  ; i++) {		
		if (!input_conns [i]) {
			conn_id = i;
			break;
		}
	}
	
	// We don't have a free connection
	if (conn_id == -1) {
		return NET_MGR_RSRC_ERR;
	}

	// Create a new input connection
	conn = new InputConnection (LOG);

	input_conns [conn_id] = conn;
	return 0;
}

int NetworkManager::getInputConn (int conn_id, InputConnection *&conn)
{
	if (conn_id < 0 || conn_id > MAX_INPUT_CONN)
		return -1;

	if (!input_conns [conn_id])
		return -1;

	conn = input_conns [conn_id];

	return 0;
}

int NetworkManager::destroyInputConn (int conn_id)
{
	ASSERT (conn_id >= 0 && conn_id < MAX_INPUT_CONN);
	ASSERT (input_conns [conn_id]);

	LOG << "Destroying input connection: " << conn_id << endl;
	
	delete input_conns [conn_id];
	input_conns [conn_id] = 0;
	
	return 0;
}

int NetworkManager::createOutputConn (int &conn_id,
									  OutputConnection *&conn)
{
	conn_id = -1;
	
	for (int i = 0 ; i < MAX_OUTPUT_CONN ; i++) {
		if (!output_conns [i]) {
			conn_id = i;
			break;
		}
	}
	
	if (conn_id == -1) {
		return NET_MGR_RSRC_ERR;
	}
	
	conn = new OutputConnection (LOG);
	
	output_conns [conn_id] = conn;
	
	return 0;
}

int NetworkManager::getOutputConn (int conn_id, OutputConnection *&conn)
{
	if (conn_id < 0 || conn_id >= MAX_OUTPUT_CONN)
		return -1;

	if (!output_conns [conn_id])
		return -1;

	conn = output_conns [conn_id];

	return 0;
}

int NetworkManager::destroyOutputConn (int conn_id)
{
	ASSERT (conn_id >= 0 && conn_id < MAX_OUTPUT_CONN);
	ASSERT (output_conns [conn_id]);

	delete output_conns [conn_id];
	output_conns [conn_id] = 0;

	LOG << "Destroying output connection: " << conn_id << endl;
	
	return 0;
}

#include <iostream>
using namespace std;

// Option string
static const char *opt_string = "l:c:p:";

static int getOpts (int argc,
					char *argv[],
					char *&logFilePref, char *&configFile,
					int &portNo)
{
	int c;
	
	logFilePref = configFile = 0;
	portNo = 0;	
	
	while ((c = getopt (argc, argv, opt_string)) != -1) {
		
		if (c == 'l') {
			
			if (logFilePref) {
				cout << "Usage: "
					 << argv [0]
					 << " -l [logFilePref] -c [configFile] -p [portNo]"
					 << endl;
				return -1;				
			}
			
			logFilePref = strdup (optarg);
		}
		
		else if (c == 'c') {
			
			if (configFile) {
				cout << "Usage: "
					 << argv [0]
					 << " -l [logFilePref] -c [configFile] -p [portNo]"
					 << endl;
				return -1;
			}
			
			configFile = strdup (optarg);
		}
		
		else if (c == 'p') {
			
			if (portNo != 0) {
				cout << "Usage: "
					 << argv [0]
					 << " -l [logFilePref] -c [configFile] -p [portNo]"
					 << endl;
				return -1;
			}
			
			portNo = atoi (optarg);

			if (portNo <= 0) {
				cout << "Invalid port number" << endl;
				return -1;
			}			
		}
	}
	
	if (!logFilePref || !configFile || (portNo == 0)) {
		cout << "Usage: "
			 << argv [0]
			 << " -l [logFilePref] -c [configFile] -p [portNo]"
			 << endl;
		return -1;
	}
	
	return 0;
}

int main(int argc, char *argv[])
{
	int rc;
	int       portNo;
	char     *logFilePref;
	char     *configFile;
	
	// Options
	if ((rc = getOpts (argc, argv,
					   logFilePref, configFile,
					   portNo)) != 0)
		return 1;	
	
	NetworkManager *netMgr = new NetworkManager (portNo, logFilePref,
												 configFile);
	
	netMgr -> run();
	
	return 0;
}
