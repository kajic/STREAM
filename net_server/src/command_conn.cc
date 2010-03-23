#include <stdlib.h>

#ifndef _NET_MGR_
#include "net_mgr.h"
#endif

#ifndef _COMMAND_CONN_
#include "command_conn.h"
#endif

#ifndef _NET_UTIL_
#include "net_util.h"
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

#define LOG (log_with_id ())

using namespace Network;
using namespace std;

CommandConnection::CommandConnection (int             id,
									  int             sockfd,
									  NetworkManager *net_mgr,
									  ostream        &_LOG)
	: logStream (_LOG)
{
	this -> id     = id;
	this -> sockfd = sockfd;
	this -> net_mgr = net_mgr;
	this -> server = 0;
	this -> b_server_started  = false;
	this -> num_out_conns = 0;
	this -> num_in_conns = 0;
	this -> bEnd = false;
}

void CommandConnection::run()
{
	int rc;
	Command command;
	
	LOG << "Starting Up" << endl;
	
	// We need to register the command connection with the network manager
	if ((rc = net_mgr -> registerCommandConn (id)) != 0) {
		
		LOG << "Error registering with the netmgr"
			<< endl;
		
		return;
	}
	
	// Create an instance of the DSMS server
	if ((rc = initServer ()) != 0) {
		return;
	}
	
	while(!bEnd) {
		
		if ((rc = get_command(command)) != 0) {			
			LOG << "Error getting command" << endl;			
			return;
		}
		
		if ((rc = handle_command(command)) != 0) {
			LOG << "Error handling command" << endl;
			return;
		}
	}
	
	LOG << "Terminating" << endl;
	
	return;
}

static const int num_commands = 11;

static const char *comm_strings [] = {
	"BEGIN APP",
	"REGISTER INPUT",
	"REGISTER QUERY",
	"REGISTER OUTPUT QUERY",
	"REGISTER NAME",
	"END APP",
	"GENERATE PLAN",
	"EXECUTE",
	"REGISTER MONITOR",
	"RESET",
	"TERMINATE"
};

static const Command comms [] = {
	BEGIN_APP,
	REGISTER_INPUT,
	REGISTER_QUERY,
	REGISTER_QUERY_OUT,
	REGISTER_VIEW,	
	END_APP,
	GEN_PLAN,
	EXECUTE,
	REGISTER_MONITOR,
	RESET,
	TERMINATE
};

static const int COMM_BUF_LEN = 1024;

int CommandConnection::get_command(Command &command)
{
	int  rc;
	int  comm_len;
	char comm_buf [COMM_BUF_LEN];	
	
	if ((rc = NetUtil::getMessage(sockfd, comm_buf,
								  COMM_BUF_LEN, comm_len)) != 0) {
		LOG << "Error getting a message"
			<< endl;
		
		return -1;
	}

	for (int c = 0 ; c < num_commands ; c++) {
		
		if (strcmp (comm_strings [c], comm_buf) == 0) {
			command = comms [c];
			
			LOG << "Received command: "
				<< comm_strings [c]
				<< endl;
			
			return 0;
		}
		
	}	
	
	// Unknown command
	LOG << "Unknown command: "
		<< comm_buf
		<< endl;
	
	return -1;
}

int CommandConnection::handle_command(Command command)
{
	switch(command) {
		
	case BEGIN_APP:
		return handle_begin_app();

	case REGISTER_INPUT:
		return handle_reg_input();

	case REGISTER_QUERY:
		return handle_reg_query();

	case REGISTER_QUERY_OUT:
		return handle_reg_out_query();

	case REGISTER_VIEW:
		return handle_reg_view();
		
	case END_APP:
		return handle_end_app();
		
	case GEN_PLAN:
		return handle_gen_plan();
		
	case EXECUTE:
		return handle_execute();
		
	case REGISTER_MONITOR:
		return handle_reg_mon();

	case RESET:
		return handle_reset ();

	case TERMINATE:
		return handle_terminate ();
		
	default:
		// should never come
		ASSERT (0);		
	}
	
	return 0;
}

int CommandConnection::handle_begin_app()
{
	int rc;
	
	if ((rc = server -> beginAppSpecification ()) != 0) {
		LOG << "Server::beginAppSpecification() error" << endl;			
		send_error_code (rc);
		
		return 0;
	}
	
	send_error_code (0);
	return 0;
}

/// debug
#include <fstream>
using std::ifstream;

int CommandConnection::handle_reg_input()
{
	int              rc;
	int              input_id;
	InputConnection *input_conn;
	
	// Get the register input string
	if ((rc = get_mesg (msg_buf, MSG_BUF_LEN, msg_len)) != 0)
		return rc;
	
	// Register input string:
	LOG << "Register Input String: " << msg_buf << endl;

	if (num_in_conns >= MAX_INPUT_CONN) {
 		LOG << "Too many input connections" << endl;
		send_error_code (-1);
		return -1;
	}
	
	// Create an input connection to handle the input
	if ((rc = net_mgr -> createInputConn (input_id, input_conn)) != 0) {
		send_error_code (rc);				
		return rc;
	}

	in_conns [num_in_conns++] = input_conn;
	
	// Register the input with the server
	if ((rc = server -> registerBaseTable (msg_buf, (unsigned int)msg_len,
										   input_conn)) != 0) {
		send_error_code (rc);
		// Server's error - not our business?
		return 0;
	}
	
	send_error_code (0);
	send_id (input_id);
	
	return 0;
}

int CommandConnection::handle_reg_query ()
{
	int rc;
	unsigned int query_id;
	
	// Get the register input string
	if ((rc = get_mesg (msg_buf, MSG_BUF_LEN, msg_len)) != 0)
		return rc;
	
	LOG << "(Internal) Query String: " <<  msg_buf << endl;
	
	if ((rc = server -> registerQuery (msg_buf, (unsigned int)msg_len, 0,
									   query_id)) != 0) {		
		LOG << "Error registering query: " <<  msg_buf << endl;
		send_error_code (rc);
		return 0;
	}
	
	if ((rc = server -> getQuerySchema (query_id,
										planBuf,
										PLAN_BUF_LEN)) != 0) {
		LOG << "Error getting schema from the server" << endl;
		send_error_code (rc);
		return 0;
	}
	
	if ((rc = send_error_code (0)) != 0)
		return rc;
	
	if ((rc = send_id (query_id)) != 0)
		return rc;
	
	if ((rc = send_mesg (planBuf, strlen(planBuf))) != 0)
		return rc;
	
	return 0;
}

int CommandConnection::handle_reg_out_query()
{	
	int rc;
	unsigned int query_id;
	int out_id;
	OutputConnection *out_conn;
	
	// Get the query string
	if ((rc = get_mesg (msg_buf, MSG_BUF_LEN, msg_len)) != 0)
		return rc;
	
	LOG << "(Output) Query String: " << msg_buf << endl;

	if (num_out_conns >= MAX_OUTPUT_CONN) {
 		LOG << "Too many output connections" << endl;
		send_error_code (-1);
		return -1;
	}
	
	// Get an output connection object to relay query output to client
	if ((rc = net_mgr -> createOutputConn (out_id, out_conn)) != 0) {
		LOG << "Error creating an output connection" << endl;
		send_error_code (rc);
		return rc;
	}	
	out_conns [num_out_conns++] = out_conn;
	
	// Register the query with the server
	if ((rc = server -> registerQuery (msg_buf, (unsigned int)msg_len,
									   out_conn,
									   query_id)) != 0) {
		
		LOG << "Error registering query: " <<  msg_buf << endl;
		send_error_code (rc);
		return 0;
	}
	
	// Get the schema of the output
	if ((rc = server -> getQuerySchema (query_id,
										planBuf,
										PLAN_BUF_LEN)) != 0) {
		
		LOG << "Error getting schema from the server" << endl;
		send_error_code (rc);
		return 0;
	}	
	
	if ((rc = send_error_code (0)) != 0)
		return rc;
	
	if ((rc = send_id ((int)query_id)) != 0)
		return rc;
	
	if ((rc = send_id (out_id)) != 0)
		return rc;
	
	if ((rc = send_mesg (planBuf, strlen(planBuf))) != 0)
		return rc;
	
	return 0;
}

int CommandConnection::handle_reg_view()
{	
	int               rc;
	int               query_id;

	// Get the query id
	if ((rc = get_id (query_id)) != 0)
		return rc;
	
	// Get the view schema
	if ((rc = get_mesg (msg_buf, MSG_BUF_LEN, msg_len)) != 0)
		return rc;
	
	LOG << "Query[" << query_id << "] = " << msg_buf << endl;
	
	// Register the view with the server
	if ((rc = server -> registerView ((unsigned int)query_id,
									  msg_buf,
									  (unsigned int)msg_len)) != 0) {
		LOG << "Error registering view with server" << endl;
		send_error_code (rc);
		return 0;
	}
	
	send_error_code (0);
	
	// Send metaplan
	// [Todo]
	
	return 0;
}

int CommandConnection::handle_end_app()
{
	int rc;
	
	if ((rc = server -> endAppSpecification ()) != 0) {		
		LOG << "server::endAppSpec() error" << endl;
		send_error_code (rc);
		return 0;
	}
	
	send_error_code (0);
	
	return 0;
}

int CommandConnection::handle_gen_plan ()
{
	int rc;
	
	if ((rc = server -> getXMLPlan (planBuf, PLAN_BUF_LEN)) != 0) {
		LOG << "Server: getXMLPlan error" << endl;
		send_error_code (rc);
		return 0;
	}
	
	if ((rc = send_error_code (0)) != 0)
		return rc;
	
	if ((rc = send_mesg (planBuf, strlen(planBuf))) != 0)
		return rc;
	
	return 0;
}

static void *start_server (void *s) {
	Server *server = (Server *)s;
	server -> beginExecution ();
	return 0;
}

int CommandConnection::handle_execute ()
{
	int rc;
	
	// start off the server thread
	rc = pthread_create (&server_thread,
						 NULL,
						 &start_server,
						 (void*)server);
	
	if (rc != 0) {
		LOG << "Error creating the server thread" << endl;		
		send_error_code (-1);
		return -1;
	}
	
	b_server_started = true;
	
	if ((rc = send_error_code (0)) != 0)
		return rc;
	
	// Todo
	return 0;
}

int CommandConnection::handle_reset ()
{
	int rc;

	LOG << "handle reset" << endl;
	
	// if we have started off the server thread, stop it
	if (b_server_started) {
		if ((rc = server -> stopExecution ()) != 0) {
			LOG << "Error stopping the server" << endl;
			send_error_code (-1);
			return -1;
		}
		
		pthread_join (server_thread, NULL);
		b_server_started = false;
	}
	
	delete server;

	LOG << "dump after delete server " << endl;
	
	// Stop all the output connections
	for (int o = 0 ; o < num_out_conns ; o++) {
		out_conns[o] -> end ();
		out_conns[o] = 0;
	}
	num_out_conns = 0;

	// Stop all input connections
	for (int i = 0 ; i < num_in_conns ; i++) {
		in_conns [i] -> end ();
		in_conns [i] = 0;
	}
	num_in_conns = 0;
	
	// Create a new server
	if ((rc = initServer ()) != 0) {
		LOG << "Error starting the server" << endl;
		send_error_code (-1);
		return -1;
	}
	
	send_error_code (0);
	return 0;
}

int CommandConnection::handle_reg_mon ()
{
	int rc;
	unsigned int mon_id;
	int out_id;
	OutputConnection *out_conn;
	
	// Get the query string
	if ((rc = get_mesg (msg_buf, MSG_BUF_LEN, msg_len)) != 0)
		return rc;
	
	LOG << "Monitor Query String: " << msg_buf << endl;
	
	if (num_out_conns >= MAX_OUTPUT_CONN) {
		LOG << "Too many output connections" << endl;
		send_error_code (-1);
		return -1;
	}

	// Get an output connection object to relay monitor output to client
	if ((rc = net_mgr -> createOutputConn (out_id, out_conn)) != 0) {
		LOG << "Error creating an output connection" << endl;
		send_error_code (rc);
		return rc;
	}
	out_conns [num_out_conns++] = out_conn;

	// Register the monitor with the server
	if ((rc = server -> registerMonitor (msg_buf, (unsigned int)msg_len,
										 out_conn, mon_id)) != 0) {		
		LOG << "Error registering monitor: " <<  msg_buf << endl;
		send_error_code (rc);
		return 0;
	}

	// Get the schema of the output
	if ((rc = server -> getQuerySchema (mon_id, planBuf, 
										PLAN_BUF_LEN)) != 0) {		
		LOG << "Error getting schema from the server" << endl;
		send_error_code (rc);
		return 0;
	}

 	if ((rc = send_error_code (0)) != 0)
		return rc;

	if ((rc = send_id ((int)mon_id)) != 0)
		return rc;

	if ((rc = send_id (out_id)) != 0)
		return rc;


	if ((rc = send_mesg (planBuf, strlen(planBuf))) != 0)
		return rc;

	if ((rc = server -> getXMLPlan (planBuf, PLAN_BUF_LEN)) != 0) {
		LOG << "Server: getXMLPlan error" << endl;
		return rc;
	}
	
	if ((rc = send_mesg (planBuf, strlen(planBuf))) != 0)
		return rc;
	
	return 0;	
}

int CommandConnection::handle_terminate ()
{
	int rc;
	
	// if we have started off the server thread, stop it
	if (b_server_started) {
		if ((rc = server -> stopExecution ()) != 0) {
			send_error_code (-1);
			return -1;
		}
		
		pthread_join (server_thread, NULL);
		b_server_started = false;
	}
	
	delete server;
	
	// Stop all the output connections
	for (int o = 0 ; o < num_out_conns ; o++) {
		out_conns[o] -> end ();
		out_conns[o] = 0;
	}
	num_out_conns = 0;
	
	bEnd = true;
	
	send_error_code (0);
	
	return 0;
}

static const int MAX_FILE_NAME = 256;
int CommandConnection::initServer ()
{
	int         rc;
	char       *logFilePref;
	char        fileNameBuf [MAX_FILE_NAME];
	ofstream    *dsmsLogStr;
	const char *configFile;
	
	// We don't want all servers to write to the same log: The server will
	// write  to   some  file  of  the   form  <logFilePrefix><id>,  where
	// <logFilePrefix>  is a  common prefix  set at  the start  of network
	// manager, and <id> is the identifier of this thread.	
	logFilePref = net_mgr -> getLogFilePref ();
	int serverId = net_mgr -> getNewServerId ();
	sprintf (fileNameBuf, "%s%d", logFilePref, serverId);
	
	LOG << "Creating Server [" << serverId << "]" << endl;
	
	dsmsLogStr = new ofstream ();
	dsmsLogStr -> open (fileNameBuf, ofstream::out);
	
	server = Server::newServer (*dsmsLogStr);

	configFile = net_mgr -> getConfigFile ();
	
	if ((rc = server -> setConfigFile (configFile)) != 0) {
		LOG << "Error setting server config file" << endl;
		return rc;
	}
	
	return 0;
}

ostream& CommandConnection::log_with_id ()
{
	logStream << "CommandConn ["
			  << id
			  << "]: ";
	
	return logStream;
}
		
int CommandConnection::get_id (int &id)
{
	int rc;
	int msg_len;
	
	if ((rc = get_mesg (msg_buf, MSG_BUF_LEN, msg_len)) != 0)
		return rc;

	id = atoi (msg_buf);
	return 0;
}

int CommandConnection::get_mesg (char *buf, int buf_len, int &msg_len)
{
	int rc;

	if ((rc = NetUtil::getMessage (sockfd, buf, buf_len, msg_len)) != 0) {

		LOG << "Error getting a message" << endl;
		return rc;
	}

	return 0;
}

int CommandConnection::send_error_code (int err_code)
{
	int rc;

	if ((rc = NetUtil::sendErrorCode (sockfd, err_code)) != 0) {

		LOG << "Error sending error code" << endl;
		return -1;
	}

	return 0;
}

int CommandConnection::send_id (int id)
{
	int rc;

	sprintf (msg_buf, "%d", id);
	
	if ((rc = NetUtil::sendMessage (sockfd, msg_buf,
									strlen(msg_buf))) != 0) {
		LOG << "Error sending message" << endl;
		return rc;
	}
	return 0;
}

int CommandConnection::send_mesg (char *msg, int msg_len)
{
	int rc;
	
	if ((rc = NetUtil::sendMessage (sockfd, msg, msg_len)) != 0) {		
		LOG << "Error sending a message" << endl;
		return rc;
	}
	
	return 0;
}
