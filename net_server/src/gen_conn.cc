#ifndef _GEN_CONN_
#include "gen_conn.h"
#endif

#ifndef _DEBUG_
#include "debug.h"
#endif

#ifndef _NET_UTIL_
#include "net_util.h"
#endif

#ifndef _COMMAND_CONN_
#include "command_conn.h"
#endif

#ifndef _IN_CONN_
#include "in_conn.h"
#endif

#ifndef _OUT_CONN_
#include "out_conn.h"
#endif

#ifndef _NET_MGR_
#include "net_mgr.h"
#endif

using namespace Network;
using namespace std;

#define LOG (log_with_id ())

GenericConnection::GenericConnection (int id,
									  NetworkManager *netMgr,
									  int sockfd,
									  ostream &_LOG)
	: log (_LOG)
{
	this -> id = id;
	this -> netMgr = netMgr;
	this -> sockfd = sockfd;
	
	return;
}

void GenericConnection::run()
{
	int rc;
	ConnSpec connSpec;
	
	LOG << "Starting" << endl;   	
	
	// Determine the type of the connection based on the first message
	// from the client 
	if ((rc = getConnectionSpec(connSpec)) != 0) {
		LOG << "Error getting connection spec" << endl;
		
		terminate();
		return;
	}
	
	if (connSpec.type == COMMAND_CONN) {
		
		if ((rc = NetUtil::sendErrorCode(sockfd, 0)) != 0) {
			LOG << "Error sending error code"
				<< endl;
			terminate();
			return;
		}
		
		if ((rc = morphToCommandConn()) != 0) {
			
			LOG << "Error morphing to command connectin"
				<< endl;
			
			terminate();
			return;
		}
	}
	
	else if (connSpec.type == INPUT_CONN) {
		
		if ((rc = NetUtil::sendErrorCode(sockfd, 0)) != 0) {
			LOG << "Error sending error code"
				<< endl;
			terminate();
			return;
		}
		
		if ((rc = morphToInputConn(connSpec.inputId)) != 0) {
			LOG << "Error morphing to input connection"
				<< endl;
			
			terminate();
			return;
		}					
	}
	
	else if (connSpec.type == OUTPUT_CONN) {
		
		if ((rc = NetUtil::sendErrorCode(sockfd, 0)) != 0) {
			LOG << "Error sending error code"
				<< endl;
			terminate();
			return;
		}		
		
		if ((rc = morphToOutputConn(connSpec.outputId)) != 0) {
			LOG << "Error morphing to input connection"
				<< endl;
			
			terminate();
			return;
		}
	}
	
	else {
		// should never reach here
		ASSERT (false);
	}
	
	terminate();
	return;	
}

static const int MSG_BUF_LEN = 128;

int GenericConnection::getConnectionSpec(ConnSpec &spec)
{
	int rc;
	char msgBuf [MSG_BUF_LEN];
	int msgLen;
	
	// The first message contains the specification of the connection type
	if((rc = NetUtil::getMessage(sockfd,
								 msgBuf,
								 MSG_BUF_LEN,
								 msgLen)) != 0)
		return -1;
	
	// It is a command connection
	if (strncmp(msgBuf, "COMMAND_CONN", 12) == 0) {
		
#ifdef _DM_
		if (msgLen != 12)
			return -1;
#endif
		
		spec.type = COMMAND_CONN;
		spec.inputId = 0;
		spec.outputId = 0;
	}	
	
	// It is an input connection
	else if (strncmp(msgBuf, "INPUT_CONN", 10) == 0) {		
		spec.type = INPUT_CONN;
		spec.inputId = atoi(msgBuf + 10);
		spec.outputId = 0;
	}
	
	else if (strncmp(msgBuf, "OUTPUT_CONN", 11) == 0) {
		spec.type = OUTPUT_CONN;
		spec.outputId = atoi(msgBuf + 11);
		spec.inputId = 0;				
	}
	
	// error
	else {		
		LOG << "Unknown connection type"
			<< msgBuf
			<< endl;
		return -1;							
	}
	
	// everything ok here
	return 0;	
}
	
int GenericConnection::morphToCommandConn()
{
	LOG << "Morphing to CommandConn" << endl;
	
	CommandConnection *commConn = new CommandConnection(id, sockfd, netMgr, LOG);
	commConn -> run();

	LOG << "Destroying command connection" << endl;
	
	delete commConn;
	return 0;
}

int GenericConnection::morphToInputConn(unsigned int inputId)
{
	int rc;
	InputConnection *inputConn;
	
	LOG << "Morphing to InputConn" << endl;
		
	// Get the input connection based on the id
	if ((rc = netMgr -> getInputConn (inputId, inputConn)) != 0) {
		LOG << "Unable to get input connection"
			<< endl;
		return rc;
	}

	// Set the socket and run ...
	inputConn -> setSocket (sockfd);	
	inputConn -> run();
	netMgr -> destroyInputConn (inputId);
	
	return 0;
}

int GenericConnection::morphToOutputConn(unsigned int outputId)
{
	int rc;	
	OutputConnection *outputConn;
	
	LOG << "Morphing to OutputConn" << endl;   
	
	if ((rc = netMgr -> getOutputConn (outputId, outputConn)) != 0) {
		LOG << "Unable to get output connection"
			<< endl;
		return rc;
	}
	
	outputConn -> setSocket (sockfd);
	outputConn -> run();
	netMgr -> destroyOutputConn (outputId);
	
	return 0;
}

void GenericConnection::terminate()
{
	close(sockfd);	
}

ostream &GenericConnection::log_with_id ()
{
	log << "GenConn[" << id << "]: ";
	return log;
}
