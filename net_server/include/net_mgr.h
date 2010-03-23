#ifndef _NET_MGR_
#define _NET_MGR_

#include <fstream>
#include <pthread.h>

#ifndef _THREAD_
#include "thread.h"
#endif

namespace Network {
	class InputConnection;
	class OutputConnection;
	class CommandConnection;
	
	class NetworkManager : public Thread {
	private:
		/// The port number on which the dsms listens for new connections
		unsigned int portNo;
		
		/// Event logger
		std::ofstream LOG;		
		
		/// Maximum number of connection threads we allow
		static const int NUM_THREADS = 20;
		
		/// Maximum number of outstanding connection requests
		static const int MAX_WAIT_CONN = 5;
		
		/// Connection thread pool
		pthread_t conn_threads [NUM_THREADS];
		
		/// Usage information of the threads
		bool b_thread_in_use [NUM_THREADS];
		
		/// Max. number of connections we can handle before "reboot"
		static const int MAX_CONN = 100000;

		int serverId;
		
		//----------------------------------------------------------------------
		// Connection Information
		//----------------------------------------------------------------------
		
		/// Different types of connections
		enum ConnType {
			COMMAND_CONN,
			INPUT_CONN,
			OUTPUT_CONN,
			UNKNOWN
		};
		
		/// Information we maintain per connection
		struct ConnInfo {
			ConnType type;			
		};
		
		/// Table containing information about all the connections we
		/// handled.
		ConnInfo connTable [MAX_CONN];
		
		/// Number of connections we have handled so far.
		int num_conns;

		/// Log file prefix for the created DSMSes
		char *logFilePref;
		
		/// Configuration file for the created DSMSes
		char *configFile;

		//----------------------------------------------------------------------
		// Input/Output connections
		//----------------------------------------------------------------------

		static const int MAX_INPUT_CONN = 100;
		InputConnection *input_conns [MAX_INPUT_CONN];
		
		static const int MAX_OUTPUT_CONN = 100;		
		OutputConnection *output_conns [MAX_OUTPUT_CONN];
		
	public:
		
		/**
		 * @param port The port no on which the network manager listens
		 *             for connections.
		 *
		 * @param logFilePref The prefix for the log files.  The file
		 *                    logFilePref0 is used by the network manager,
		 *                    and the files logFilePref%d are used by
		 *                    instantiations of DSMSes.  The network
		 *                    manager owns the string, ie, it can
		 *                    deallocate it later.
		 *
		 * @param configFile  The configuration file for the DSMS instances.
		 */
		
		NetworkManager (int portNo, char *logFilePref, char *configFile);
		
		/**
		 * Starts off the network manager.  Never returns (if everything
		 * goes well).  Waits for new connections, and starts off a new
		 * generic client thread for every new connection request.
		 */	
		virtual void run();

		/**
		 * A command connection registers itself with a network manager at
		 * the time of its startup.  [[ Explanation of id ]]
		 */
		int registerCommandConn (int id);
		
		
		/**
		 * Called by a command connection to get a log file (stream) of
		 * the new DSMS instance that it is starting off.
		 */		
		char *getLogFilePref () const;

		/**
		 * Get an identifier for a new serverx
		 */
		
		int getNewServerId ();
		
		/**
		 * Called by a command connection to get the configuration file
		 * for a new DSMS server instance.
		 */
		char *getConfigFile () const;
		
		/**
		 * Create a new input connection.  Usage: A command connection
		 * creates a new input connection in response to a "register
		 * input" command from the client.  The comm. connection returns
		 * the identifier for the input connection (in_id) to the client,
		 * which later establishes a network connection with the input
		 * connection object using the in_id.
		 *
		 * @param conn_id    Identifier for the connection.  Can be used to
		 *                   retrieve the connection instance using
		 *                   getInputConn() method.
		 *
		 * @param conn     The new input connection.
		 */
		
		int createInputConn (int &conn_id, InputConnection *&conn);
		
		/**
		 * Retrieve a previously created input connection instance using
		 * its identifier.
		 */
		
		int getInputConn (int conn_id, InputConnection *&conn);
		
		/**
		 * Destroy an input connection identifier by conn_id
		 */
		int destroyInputConn (int conn_id);
		
		/** 
		 * Create a new output connection.  An output connection is used
		 * to communicate the results of a query to a remote client.
		 * Usage: a command connection creates an output connection in
		 * response to "register output" command from the client.  The
		 * command connection returns the identifer of the output
		 * connection to the client which uses the identifier to establish
		 * a connection with the output connection.
		 */
		int createOutputConn (int &conn_id, OutputConnection *&conn);
		
		/**
		 * Retrieve a previously created output connection instance
		 */		
		int getOutputConn (int conn_id, OutputConnection *&conn);
		
		/**
		 * Destroy an output connection identifier by conn_id
		 */
		int destroyOutputConn (int conn_id);
		
	private:
		
		/// Handle a new connection - start off a generic client thread
		int handleNewConn (int cli_sockfd);
	};
}

#endif
