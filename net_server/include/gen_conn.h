#ifndef _GEN_CONN_
#define _GEN_CONN_

/**
 * @file        gen_conn.h
 * @date        Oct. 15, 2004
 * @brief       A generic connection to DSMS server
 */

#include <ostream>
using std::ostream;

#ifndef _THREAD_
#include "thread.h"
#endif

namespace Network {
    // forward decl.
	class NetworkManager;
	
	/**
	 * A generic connection.
	 *
	 * Every  connection  to the  server  starts  its  life as  a  generic
	 * connection.  Later it morphs into one  of 3 more specific types - a
	 * command connection, an input connection, or an output connection.
	 *
	 * The first message that a client sends to the generic connection
	 * should be a connection identifier which lets to generic connection
	 * to transform to one of the 3 connections.
	 *
	 */
	
	class GenericConnection : public Thread {
	private:
		/// Unique identifier for the connection		
		int id;
		
		/// Network manager who created us
		NetworkManager *netMgr;
		
		/// Event logger
		std::ostream &log;
		
		/// connection socket
		int sockfd;
		
		/// The three types of specific connections that this generic
		/// connection object morphs into.
		enum ConnType {
			COMMAND_CONN,
			INPUT_CONN,
			OUTPUT_CONN
		};
		
		/// Connection specification: connection type + some additional
		/// information.  This struct is used to exchange connection
		/// information from getConnectionSpec() and run() method.
		struct ConnSpec {
			ConnType type;
			unsigned int inputId;
			unsigned int outputId;
		};
		
	public:

		/**
		 * @param  thread_id  An identifier for the thread in which the
		 *                    connection runs.
		 * @param  netMgr     Network manager who constructed us
		 * @param  sockfd     Socket for the connection
		 * @param  LOG        System-wide log
		 */
		
		GenericConnection (int id,
						   NetworkManager *netMgr,
						   int sockfd,
						   ostream &LOG);
		
		/**
		 * The run method - the thread for the generic connection is
		 * started off on the run method.
		 *
		 * The run() method determines the type of the connection based on
		 * the first message from the client and then morphs the
		 * connection to the appropriate type.
		 */ 
		
		virtual void run();
		
	private:

		/**
		 * Called when the generic connection thread is run.  It gets the
		 * connection specification info, which is used to transform to
		 * one of the 3 connection types
		 */
		int getConnectionSpec (ConnSpec& spec);

		/**
		 * Morph to a command connection
		 */		
		int morphToCommandConn ();
		
		/**
		 * Morph to an input connection
		 */		
		int morphToInputConn (unsigned int inputId);

		/**
		 * Morph to an output connection
		 */		
		int morphToOutputConn (unsigned int outputId);
		
		/**
		 * Terminate the connection: called only on error
		 */
		void terminate();
		
		ostream &log_with_id ();
	};
}

#endif
