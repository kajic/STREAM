#ifndef _COMMAND_CONN_
#define _COMMAND_CONN_

#ifndef _THREAD_
#include "thread.h"
#endif

#ifndef _IN_CONN_
#include "in_conn.h"
#endif

#ifndef _OUT_CONN_
#include "out_conn.h"
#endif

#ifndef _SERVER_
#include "interface/server.h"
#endif

#include <pthread.h>

#include <ostream>
using namespace std;

namespace Network {
	class NetworkManager;

	/// List of commands
	enum Command {
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
	
	class CommandConnection : public Thread {
	private:
		/// Unique for this connection
		int id;
		
		/// Event log stream
		ostream &logStream;
		
		/// Socket for the TCP connection
		int sockfd;

		/// has the command connection terminated?
		bool bEnd;
		
		/// Network manager who created us
		NetworkManager *net_mgr;
		
		/// DSMS server
		Server *server;

		/// Server thread (once it starts executing)
		pthread_t server_thread;
		
		/// Has the server started (serverThread created)?
		bool b_server_started;
		
		/// maximum nmber of output connections per comm conn
		static const int MAX_OUTPUT_CONN = 20;
		
		/// The active output connections of this command connection
		OutputConnection *out_conns [MAX_OUTPUT_CONN];
		
		/// The number of active output connections
		int num_out_conns;		
		
		/// Maximum no of input conns per comm conn
		static const int MAX_INPUT_CONN = 20;
		
		/// The active input connections of this command connection
		InputConnection *in_conns [MAX_INPUT_CONN];
		
		/// The number of active input conections
		int num_in_conns;
		
		/// Scratch space used for messages
		static const int MSG_BUF_LEN = 2048;
		
		char msg_buf [MSG_BUF_LEN];
		int msg_len;
		
		/// Space used for encoding plans and schema
		static const int PLAN_BUF_LEN = (1 << 20);
		
		/// 1 MB
		char planBuf [PLAN_BUF_LEN];
		
	public:
		CommandConnection (int id,
						   int sockfd,
						   NetworkManager *net_mgr,
						   std::ostream &LOG);
		
		virtual void run();
		
	private:
		
		int get_command (Command &command);
		int handle_command (Command command);
		
		int handle_begin_app();
		int handle_reg_input();
		int handle_reg_query();
		int handle_reg_out_query();
		int handle_reg_view();
		int handle_end_app();
		int handle_gen_plan();
		int handle_execute();
		int handle_reset ();
		int handle_reg_mon();	
		int handle_terminate ();
		
		int initServer ();

		int get_id (int &);
		int get_mesg (char *, int, int&);
		int send_error_code (int);
		int send_id (int);
		int send_mesg (char *msg, int msg_len);
		
		ostream &log_with_id();
	};
}

#endif
