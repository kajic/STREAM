#include "server/server_impl.h"

#include <ostream>

Server *Server::newServer(std::ostream& LOG) 
{
	return new ServerImpl(LOG);
}
