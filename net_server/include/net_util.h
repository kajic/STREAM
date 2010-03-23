#ifndef _NET_UTIL_
#define _NET_UTIL_

namespace Network {
	class NetUtil {
	public:
		// Read a message from a socket [[ what is a message ]]
		static int getMessage(int   sockfd,
							  char *msgBuf,
							  int   msgBufLen,
							  int  &msgLen);
		
		static int sendErrorCode (int sockfd, int errorCode);
		
		static int sendMessage (int   sockfd,
								char *msgBuf,
								int   msgLen);
	};
}
#endif
