#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#ifndef _NET_UTIL_
#include "net_util.h"
#endif

#ifndef _DEBUG_
#include "debug.h"
#endif

#include <iostream>
using namespace std;
using namespace Network;

// read exactly nbytes into the buffer
static int readn(int sockfd, char *buffer, unsigned int nbytes);
static int writen(int sockfd, char *buffer, unsigned int nbytes);
	
int NetUtil::getMessage(int   sockfd,
						char *msgBuf,
						int   msgBufLen,
						int  &msgLen)
{
	int rc;
	int  net_msgLen;
	
	// read first four bytes
	if ((rc = readn(sockfd, (char *)&net_msgLen, sizeof(int))) != 0) 
		return rc;
	
	// network -> host  order
	msgLen = ntohl(net_msgLen);
	
	// we don't have space
	if ((msgLen < 0) || (msgLen > msgBufLen - 1))
		return -1;
	
	// read the message
	if ((rc = readn(sockfd, msgBuf, msgLen)) != 0)
		return rc;
	
	// discount the null
	msgLen--;
	
	ASSERT (msgBuf [msgLen] == '\0');	
	
	return 0;	
}

/// Ugly !
static char errorBuf[100];

int NetUtil::sendErrorCode (int sockfd, int errorcode)
{
	
	sprintf(errorBuf, "%d", errorcode);
	
	return sendMessage(sockfd, errorBuf, strlen(errorBuf));	
}

int NetUtil::sendMessage(int sockfd, char *msgBuf,
						 int msgLen)
{
	int rc;
	int net_msgLen;

	net_msgLen = htonl(msgLen + 1);

	if ((rc = writen(sockfd, (char *)&net_msgLen, sizeof(int))) != 0)
		return rc;

	ASSERT (msgBuf [msgLen] == '\0');
	
	if ((rc = writen(sockfd, msgBuf, msgLen + 1)) != 0)
		return rc;

	return 0;
}



static int readn(int sockfd, char *buffer, unsigned int nbytes)
{
	int nread, n;

	nread = 0;
	while(nread < (int)nbytes) {
		n = read(sockfd, buffer + nread, nbytes - nread);

		ASSERT (n <= (int)(nbytes - nread));
		
		// error
		if (n <= 0)
			return -1;
		
		nread += n;
	}

	return 0;
}

static int writen(int sockfd, char *buffer, unsigned int nbytes)
{
	int nwritten, n;

	nwritten = 0;
	while(nwritten < (int) nbytes) {
		n = write(sockfd, buffer + nwritten, nbytes - nwritten);

		ASSERT (n <= (int)(nbytes - nwritten));
		
		if (n <= 0)
			return -1;
		nwritten += n;
	}

	return 0;
}
