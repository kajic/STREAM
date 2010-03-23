#ifndef _OUT_CONN_
#include "out_conn.h"
#endif

#include <unistd.h>
#include <time.h>
#include <errno.h>

#include "debug.h"
#include "net_util.h"

using namespace Network;
using namespace std;

OutputConnection::OutputConnection (std::ostream& _LOG)
	: LOG (_LOG)
{
	this -> sockfd = -1;
	this -> read_ptr = 0;
	this -> write_ptr = 0;
	this -> bEnd = false;
	this -> b_prod_waiting = false;
	
	pthread_mutex_init (&mutex, NULL);
	pthread_cond_init (&prod_wait_cond, NULL);

	totalBytes = 0;
}

OutputConnection::~OutputConnection ()
{
	pthread_mutex_destroy (&mutex);
	pthread_cond_destroy (&prod_wait_cond);
}

int OutputConnection::setSocket (int fd)
{
	this -> sockfd = fd;
	return 0;
}

// Consumer
void OutputConnection::run()
{
	int rc;
	struct timespec d, r;
	
	// Check if the socket is set
	if (sockfd == -1) {
		LOG << "OutputConnection: Socket not set" << endl;
		return;
	}
	
	// Set the sleep duration (0.1 sec)
	d.tv_sec = 0;
	d.tv_nsec = 100000000;	
	
	while (!bEnd) {
		
		// Sleep for some duration: It is not critical that we sleep for
		// exactly duration 'd' - so EINTR (error interrupt) is ok
		if ((rc = nanosleep (&d, &r)) != 0) {
			
			if (errno != EINTR) {
				LOG << "OutputConnection: Unexpected error in nanosleep"
					<< endl;
				return;
			}
		}
		
		// There is some data in the buffer: send it out to the client
		if (!buf_empty ()) {
			
			if ((rc = consumeBuf ()) != 0) {
				LOG << "OutputConnection: Error sending output"
					<< endl;
				return;
			}
		}
		
		// We wake up the producer if he is waiting for buffer space
		pthread_mutex_lock (&mutex);
		
		if (b_prod_waiting && !buf_full ()) {
			pthread_cond_signal (&prod_wait_cond);
		}
		
		pthread_mutex_unlock (&mutex);
	}

	// There is (still) some data in the buffer: send it out to the client
	if (!buf_empty ()) {
		
		if ((rc = consumeBuf ()) != 0) {
			LOG << "OutputConnection: Error sending output"
				<< endl;
			return;
		}		
	}
	
	return;
}

int OutputConnection::consumeBuf ()
{
	int rc;
	int nbytes;
	
	ASSERT (read_ptr != write_ptr);
	
	// Note: write_ptr is being updated concurrently
	nbytes = write_ptr - read_ptr;
	
	if (nbytes > 0) {
		ASSERT (nbytes + read_ptr <= BUF_SIZE);
		
		if ((rc = consumeBuf (shared_buf + read_ptr,
							  nbytes)) != 0)
			return rc;
	}
	
	else {
		
		ASSERT (nbytes < 0);
		ASSERT (nbytes + read_ptr >= 0);
		
		if ((rc = consumeBuf (shared_buf + read_ptr,
							  BUF_SIZE - read_ptr)) != 0)
			return rc;
		
		if ((rc = consumeBuf (shared_buf, nbytes + read_ptr)) != 0)
			return rc;				
	}

	read_ptr += nbytes;
	
	return 0;
}

#ifdef _TEST_
#include <iostream>
int OutputConnection::consumeBuf (char *buf, int size)
{
	for (int c = 0 ; c < size ; c++)
		std::cout << buf [c];

	return 0;
}

#else
int OutputConnection::consumeBuf (char *buf, int size)
{	
	int nleft, nwritten;
	
	nleft = size;

	/// debug
	totalBytes += size;
	//LOG << "Sending out: " << size << " bytes" << endl;
	//LOG << "Total sent: " << totalBytes << endl; 
	
	while (nleft > 0) {

		nwritten = write (sockfd, buf, nleft);
		
		if (nwritten <= 0) {
			return -1;
		}
		
		nleft -= nwritten;
		buf += nwritten;
	}
	
	return 0;
}
#endif

int OutputConnection::setNumAttrs (unsigned int numAttrs)
{
	if (numAttrs > MAX_ATTRS)
		return -1;
	
	this -> numAttrs = numAttrs;
	return 0;
}

int OutputConnection::setAttrInfo (unsigned int attrPos,
								   Type attrType, unsigned len)
{
	if ((int)attrPos >= numAttrs)
		return -1;

	attrTypes [attrPos] = attrType;
	attrLen [attrPos] = len;

	return 0;
}
	
int OutputConnection::start ()
{
	int offset = 0;
	
	if (numAttrs == 0)
		return -1;


	tstampOffset = offset;
	offset += TIMESTAMP_SIZE;

	signOffset = offset;
	offset += 1;
	
	for (int a = 0 ; a < numAttrs ; a++) {

		offsets [a] = offset;

		switch (attrTypes [a]) {
		case INT:
			offset += INT_SIZE;
			break;

		case FLOAT:
			offset += FLOAT_SIZE;
			break;

		case CHAR:
			offset += attrLen [a];
			break;

		case BYTE:
			offset ++;
			break;

		default:
			return -1;
		}
	}

	tupleLen = offset;
	
	return 0;
}

int OutputConnection::putNext (const char *tuple, unsigned int len)
{
	int       rc;
	Timestamp timestamp;
	int       ival;
	float     fval;	
	char      *ptr;
	int       nwritten, nfree;
	
	if ((int)len < tupleLen)
		return -1;

	// number of free bytes left in the line buffer
	nfree = LINE_BUF_SIZE;
	
	ptr = lineBuf;

	//----------------------------------------------------------------------
	// timestamp

	// We don't have enough space
	if (nfree <= 1)
		return -1;
	
	memcpy (&timestamp, tuple + tstampOffset, TIMESTAMP_SIZE);	
	ival = (int)timestamp;
	
	nwritten = snprintf (ptr, nfree, "%d,", ival);	
	ASSERT (nwritten < nfree);		
	nfree -= nwritten;
	ptr += nwritten;
	
	//
	//----------------------------------------------------------------------
	
	//----------------------------------------------------------------------
	// sign
	
	// We don't have enough space
	if (nfree <= 1)
		return -1;
	
	nwritten = snprintf (ptr, nfree, "%c,", tuple[signOffset]);
	ASSERT (nwritten < nfree);		
	nfree -= nwritten;
	ptr += nwritten;
	
	//
	//----------------------------------------------------------------------
	
	//----------------------------------------------------------------------
	// attributes:
	
   	
	for (int a = 0 ; a < numAttrs ; a++) {
		
		// ran out of space
		if (nfree <= 1)
			return -1;
		
		switch (attrTypes [a]) {
		case INT:
			memcpy (&ival, tuple + offsets [a], sizeof(int));
			
			nwritten = snprintf (ptr, nfree, "%d", ival);
			ASSERT (nwritten < nfree);			
			nfree -= nwritten;
			ptr += nwritten;

			break;
			
		case FLOAT:
			
			memcpy (&fval, tuple + offsets [a], sizeof(float));
			
			nwritten = snprintf (ptr, nfree, "%f", fval);
			ASSERT (nwritten < nfree);			
			nfree -= nwritten;
			ptr += nwritten;
			
			break;
			
		case CHAR:
			
			nwritten = snprintf (ptr, nfree, "%s", tuple + offsets[a]);			
			ASSERT (nwritten < nfree);
			nfree -= nwritten;
			ptr += nwritten;

			break;
			
		case BYTE:

			nwritten = snprintf (ptr, nfree, "%c", tuple [offsets [a]]);
			ASSERT (nwritten < nfree);
			nfree -= nwritten;
			ptr += nwritten;
			
			break;
			
		default:
			ASSERT (0);
			break;
		}
		
		if (a < numAttrs - 1) {
			
			if (nfree <= 1)
				return -1;
			
			nwritten = snprintf (ptr, nfree, ",");
			ASSERT (nwritten < nfree);
			nfree -= nwritten;
			ptr += nwritten;
		}
	}
	
	// Assert: lineBuf is null terminated
	if ((rc = writeln(lineBuf)) != 0)
		return rc;
	
	return 0;
}

int OutputConnection::end ()
{
	ASSERT (!b_prod_waiting);
	
	bEnd = true;
	return 0;
}

int OutputConnection::writeln (char *line)
{
	int rc;
	
	for (; *line ; line++)
		if ((rc = writechar (*line)) != 0)
			return rc;

	if ((rc = writechar ('\n')) != 0)
		return rc;
	return 0;
}


#define INCR(p) { if ((++(p)) == BUF_SIZE) (p) = 0;}

int OutputConnection::writechar (char c)
{
	ASSERT(!b_prod_waiting);
	
	if (buf_full ()) {
		pthread_mutex_lock (&mutex);
		
		b_prod_waiting = true;
		pthread_cond_wait (&prod_wait_cond, &mutex);
		b_prod_waiting = false;
		
		pthread_mutex_unlock (&mutex);
	}
	
	ASSERT (!buf_full());

	shared_buf [write_ptr] = c;
	INCR (write_ptr);
	
	return 0;	
}

bool OutputConnection::buf_empty () const
{
	return (read_ptr == write_ptr);
}

bool OutputConnection::buf_full () const
{
	return (((write_ptr == BUF_SIZE - 1) &&
			 (read_ptr == 0)) ||
			(write_ptr + 1 == read_ptr));
}

#ifdef _TEST_
#include <iostream>
using namespace std;

void *producer (void *c)
{
	int rc;
	char tuple [sizeof(int) * 2 + 1];
	
	OutputConnection *conn = (OutputConnection*)c;

	if ((rc = conn -> setNumAttrs (1)) != 0) {
		cout << "Error setting num attrs" << endl;
		pthread_exit (NULL);
	}
		
	if ((rc = conn -> setAttrInfo (0, INT, sizeof(int))) != 0) {
		cout << "Error setting attr info" << endl;
		pthread_exit (NULL);
	}		
	
	if ((rc = conn -> start ()) != 0) {		
		cout << "Error starting the connection" << endl;
		pthread_exit (NULL);
	}
	
	for (int t = 0 ; t < 20000 ; t++) {
		memcpy (tuple, &t, sizeof(int));
		tuple [sizeof(int)] = '+';
		memcpy (tuple + sizeof(int) + 1, &t, sizeof(int));
		
		if ((rc = conn -> putNext (tuple, sizeof(int)*2+1)) != 0) {
			cout << "Error starting the connection" << endl;
		}		
	}

	if ((rc = conn -> end ()) != 0) {
		cout << "Error ending" << endl;
	}
	
	pthread_exit (NULL);
}

void *consumer (void *c)
{
	OutputConnection *conn = (OutputConnection*)c;

	conn -> run ();
}

int main ()
{
	int rc;
	OutputConnection *outConn;
	pthread_t prod_thread, cons_thread;
	
	outConn = new OutputConnection (0, cout);

	if ((rc = pthread_create (&prod_thread, NULL, producer,
							  (void*)outConn)) != 0)
		return rc;

	if ((rc = pthread_create (&cons_thread, NULL, consumer,
							  (void*)outConn)) != 0)
		return rc;
	
	pthread_join (prod_thread, NULL);
	pthread_join (cons_thread, NULL);
	return 0;
}

#endif
