#include <unistd.h>

#ifndef _IN_CONN_
#include "in_conn.h"
#endif

#ifndef _DEBUG_
#include "debug.h"
#endif

using namespace Network;
using namespace std;

InputConnection::InputConnection(ostream &_LOG)
	: LOG (_LOG)
{
	this -> sockfd    = -1;
	this -> bInit     = false;
	this -> queue     = 0;	
	this -> curPtr    = readBuf;
	this -> nbytes    = 0;
	this -> bEnd      = false;

	this -> b_wait_for_end = false;
	pthread_mutex_init (&mutex, NULL);
	pthread_cond_init (&wait_for_end_cond, NULL);
	
	return;
}

int InputConnection::setSocket (int fd)
{
	this -> sockfd = fd;
	return 0;
}

static bool emptyLine (const char *line)
{
	for (; *line && isspace (*line); line++);
	
	return (*line == '\0');
}

static const unsigned int MAX_LINE_SIZE =  1024;

void InputConnection::run()
{
	int rc;
	char *nextTupleSlot;
	char lineBuffer [MAX_LINE_SIZE];
	
	if (sockfd == -1) {
		LOG << "InputConnection: Socket not set"
			<< endl;
		return;
	}
	
	// Initialization routine gets the schema from the client, computes
	// tupleLen, and constructs 'queue'
	if ((rc = initialize ()) != 0) {
		LOG << "InputConnection: Error initializing"
			<< endl;
		return;
	}
	
	while (!beof) {		
		// Get the location to write out the next tuple
		if ((rc = queue -> getNextWriteSlot (nextTupleSlot)) != 0) {
			LOG << "InputConnection: Error getting a slot from queue"
				<< endl;
			return;
		}
		
		// Get the next line
		if ((rc = readline (lineBuffer, MAX_LINE_SIZE)) != 0) {
			LOG << "InputConnection: readline error"
				<< endl;
			return;
		}
		
		// Ignore empty lines
		if (emptyLine (lineBuffer)) 
			continue;		
		
		// Read the input network stream and construct the next tuple
		if ((rc = constructTuple (nextTupleSlot, lineBuffer)) != 0) {
			LOG << "InputConnectin:: Error constructing tuple"
				<< endl;
			return;
		}
		
		if ((rc = queue -> commitWrite (nextTupleSlot)) != 0) {
			LOG << "InputConnection: Error writing to queue" << endl;
			return;
		}		
	}
	
	ASSERT (!b_wait_for_end);	
	pthread_mutex_lock (&mutex);	
	if (!bEnd) {
		LOG << "Waiting for end()" << endl;
		b_wait_for_end = true;
		pthread_cond_wait (&wait_for_end_cond, &mutex);
		b_wait_for_end = false;
		ASSERT (bEnd);
		LOG << "end() called" << endl;
	}
	
	pthread_mutex_unlock (&mutex);	

	pthread_mutex_destroy (&mutex);
	pthread_cond_destroy (&wait_for_end_cond);
	
	LOG << "InputConnection: connection closed" << endl;
	
	return;
}

static const int SCHEMA_BUF_SIZE = 128;

int InputConnection::initialize ()
{
	int rc;
	char schemaBuf [SCHEMA_BUF_SIZE];

	beof = false;
	
	// The first message encodes the schema
	if ((rc = readline (schemaBuf, SCHEMA_BUF_SIZE)) != 0) {
		LOG << "read Error"
			<< endl;
		return rc;
	}
	
	// Parse the schema
	if ((rc = parseSchema (schemaBuf)) != 0) {
		LOG << "parse Error"
			<< endl;
		LOG << "Schema: " << schemaBuf << endl;
		return rc;
	}
	
	// compute offsets
	if ((rc = computeOffsets ()) != 0) {
		LOG << "offset computation Error"
			<< endl;
		return rc;
	}
	
	// Construct the tuple queue (we use one extra byte to encode
	// if this tuple is a heartbeat or not
	queue = new Queue (LOG, tupleLen+1);
		
	bInit = true;
	
	return 0;
}

int InputConnection::readline (char *lineBuf, int lineBufSize)
{
	int rc;

	for (int i = 0 ; i < lineBufSize ; i++) {
		
		ASSERT (curPtr <= readBuf + nbytes);
		
		// we don't have any unread data in the input buffer, recharge the
		// buffer
		if (curPtr - readBuf == nbytes) {
			
			// Load the buffer with input data
			if ((rc = loadBuf ()) != 0)
				return rc;
			
			// At the end of loading, curPtr should be at the beginning of
			// the buffer.  Also loadBuf reads 0 bytes only at the eof.
			ASSERT (curPtr == readBuf);			
			ASSERT (nbytes > 0 || beof);
			
			// We have reached the end of file: return whatever we have
			// got 
			if (beof) {
				*lineBuf = '\0';
				return 0;
			}
		}
		
		ASSERT (curPtr < readBuf + nbytes);
		
		// Eol
		if (*curPtr == '\n' || *curPtr == '\r') {
			*lineBuf = '\0';
			curPtr ++;
			return 0;
		}
		
		else {
			*lineBuf++ = *curPtr++;
		}
	}
	
	// linebuffer too small
	LOG << "readline: Input line too large" << endl;
	return -1;
}

int InputConnection::loadBuf ()
{
	ASSERT (curPtr == readBuf + nbytes);
	
	curPtr = readBuf;
	
	// try to read an entire chunk of input
	nbytes = read (sockfd, readBuf, READ_BUF_SIZE);

	// Error
	if (nbytes < 0) {
		LOG << "InputConnection: error reading from the socker"
			<< endl;
		return -1;
	}

	// Eof
	if (nbytes == 0) {
		beof = true;
	}
	
	return 0;
}

#include <iostream>
int InputConnection::parseSchema (char *ptr)
{	
	numAttrs = 0;	
	while (*ptr && numAttrs < MAX_ATTRS) {
		
		switch (*ptr++) {
		case 'i':
			
			attrTypes [numAttrs++] = INT;
			break;
			
		case 'f':
			
			attrTypes [numAttrs++] = FLOAT;
			break;
			
		case 'b':
			
			attrTypes [numAttrs++] = BYTE;
			break;
			
		case 'c':
			
			attrTypes [numAttrs] = CHAR;
			attrLen [numAttrs] = strtol (ptr, &ptr, 10);
			if (attrLen [numAttrs] <= 0)
				return -1;
			numAttrs ++;
			break;
			
		default:
			return -1;
		}
		
		if (*ptr && *ptr != ',')
			return -1;
		
		if (*ptr == ',')
			ptr ++;		
	}
	
	return 0;
}

int InputConnection::constructTuple (char *tupleBuf, char *lineBuffer)
{
	char *begin, *end;
	int ival;
	float fval;	
	bool bEnd;
	
	begin = end = lineBuffer;
	
	bEnd = false;
	int a;
	for (a = 0 ; (a < numAttrs) && !bEnd ; a++) {
		
		// Seek a comma
		for (; *end && *end != ',' ; end++)
			;
		
		// Empty attribute
		if (begin == end)
			return -1;
		
		if (*end == '\0') {
			bEnd = true;
		}
		
		*end = '\0';
		
		switch (attrTypes [a]) {
			
		case INT:
			
			ival = atoi (begin);
			memcpy (tupleBuf + offsets [a], &ival, sizeof (int));
			break;
			
		case FLOAT:
			
			fval = atof (begin);
			memcpy (tupleBuf + offsets [a], &fval, sizeof (float));
			break;
			
		case CHAR:
			
			strncpy (tupleBuf + offsets [a], begin, attrLen [a]);
			tupleBuf [offsets [a] + attrLen [a] - 1] = '\0';
			break;
			
		case BYTE:
			
			tupleBuf [offsets [a]] = *begin;
			break;
			
		default:
			
			ASSERT (0);
			break;
		}
		
		begin = ++end;
	}

	// Heartbeat contains only the timestamp and nothing else
	if (a == 1) {
		tupleBuf [tupleLen] = 'H';
	}

	// Normal tuple
	else if (a == numAttrs) {
		tupleBuf [tupleLen] = 'N';
	}
	
	else {
		LOG << "Malformed input tuple" << endl;
		return -1;
	}
	
	return 0;
}

int InputConnection::computeOffsets ()
{
	int offset = 0;

	for (int a = 0 ; a < numAttrs ; a++) {
		offsets [a] = offset;
		
		switch (attrTypes [a]) {
		case INT:
			offset += sizeof(int);
			break;

		case FLOAT:
			offset += sizeof(float);
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


int InputConnection::start ()
{
	lastTuple = 0;
	
	return 0;
}

int InputConnection::getNext (char *&tuple, unsigned int &len,
							  bool &bHeartbeat)
{
	int rc;

	// There are no tuples to get until the connection is properly
	// initialized by the client.
	if (!bInit) {
		bHeartbeat = false;
		tuple = 0;
		len   = 0;
		return 0;
	}
	
	// If we had returned a tuple in the last getNext() call, we commit
	// the read now.  This implies that the location pointed to by
	// lastTuple can now be overwritten by the queue.
	
	if (lastTuple) {
		
		if ((rc = queue -> commitRead (lastTuple)) != 0)
			return rc;
		
		lastTuple = 0;
	}
	
	// The queue containing the tuples is empty, so just return
	if (queue -> isEmpty ()) {
		bHeartbeat = false;
		tuple = 0;
		len   = 0;
		return 0;
	}
	
	// Get the next tuple.
	if ((rc = queue -> getNextReadSlot (tuple)) != 0)
		return rc;	

	// We use tuple[tupleLen] to store info about heartbeats/normal tuples
	ASSERT (tuple[tupleLen] == 'N' || tuple[tupleLen] == 'H');

	if (tuple[tupleLen] == 'H')
		bHeartbeat = true;
	else
		bHeartbeat = false;
	
	len = tupleLen;	
	lastTuple = tuple;
	
	return 0;
}

int InputConnection::end ()
{
	ASSERT (!bEnd);
	pthread_mutex_lock (&mutex);
	bEnd = true;
	
	if (b_wait_for_end)
		pthread_cond_signal (&wait_for_end_cond);
	
	pthread_mutex_unlock (&mutex);
	return 0;
}

#ifdef _TEST_
#include <iostream>
using namespace std;

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <time.h>


void *producer (void *c)
{	
	InputConnection *conn;
	
	conn = (InputConnection *)c;
	
	conn -> run ();
}

void *consumer (void *c)
{
	int rc;
	InputConnection *conn;
	struct timespec d, r;
	char *tuple;
	unsigned int len;
	int a, b;
	
	// Set the sleep duration (1 sec)
	d.tv_sec = 1;
	d.tv_nsec = 0;

	conn = (InputConnection *)c;

	if ((rc = conn -> start ()) != 0) {
		cout << "Error starting up the connection" << endl;
		pthread_exit (NULL);
	}

	while (true) {

		// Sleep for sometime
		if ((rc = nanosleep (&d, &r)) != 0) {
			
			if (errno != EINTR) {
				cout << "Consumer: Unexpected error in nanosleep"
					 << endl;
				pthread_exit (NULL);
			}			
		}
		
		while (true) {
			if ((rc = conn -> getNext (tuple, len)) != 0) {
				cout << "Consumer: error getting next tuple"
					 << endl;
				pthread_exit (NULL);
			}
			
			if (!tuple)
				break;

			if (len != 8) {
				cout << "Consumer: invalid len" << endl;
				pthread_exit (NULL);
			}
			
			memcpy (&a, tuple, sizeof(int));
			memcpy (&b, tuple + sizeof(int), sizeof(int));
			
			cout << a << "," << b << endl;
		}
	}	
}

static const char *file = "input.dat";

int main ()
{
	int rc;
	pthread_t prod_thread, cons_thread;
	InputConnection *conn;
	int sockfd;

	// Open the file
	sockfd = open (file, O_RDONLY);
	if (sockfd == -1) {
		cout << "Error opening the file" << endl;
		return 1;
	}

	// Input connection
	conn = new InputConnection (sockfd, cout);

	// producer thread
	if ((rc = pthread_create (&prod_thread, NULL, producer, (void*)conn))
		!= 0) {
		cout << "Error creating the producer thread" << endl;
		return 1;
	}

	// consumer thread
	if ((rc = pthread_create (&cons_thread, NULL, consumer, (void*)conn))
		!= 0) {
		cout << "Error creating the consumer thread" << endl;
		return 1;
	}
	
	// Wait for the producer and consumer to end
	pthread_join (prod_thread, NULL);
	pthread_join (cons_thread, NULL);

	delete conn;
	return 0;
}

#endif
