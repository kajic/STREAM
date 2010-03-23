#include "queue.h"
#include "debug.h"

using namespace Network;
using std::endl;

#define INCR(s) ((s) = ((s) == lastSlot)? (buffer) : ((s)+objLen))

Queue::Queue (ostream &_LOG, int objLen)
	: LOG (_LOG)
{
	int numObjs;
	
	this -> objLen = objLen;
	
	numObjs = BUF_SIZE / objLen;
	if (numObjs <= 0) {		
		LOG << "Queue(constructor): objSize too large"
			<< endl;
	}
	
	nextReadSlot   = buffer;
	nextWriteSlot  = buffer;
	lastSlot       = buffer + objLen * (numObjs - 1);
	bEmpty         = true;
	bWriterBlocked = false;
	bReaderBlocked = false;
	
	pthread_mutex_init (&mutex, NULL);
	pthread_cond_init (&nonempty_cond, NULL);
	pthread_cond_init (&nonfull_cond, NULL);
}

Queue::~Queue ()
{	
	pthread_mutex_destroy (&mutex);
	pthread_cond_destroy (&nonempty_cond);
	pthread_cond_destroy (&nonfull_cond);
}

int Queue::getNextWriteSlot (char *&ptr)
{
	// The queue can be empty only if the read and write slot point to the
	// same location.
	ASSERT (!bEmpty || (nextReadSlot == nextWriteSlot));

	// I (writer) am here, so I am unblocked
	ASSERT (!bWriterBlocked);
	
	// We do have a free slot to write
	if ((nextReadSlot != nextWriteSlot) || bEmpty) {
		ptr = nextWriteSlot;
		return 0;
	}
	
	// The queue is full
	pthread_mutex_lock(&mutex);
	
	// Note: we have to check again if the queue is full!!!
	if ((nextReadSlot == nextWriteSlot) && !bEmpty) {
		
		// Set the indicator that we have blocked
		bWriterBlocked = true;
		
		// We wait until the queue becomes non-full
		pthread_cond_wait (&nonfull_cond, &mutex);

		// We are out of the block:
		bWriterBlocked = false;		
	}
	
	pthread_mutex_unlock(&mutex);
	
	// The queue has to be nonfull now
	ASSERT ((nextReadSlot != nextWriteSlot) || !bEmpty);
	
	ptr = nextWriteSlot;
	
	return 0;
}

int Queue::commitWrite (char *ptr)
{
	// The queue has to be non-full
	ASSERT (bEmpty || (nextReadSlot != nextWriteSlot));
	
	// I (writer) am here, so I am unblocked
	ASSERT (!bWriterBlocked);
	
	// ptr has to be the same as nextWriteSlot
	ASSERT (ptr == nextWriteSlot);

	// Critical code:
	pthread_mutex_lock(&mutex);
	
	// Queue can never be empty now
	bEmpty = false;
	
	// Go to the next available write slot
	INCR(nextWriteSlot);

	// Wake up any blocked readers
	if (bReaderBlocked) {
		pthread_cond_signal (&nonempty_cond);
	}
	
	pthread_mutex_unlock (&mutex);
	
	return 0;
}

int Queue::getNextReadSlot (char *&ptr)
{	
	// The queue can be empty only if the read and write slot point to the
	// same location.
	ASSERT (!bEmpty || (nextReadSlot == nextWriteSlot));
	
	// I am a reader and I am not blocked
	ASSERT (!bReaderBlocked);
	
	// The queue is not empty:
	if (!bEmpty) {
		ptr = nextReadSlot;
		return 0;
	}

	// The queue is empty
	pthread_mutex_lock(&mutex);
	
	// But we have to check it again!
	if (bEmpty) {
		
		// Indicate we are blocked
		bReaderBlocked = true;
		
		// Wait until the queue becomes nonempty
		pthread_cond_wait (&nonempty_cond, &mutex);
		
		// Out of the block
		bReaderBlocked = false;
	}
	
	pthread_mutex_unlock(&mutex);
	
	ASSERT (!bEmpty);
	
	ptr = nextReadSlot;
	return 0;
}

int Queue::commitRead (char *ptr)
{
	// The queue has to be nonempty
	ASSERT (!bEmpty);
	
	// I am a reader and I am not blocked, so:
	ASSERT (!bReaderBlocked);
	
	// ptr has to be the same as next read block
	ASSERT (ptr == nextReadSlot);
	
	// critical code
	pthread_mutex_lock(&mutex);
	
	// Update the nextReadSlot
	INCR (nextReadSlot);	
	
	// If the read slot and write slot are identical now, it can happen
	// only because the queue is empty.
	if (nextReadSlot == nextWriteSlot) {
		bEmpty = true;
	}
	
	// Wake up any writers blocked due to queue being full.
	if (bWriterBlocked) {
		pthread_cond_signal (&nonfull_cond);
	}
	
	pthread_mutex_unlock(&mutex);
	
	return 0;
}

bool Queue::isEmpty () const {
	return bEmpty;
}
