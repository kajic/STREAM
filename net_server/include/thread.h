#ifndef _THREAD_
#define _THREAD_

/**
 * Abstract superclass of all threads
 */

class Thread {
 public:
	virtual ~Thread() {}
	virtual void run() = 0;
};

/**
 * Start off a thread
 */ 
void *start_thread (void *thread);

#endif
