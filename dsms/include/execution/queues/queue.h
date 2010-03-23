#ifndef _QUEUE_
#define _QUEUE_

/**
 * @file         queue.h
 * @date         May 30, 2004
 * @brief        Queue interface, as seen by operators.
 */

#ifndef _ELEMENT_
#include "execution/queues/element.h"
#endif

#ifdef _MONITOR_
#ifndef _QUEUE_MONITOR_
#include "execution/monitors/queue_monitor.h"
#endif
#endif

/**
 * Queues are entities that connect different operators and define the
 * paths through which tuples flow in the system.  
 * 
 * Conceptualy, each queue has one operator inserting elements into the
 * queue, and one operator reading off elements from the queue.
 */

namespace Execution {

#ifdef _MONITOR_
	class Queue : public Monitor::QueueMonitor {
#else
	class Queue {
#endif
	public:
		virtual ~Queue () {}
		
		/**
		 * Enqueue a new element into the queue.
		 * 
		 * @param  element     The element to be enqueued
		 * @return             True if enqueue succeeded, false otherwise
		 */
		virtual bool enqueue (Element element) = 0;
		
		/**
		 * Dequeue the oldest element from the queue.
		 * 
		 * @param  element     (output) The dequeued element.
		 * @return             True if an element was dequeued, false
		 *                     otherwise 
		 */
		virtual bool dequeue (Element &element) = 0;
		
		/**
		 * Peek @ the oldest element without dequeueing.
		 * 
		 * @param element     (output) the oldest element
		 * @return            True if queue is nonempty, false otherwise
		 */ 
		virtual bool peek (Element &element) const = 0;
		
		/**
		 * @return true if the queue is full, false otherwise.  
		 */
		virtual bool isFull () const = 0;
		
		/**
		 * @return true if the queue is empty, false otherwise.
		 */
		virtual bool isEmpty () const = 0;
	};
}

#endif
