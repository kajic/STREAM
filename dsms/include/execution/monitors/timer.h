#ifndef _TIMER_
#define _TIMER_

#include <sys/time.h>

namespace Monitor {
	class Timer {
	private:
		
		/// Manually hardcoded clock frequency in Hz (Sponge)
		//static const double CPS = 2800000000.0;
		
		double iCPS;
		
		/// Total time measured by the timer
		unsigned long long int totalTime;
		
		/// The most recent timer start time
		unsigned long long int startTime;
		
	public:
		Timer ();
		~Timer ();

		/**
		 * Reset the timer to zero
		 */ 
		int reset ();
		
		/**
		 * Start the timer.
		 */ 
		int start ();
		
		/** 
		 * Stop the timer.
		 */ 
		
		int stop ();
		
		/**
		 * Return the total measured by timer in seconds.
		 */ 
		double getSecs ();
	};
}

#endif
