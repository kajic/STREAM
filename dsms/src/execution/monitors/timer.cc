#ifndef _TIMER_
#include "execution/monitors/timer.h"
#endif

extern unsigned int CPU_SPEED;

using namespace Monitor;

/**
 * Assembly code fragment from:
 * 
 * http://www.ncsa.uiuc.edu/UserInfo/Resources/Hardware/IA32LinuxCluster/Doc/timing.html
 * 
 * to get the wall clock time
 */ 

static unsigned long long int nanotime_ia32(void)
{
	unsigned long long int val;
	__asm__ __volatile__("rdtsc" : "=A" (val) : );
	return(val);
}

Timer::Timer ()
{
	totalTime = 0LL;
	startTime = 0LL;
	iCPS = 1.0 / (1.0 * CPU_SPEED * 1000 * 1000);
}

Timer::~Timer () {}

int Timer::reset ()
{
	totalTime = 0LL;
	return 0;
}

int Timer::start ()
{
	startTime = nanotime_ia32();
	return 0;
}

int Timer::stop ()
{
	unsigned long long int endTime;
	
	endTime = nanotime_ia32();
	totalTime += (endTime - startTime);
	
	return 0;
}

double Timer::getSecs ()
{
	return (totalTime * iCPS);
}
