#ifndef _PARAMS_
#define _PARAMS_

/// Size of the memory managed by MemoryManager that is available to the
/// execution units  
unsigned int MEMORY;

/// Default memory size = 64 MB
static const unsigned int MEMORY_DEFAULT = (1 << 20) * 64;

/// Memory allocated to a queue in number of pages
unsigned int QUEUE_SIZE;

/// Default queue size = 1 Page
static const unsigned int QUEUE_SIZE_DEFAULT = 1;

/// Memory allocated to a shared queue in number of pages
unsigned int SHARED_QUEUE_SIZE;

/// Default shared queue size = 30 Page
static const unsigned int SHARED_QUEUE_SIZE_DEFAULT = 30;

/// Threshold used for index bucket splitting
double INDEX_THRESHOLD;

static const double INDEX_THRESHOLD_DEFAULT = 0.85;

/// Number of iterations of the scheduler
long long int SCHEDULER_TIME;

static const long long int SCHEDULER_TIME_DEFAULT = 5000000 * 100;

/// The cpu speed MHz
int CPU_SPEED;

static const int CPU_SPEED_DEFAULT = 2000; // 2000 MHz

#endif
