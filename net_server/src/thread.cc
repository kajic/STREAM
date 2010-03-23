#include "thread.h"

void *start_thread(void *thread)
{
	((Thread *)thread) -> run();
	return 0;
}
