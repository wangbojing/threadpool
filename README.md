# threadpool

thread_pool_simple.c : 简单的线程池
gcc -o thread_pool_simple thread_pool_simple.c -lpthread

thread_pool_active.c : 复杂的线程池，可以直接复用的。
gcc -o thread_pool_active thread_pool_active.c -lpthread
