/*
 * Copyright (C) 2018 DataJaguar, Inc.
 *
 * This file is part of JaguarDB.
 *
 * JaguarDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * JaguarDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with JaguarDB (LICENSE.txt). If not, see <http://www.gnu.org/licenses/>.
 */

#ifndef _THPOOL_
#define _THPOOL_

/* Binary semaphore */
typedef struct bsem {
	pthread_mutex_t mutex;
	pthread_cond_t   cond;
	int v;
} bsem;


/* Job */
typedef struct job{
	struct job*  prev;                   /* pointer to previous job   */
	void*  (*function)(void* arg);       /* function pointer          */
	void*  arg;                          /* function's argument       */
} job;


/* Job queue */
typedef struct jobqueue{
	pthread_mutex_t rwmutex;             /* used for queue r/w access */
	job  *front;                         /* pointer to front of queue */
	job  *rear;                          /* pointer to rear  of queue */
	bsem *has_jobs;                      /* flag as binary semaphore  */
	int   len;                           /* number of jobs in queue   */
} jobqueue;


/* Thread */
typedef struct thread{
	int       id;                        /* friendly id               */
	pthread_t pthread;                   /* pointer to actual thread  */
	struct thpool_* thpool_p;            /* access to thpool          */
} thread;


/* Threadpool */
typedef struct thpool_{
	thread**   threads;                  /* pointer to threads        */
	volatile int num_threads_alive;      /* threads currently alive   */
	volatile int num_threads_working;    /* threads currently working */
	pthread_mutex_t  thcount_lock;       /* used for thread count etc */
	pthread_cond_t  threads_all_idle;    /* signal to thpool_wait     */
	jobqueue*  jobqueue_p;               /* pointer to the job queue  */    

    volatile int threads_keepalive;
    //volatile int threads_on_hold;

} thpool_;

typedef struct thpool_* threadpool;



/**
 * @brief  Initialize threadpool
 * 
 * Initializes a threadpool. This function will not return untill all
 * threads have initialized successfully.
 * 
 * @example
 * 
 *    ..
 *    threadpool thpool;                     //First we declare a threadpool
 *    thpool = thpool_init(4);               //then we initialize it to 4 threads
 *    ..
 * 
 * @param  num_threads   number of threads to be created in the threadpool
 * @return threadpool    created threadpool on success,
 *                       NULL on error
 */
threadpool thpool_init(int num_threads);


/**
 * @brief Add work to the job queue
 * 
 * Takes an action and its argument and adds it to the threadpool's job queue.
 * If you want to add to work a function with more than one arguments then
 * a way to implement this is by passing a pointer to a structure.
 * 
 * NOTICE: You have to cast both the function and argument to not get warnings.
 * 
 * @example
 * 
 *    void print_num(int num){
 *       printf("%d\n", num);
 *    }
 * 
 *    int main() {
 *       ..
 *       int a = 10;
 *       thpool_add_work(thpool, (void*)print_num, (void*)a);
 *       ..
 *    }
 * 
 * @param  threadpool    threadpool to which the work will be added
 * @param  function_p    pointer to function to add as work
 * @param  arg_p         pointer to an argument
 * @return 0 on successs, -1 otherwise.
 */
int thpool_add_work(threadpool, void *(*function_p)(void*), void* arg_p);


/**
 * @brief Wait for all queued jobs to finish
 * 
 * Will wait for all jobs - both queued and currently running to finish.
 * Once the queue is empty and all work has completed, the calling thread
 * (probably the main program) will continue.
 * 
 * Smart polling is used in wait. The polling is initially 0 - meaning that
 * there is virtually no polling at all. If after 1 seconds the threads
 * haven't finished, the polling interval starts growing exponentially 
 * untill it reaches max_secs seconds. Then it jumps down to a maximum polling
 * interval assuming that heavy processing is being used in the threadpool.
 *
 * @example
 * 
 *    ..
 *    threadpool thpool = thpool_init(4);
 *    ..
 *    // Add a bunch of work
 *    ..
 *    thpool_wait(thpool);
 *    puts("All added work has finished");
 *    ..
 * 
 * @param threadpool     the threadpool to wait for
 * @return nothing
 */
void thpool_wait(threadpool);


/**
 * @brief Pauses all threads immediately
 * 
 * The threads will be paused no matter if they are idle or working.
 * The threads return to their previous states once thpool_resume
 * is called.
 * 
 * While the thread is being paused, new work can be added.
 * 
 * @example
 * 
 *    threadpool thpool = thpool_init(4);
 *    thpool_pause(thpool);
 *    ..
 *    // Add a bunch of work
 *    ..
 *    thpool_resume(thpool); // Let the threads start their magic
 * 
 * @param threadpool    the threadpool where the threads should be paused
 * @return nothing
 */
void thpool_pause(threadpool);


/**
 * @brief Unpauses all threads if they are paused
 * 
 * @example
 *    ..
 *    thpool_pause(thpool);
 *    thpool_resume(thpool);
 *    ..
 * 
 * @param threadpool     the threadpool where the threads should be unpaused
 * @return nothing
 */
void thpool_resume(threadpool);


/**
 * @brief Destroy the threadpool
 * 
 * This will wait for the currently active threads to finish and then 'kill'
 * the whole threadpool to free up memory.
 * 
 * @example
 * int main() {
 *    threadpool thpool1 = thpool_init(2);
 *    threadpool thpool2 = thpool_init(2);
 *    ..
 *    thpool_destroy(thpool1);
 *    ..
 *    return 0;
 * }
 * 
 * @param threadpool     the threadpool to destroy
 * @return nothing
 */
void thpool_destroy(threadpool);

class JaguarThreadPool
{
  public:
	JaguarThreadPool( int numThreads = -1 );
	~JaguarThreadPool();

	int addWork( void *(*functionp)(void*), void* arg);
	void wait();

  protected:
 	thpool_ *_pool; 

};


#endif
