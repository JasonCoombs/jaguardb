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
#include <JagGlobalDef.h>

/* ********************************
 * Author:       Johan Hanssen Seferidis
 * License:	     MIT
 * Description:  Library providing a threading pool where you can add
 *               work. For usage, check the thpool.h file or README.md
 *
 *//** @file thpool.h *//*
 * 
 ********************************/
#include <unistd.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <errno.h>
#include <time.h> 

#if defined(__linux__)
#include <sys/prctl.h>
#endif

#include <JagThreadPool.h>
#include <JagUtil.h>
#include <JagSystem.h>

#ifdef THPOOL_DEBUG
#define THPOOL_DEBUG 1
#else
#define THPOOL_DEBUG 0
#endif

//static volatile int threads_keepalive;
//static volatile int threads_on_hold;

static int  thread_init(thpool_* thpool_p, struct thread** thread_p, int id);
static void* thread_do(struct thread* thread_p);
//static void  thread_hold();
static void  thread_destroy(struct thread* thread_p);

static int   jobqueue_init(thpool_* thpool_p);
static void  jobqueue_clear(thpool_* thpool_p);
static void  jobqueue_push(thpool_* thpool_p, struct job* newjob_p);
static struct job* jobqueue_pull(thpool_* thpool_p);
static void  jobqueue_destroy(thpool_* thpool_p);

static void  bsem_init(struct bsem *bsem_p, int value);
static void  bsem_reset(struct bsem *bsem_p);
static void  bsem_post(struct bsem *bsem_p);
static void  bsem_post_all(struct bsem *bsem_p);
static void  bsem_wait(struct bsem *bsem_p);



/* Initialise thread pool */
struct thpool_* thpool_init(int num_threads)
{
	//threads_on_hold   = 0;
	//threads_keepalive = 1;

	if (num_threads < 0){
		num_threads = 0;
	}

	//printf("s4485 thpool_init num_threads=%d\n", num_threads );
	//fflush(stdout );

	/* Make new thread pool */
	thpool_* thpool_p;
	thpool_p = (struct thpool_*)malloc(sizeof(struct thpool_));
	if (thpool_p == NULL){
		fprintf(stderr, "thpool_init(): Could not allocate memory for thread pool\n");
		return NULL;
	}
	thpool_p->num_threads_alive   = 0;
	thpool_p->num_threads_working = 0;

	//thpool_p->threads_on_hold = 0;
	thpool_p->threads_keepalive = 1;

	/* Initialise the job queue */
	if (jobqueue_init(thpool_p) == -1){
		fprintf(stderr, "thpool_init(): Could not allocate memory for job queue\n");
		if ( thpool_p ) free(thpool_p);
		thpool_p = NULL;
		return NULL;
	}

	/* Make threads in pool */
	thpool_p->threads = (struct thread**)malloc(num_threads * sizeof(struct thread *));
	if (thpool_p->threads == NULL){
		fprintf(stderr, "thpool_init(): Could not allocate memory for threads\n");
		jobqueue_destroy(thpool_p);
		if ( thpool_p->jobqueue_p ) free(thpool_p->jobqueue_p);
		if ( thpool_p ) free(thpool_p);
		thpool_p->jobqueue_p = NULL;
		thpool_p = NULL;
		return NULL;
	}

	pthread_mutex_init(&(thpool_p->thcount_lock), NULL);
	pthread_cond_init(&thpool_p->threads_all_idle, NULL);
	
	/* Thread init */
	int n;
	for (n=0; n<num_threads; n++){
		thread_init(thpool_p, &thpool_p->threads[n], n);
		if (THPOOL_DEBUG)
			printf("THPOOL_DEBUG: Created thread %d in pool \n", n);
	}
	
	/* Wait for threads to initialize */
	while (thpool_p->num_threads_alive != num_threads) {}

	return thpool_p;
}


/* Add work to the thread pool */
int thpool_add_work(thpool_* thpool_p, void *(*function_p)(void*), void* arg_p){
	job* newjob;

	newjob=(struct job*)malloc(sizeof(struct job));
	if (newjob==NULL){
		fprintf(stderr, "thpool_add_work(): Could not allocate memory for new job\n");
		return -1;
	}

	/* add function and argument */
	newjob->function=function_p;
	newjob->arg=arg_p;

	/* add job to queue */
	pthread_mutex_lock(&thpool_p->jobqueue_p->rwmutex);
	jobqueue_push(thpool_p, newjob);
	pthread_mutex_unlock(&thpool_p->jobqueue_p->rwmutex);

	return 0;
}


/* Wait until all jobs have finished */
void thpool_wait(thpool_* thpool_p){
	pthread_mutex_lock(&thpool_p->thcount_lock);
	while (thpool_p->jobqueue_p->len || thpool_p->num_threads_working) {
		pthread_cond_wait(&thpool_p->threads_all_idle, &thpool_p->thcount_lock);
	}
	pthread_mutex_unlock(&thpool_p->thcount_lock);
}


/* Destroy the threadpool */
void thpool_destroy(thpool_* thpool_p){
	/* No need to destory if it's NULL */
	if (thpool_p == NULL) return ;

	volatile int threads_total = thpool_p->num_threads_alive;

	/* End each thread 's infinite loop */
	// threads_keepalive = 0;
	thpool_p->threads_keepalive = 0;
	
	/* Give one second to kill idle threads */
	time_t start, end;
	double tpassed = 0.0;
	double TIMEOUT = 1.0;
	time (&start);
	while (tpassed < TIMEOUT && thpool_p->num_threads_alive){
		bsem_post_all(thpool_p->jobqueue_p->has_jobs);
		time (&end);
		tpassed = difftime(end,start);
	}

	//printf("s5503 in pool destroy 111\n"); fflush( stdout );
	
	/* Poll remaining threads */
	while (thpool_p->num_threads_alive){
		bsem_post_all(thpool_p->jobqueue_p->has_jobs);
		jagsleep(1, JAG_SEC);
	}
	//printf("s5504 in pool destroy 222\n"); fflush( stdout );

	/* Job queue cleanup */
	jobqueue_destroy(thpool_p);
	//printf("s5505 in pool destroy 333\n"); fflush( stdout );

	if ( thpool_p->jobqueue_p ) free(thpool_p->jobqueue_p);
	thpool_p->jobqueue_p = NULL;
	
	/* Deallocs */
	int n;
	for (n=0; n < threads_total; n++){
		thread_destroy(thpool_p->threads[n]);
	}
	//printf("s5506 in pool destroy 444\n"); fflush( stdout );

	if ( thpool_p->threads ) {
		free(thpool_p->threads);
	}
	thpool_p->threads = NULL;

	if ( thpool_p ) {
		free(thpool_p);
	}
	thpool_p = NULL;

	//printf("s5507 in pool destroy 555\n"); fflush( stdout );
}


/* Pause all threads in threadpool */
/***
void thpool_pause(thpool_* thpool_p) {
	int n;
	for (n=0; n < thpool_p->num_threads_alive; n++){
		pthread_kill(thpool_p->threads[n]->pthread, SIGUSR1);
	}
}
***/


/* Resume all threads in threadpool */
/***
void thpool_resume(thpool_* thpool_p) {
	// threads_on_hold = 0;
	thpool_p->threads_on_hold = 0;
}
**/





/* ============================ THREAD ============================== */


/* Initialize a thread in the thread pool
 * 
 * @param thread        address to the pointer of the thread to be created
 * @param id            id to be given to the thread
 * @return 0 on success, -1 otherwise.
 */
static int thread_init (thpool_* thpool_p, struct thread** thread_p, int id){
	
	*thread_p = (struct thread*)malloc(sizeof(struct thread));
	if (thread_p == NULL){
		fprintf(stderr, "thpool_init(): Could not allocate memory for thread\n");
		return -1;
	}

	(*thread_p)->thpool_p = thpool_p;
	(*thread_p)->id       = id;

	// printf("s3765 jagpthread_create thread_do() ...\n"); fflush( stdout );
	jagpthread_create(&(*thread_p)->pthread, NULL, (void * (*) (void*))thread_do, (*thread_p));

	//printf("s4603 thread_init jagpthread_create \n");
	//fflush( stdout );
	
	pthread_detach((*thread_p)->pthread);
	return 0;
}

/* What each thread is doing
* 
* In principle this is an endless loop. The only time this loop gets interuppted is once
* thpool_destroy() is invoked or the program exits.
* 
* @param  thread        thread that will run this function
* @return nothing
*/
static void* thread_do(struct thread* thread_p){

	/* Set thread name for profiling and debuging */
	char thread_name[128] = {0};
	sprintf(thread_name, "thread-pool-%d", thread_p->id);

#if defined(__linux__)
	/* Use prctl instead to prevent using _GNU_SOURCE flag and implicit declaration */
	prctl(PR_SET_NAME, thread_name);
#elif defined(__APPLE__) && defined(__MACH__)
	pthread_setname_np(thread_name);
#else
	// fprintf(stderr, "thread_do(): pthread_setname_np is not supported on this system");
#endif

	/* Assure all threads have been created before starting serving */
	thpool_* thpool_p = thread_p->thpool_p;
	
	/* Register signal handler */
	/******
	struct sigaction act;
	sigemptyset(&act.sa_mask);
	act.sa_flags = 0;
	act.sa_handler = (__sighandler_t)thread_hold;
	if (sigaction(SIGUSR1, &act, NULL) == -1) {
		fprintf(stderr, "thread_do(): cannot handle SIGUSR1");
	}
	******/
	
	/* Mark thread as alive (initialized) */
	pthread_mutex_lock(&thpool_p->thcount_lock);
	thpool_p->num_threads_alive += 1;
	// printf("s2839 num_threads_alive=%d\n", thpool_p->num_threads_alive ); fflush( stdout );
	pthread_mutex_unlock(&thpool_p->thcount_lock);

	// while(threads_keepalive){
	while( thpool_p->threads_keepalive){

		bsem_wait(thpool_p->jobqueue_p->has_jobs);

		// if (threads_keepalive){
		if ( thpool_p->threads_keepalive){
			
			pthread_mutex_lock(&thpool_p->thcount_lock);
			thpool_p->num_threads_working++;
			// printf("s1839 num_threads_working=%d\n", thpool_p->num_threads_working ); fflush( stdout );
			pthread_mutex_unlock(&thpool_p->thcount_lock);
			
			/* Read job from queue and execute it */
			void*(*func_buff)(void* arg);
			void*  arg_buff;
			job* job_p;
			pthread_mutex_lock(&thpool_p->jobqueue_p->rwmutex);
			job_p = jobqueue_pull(thpool_p);
			pthread_mutex_unlock(&thpool_p->jobqueue_p->rwmutex);
			if (job_p) {
				func_buff = job_p->function;
				arg_buff  = job_p->arg;
				//printf("s3293 pool thread job %0x ...\n", job_p ); fflush( stdout );
				func_buff(arg_buff);
				//printf("s3294 pool thread job %0x done\n", job_p ); fflush( stdout );
				if ( job_p ) free(job_p);
				job_p = NULL;
			}
			
			pthread_mutex_lock(&thpool_p->thcount_lock);
			thpool_p->num_threads_working--;
			// printf("s1819 num_threads_working=%d\n", thpool_p->num_threads_working ); fflush( stdout );
			if (!thpool_p->num_threads_working) {
				pthread_cond_signal(&thpool_p->threads_all_idle);
			}
			pthread_mutex_unlock(&thpool_p->thcount_lock);

		}
	}
	pthread_mutex_lock(&thpool_p->thcount_lock);
	thpool_p->num_threads_alive --;
	// printf("s2849 num_threads_alive=%d\n", thpool_p->num_threads_alive ); fflush( stdout );
	pthread_mutex_unlock(&thpool_p->thcount_lock);

	return NULL;
}


/* Frees a thread  */
static void thread_destroy (thread* thread_p){
	if ( thread_p ) free(thread_p);
	thread_p = NULL;
}





/* ============================ JOB QUEUE =========================== */


/* Initialize queue */
static int jobqueue_init(thpool_* thpool_p){
	
	thpool_p->jobqueue_p = (struct jobqueue*)malloc(sizeof(struct jobqueue));
	if (thpool_p->jobqueue_p == NULL){
		return -1;
	}
	thpool_p->jobqueue_p->len = 0;
	thpool_p->jobqueue_p->front = NULL;
	thpool_p->jobqueue_p->rear  = NULL;

	thpool_p->jobqueue_p->has_jobs = (struct bsem*)malloc(sizeof(struct bsem));
	if (thpool_p->jobqueue_p->has_jobs == NULL){
		return -1;
	}

	pthread_mutex_init(&(thpool_p->jobqueue_p->rwmutex), NULL);
	bsem_init(thpool_p->jobqueue_p->has_jobs, 0);

	return 0;
}


/* Clear the queue */
static void jobqueue_clear(thpool_* thpool_p){

	while(thpool_p->jobqueue_p->len){
		if ( jobqueue_pull(thpool_p) ) free(jobqueue_pull(thpool_p));
	}

	thpool_p->jobqueue_p->front = NULL;
	thpool_p->jobqueue_p->rear  = NULL;
	bsem_reset(thpool_p->jobqueue_p->has_jobs);
	thpool_p->jobqueue_p->len = 0;

}


/* Add (allocated) job to queue
 *
 * Notice: Caller MUST hold a mutex
 */
static void jobqueue_push(thpool_* thpool_p, struct job* newjob){

	newjob->prev = NULL;

	switch(thpool_p->jobqueue_p->len){

		case 0:  /* if no jobs in queue */
					thpool_p->jobqueue_p->front = newjob;
					thpool_p->jobqueue_p->rear  = newjob;
					break;

		default: /* if jobs in queue */
					thpool_p->jobqueue_p->rear->prev = newjob;
					thpool_p->jobqueue_p->rear = newjob;
					
	}
	thpool_p->jobqueue_p->len++;
	
	bsem_post(thpool_p->jobqueue_p->has_jobs);
}


/* Get first job from queue(removes it from queue)
 * 
 * Notice: Caller MUST hold a mutex
 */
static struct job* jobqueue_pull(thpool_* thpool_p){

	job* job_p;
	job_p = thpool_p->jobqueue_p->front;

	switch(thpool_p->jobqueue_p->len){
		
		case 0:  /* if no jobs in queue */
		  			break;
		
		case 1:  /* if one job in queue */
					thpool_p->jobqueue_p->front = NULL;
					thpool_p->jobqueue_p->rear  = NULL;
					thpool_p->jobqueue_p->len = 0;
					break;
		
		default: /* if >1 jobs in queue */
					thpool_p->jobqueue_p->front = job_p->prev;
					thpool_p->jobqueue_p->len--;
					/* more than one job in queue -> post it */
					bsem_post(thpool_p->jobqueue_p->has_jobs);
					
	}
	
	return job_p;
}


/* Free all queue resources back to the system */
static void jobqueue_destroy(thpool_* thpool_p){
	jobqueue_clear(thpool_p);
	if ( thpool_p->jobqueue_p->has_jobs ) free(thpool_p->jobqueue_p->has_jobs);
	thpool_p->jobqueue_p->has_jobs = NULL;
}





/* ======================== SYNCHRONISATION ========================= */


/* Init semaphore to 1 or 0 */
static void bsem_init(bsem *bsem_p, int value) {
	if (value < 0 || value > 1) {
		fprintf(stderr, "bsem_init(): Binary semaphore can take only 1 or 0");
		exit(1);
	}
	pthread_mutex_init(&(bsem_p->mutex), NULL);
	pthread_cond_init(&(bsem_p->cond), NULL);
	bsem_p->v = value;
}


/* Reset semaphore to 0 */
static void bsem_reset(bsem *bsem_p) {
	bsem_init(bsem_p, 0);
}


/* Post to at least one thread */
static void bsem_post(bsem *bsem_p) {
	pthread_mutex_lock(&bsem_p->mutex);
	bsem_p->v = 1;
	pthread_cond_signal(&bsem_p->cond);
	pthread_mutex_unlock(&bsem_p->mutex);
}


/* Post to all threads */
static void bsem_post_all(bsem *bsem_p) {
	pthread_mutex_lock(&bsem_p->mutex);
	bsem_p->v = 1;
	pthread_cond_broadcast(&bsem_p->cond);
	pthread_mutex_unlock(&bsem_p->mutex);
}


/* Wait on semaphore until semaphore has value 0 */
static void bsem_wait(bsem* bsem_p) {
	pthread_mutex_lock(&bsem_p->mutex);
	while (bsem_p->v != 1) {
		pthread_cond_wait(&bsem_p->cond, &bsem_p->mutex);
	}
	bsem_p->v = 0;
	pthread_mutex_unlock(&bsem_p->mutex);
}


JaguarThreadPool::JaguarThreadPool( int numThreads )
{
	if ( numThreads >= 1 ) {
    	_pool = thpool_init( numThreads );
	} else {
		int numCPU = JagSystem::getNumCPUs();
    	_pool = thpool_init( numCPU );
	}
}

JaguarThreadPool::~JaguarThreadPool()
{
	thpool_wait( _pool );
	thpool_destroy( _pool ); 
}

int JaguarThreadPool::addWork( void *(*functionp)(void*), void* arg)
{
	return thpool_add_work( _pool, functionp, arg);
}

void JaguarThreadPool::wait()
{
	thpool_wait( _pool );
}

