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
#ifndef _jag_mutex_h_
#define _jag_mutex_h_

#include <stdio.h>
#include <pthread.h>

// simple mutex wrapper
class JagMutex
{
  public:
    JagMutex( pthread_mutex_t *_mutex );
	void lock();
	void unlock();
    ~JagMutex( );

  protected:
	pthread_mutex_t  *_mutex;
};

class JagReadWriteLock 
{
  protected:
    int 	_numReaders;
    int 	_numWriters, _numWritersWaiting;
    pthread_mutex_t 	class_mutex;
    pthread_cond_t  	_readerCond;
    pthread_cond_t  	_writeCond;

  public:

    JagReadWriteLock();
    ~JagReadWriteLock();
    void readLock();
    void writeLock();
    void readUnlock();
    void writeUnlock();
};


/////////// Read and write mutex for readers and writers
class JagReadWriteMutex
{
  public:
    JagReadWriteMutex( JagReadWriteLock *_mutex );
    JagReadWriteMutex( JagReadWriteLock *_mutex, int lockType );
	void readLock();
	void writeLock();
	void readUnlock();
	void writeUnlock();
	void unlock();
    ~JagReadWriteMutex( );

	static  const  int  READ_LOCK = 1;
	static  const  int  WRITE_LOCK = 2;

  protected:
	JagReadWriteLock  *_lock;
	int				   _type;  
};


#endif
