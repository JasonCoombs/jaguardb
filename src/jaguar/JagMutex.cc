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

#include <JagMutex.h>
#include <JagUtil.h>


JagMutex::JagMutex( pthread_mutex_t *mutex )
{
	_mutex = mutex;
	pthread_mutex_lock( _mutex );
}

JagMutex::~JagMutex( )
{
	pthread_mutex_unlock( _mutex );
}

void JagMutex::lock()
{
	pthread_mutex_lock( _mutex );
}

void JagMutex::unlock()
{
	pthread_mutex_unlock( _mutex );
}



JagReadWriteMutex::JagReadWriteMutex( JagReadWriteLock *mutex )
{
	_lock = mutex;
	_type = 0;
}

JagReadWriteMutex::JagReadWriteMutex( JagReadWriteLock *mutex, int type )
{
	_lock = mutex;
	_type = type;
	// prt(("s6376 JagReadWriteMutex _lock=%0x type=%d getlock...\n", _lock, type ));

	if ( type != READ_LOCK && type != WRITE_LOCK ) {
		printf("c2738 Error JagReadWriteMutex ctor invalid mutex. set it to WRITE lock.\n");
		_type = WRITE_LOCK;
	}

	if ( _type == READ_LOCK ) {
		if ( _lock ) _lock->readLock();
		// printf("ctor of JagReadWriteMutex %0x  read called\n", _lock );
	} else {
		if ( _lock ) _lock->writeLock();
		// printf("ctor of JagReadWriteMutex %0x  write called\n", _lock );
	}

	// prt(("s6377 JagReadWriteMutex lock=%0x type=%d gotlock\n", _lock, type ));
}

JagReadWriteMutex::~JagReadWriteMutex( )
{
	if ( _type == READ_LOCK ) {
		if ( _lock ) _lock->readUnlock();
		// printf("dtor of JagReadWriteMutex %0x  read called\n", _lock );
	} else if (  _type == WRITE_LOCK ) {
		if ( _lock ) _lock->writeUnlock();
		// printf("dtor of JagReadWriteMutex %0x  write called\n", _lock );
	} 

	// prt(("s6378 JagReadWriteMutex lock=%0x unlocked\n", _lock ));
}

void JagReadWriteMutex::readLock()
{
	_type = READ_LOCK;
	if ( _lock ) _lock->readLock();
}

void JagReadWriteMutex::readUnlock()
{
	_type = 0;
	if ( _lock ) _lock->readUnlock();
}


void JagReadWriteMutex::writeLock()
{
	_type = WRITE_LOCK;
	if ( _lock ) _lock->writeLock();
}

void JagReadWriteMutex::writeUnlock()
{
	_type = 0; 
	if ( _lock ) _lock->writeUnlock();
}

void JagReadWriteMutex::unlock()
{
	if ( ! _lock ) return;

	if ( WRITE_LOCK == _type ) _lock->writeUnlock();
	else _lock->readUnlock();

	_type = 0; 
}


// class JagReadWriteLock 

    JagReadWriteLock::JagReadWriteLock()
      : _numReaders(0), _numWriters(0), _numWritersWaiting(0),
      class_mutex(PTHREAD_MUTEX_INITIALIZER),
      _readerCond(PTHREAD_COND_INITIALIZER),
      _writeCond(PTHREAD_COND_INITIALIZER)
    {
	}

    JagReadWriteLock::~JagReadWriteLock()
    {
        pthread_mutex_destroy(&class_mutex);
        pthread_cond_destroy(&_readerCond);
        pthread_cond_destroy(&_writeCond);
    }

    void JagReadWriteLock::readLock()
    {
        pthread_mutex_lock(&class_mutex);
        while(_numWriters>0)
        {
            pthread_cond_wait(&_readerCond, &class_mutex);
        }
        _numReaders++;        
        pthread_mutex_unlock(&class_mutex);
    }

    void JagReadWriteLock::writeLock()
    {
		/***
		// printf("s999111 this=%0x JagReadWriteLock::writeLock()... _numReaders=%d _numWritersWaiting=%d _numWriters=%d\n",
				this, _numReaders, _numWritersWaiting, _numWriters ); fflush( stdout );
				***/

        pthread_mutex_lock(&class_mutex);
        _numWritersWaiting++;

        while(_numReaders>0 || _numWriters>0)
        {
            pthread_cond_wait(&_writeCond, &class_mutex);
        }

        _numWritersWaiting--; 
		_numWriters++;
        pthread_mutex_unlock(&class_mutex);
    }

    void JagReadWriteLock::readUnlock()
    {
        pthread_mutex_lock(&class_mutex);
        _numReaders--;

        if(_numReaders==0 && _numWritersWaiting>0)
            pthread_cond_signal(&_writeCond);

        pthread_mutex_unlock(&class_mutex);
    }

    void JagReadWriteLock::writeUnlock()
    {
        pthread_mutex_lock(&class_mutex);
        _numWriters--;

        if( _numWritersWaiting > 0)
            pthread_cond_signal(&_writeCond);

        pthread_cond_broadcast(&_readerCond);
        pthread_mutex_unlock(&class_mutex);
    }

