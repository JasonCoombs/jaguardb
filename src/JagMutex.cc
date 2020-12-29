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



JagReadWriteMutex::JagReadWriteMutex( pthread_rwlock_t *lock )
{
	_lock = lock;
	_type = 0;
}

JagReadWriteMutex::JagReadWriteMutex( pthread_rwlock_t *lock, int type )
{
	_lock = lock;
	_type = type;
	//prt(("s6376 JagReadWriteMutex _lock=%0x type=%d getlock...\n", _lock, type ));

	if ( type != READ_LOCK && type != WRITE_LOCK ) {
		printf("c2738 Error JagReadWriteMutex ctor invalid mutex. set it to WRITE lock.\n");
		_type = WRITE_LOCK;
	}

	if ( _type == READ_LOCK ) {
    	JAG_BLURT 
		//if ( _lock ) _lock->readLock();
		if ( _lock )  pthread_rwlock_rdlock( _lock );


		// printf("ctor of JagReadWriteMutex %0x  read called\n", _lock );
    	JAG_OVER
	} else {
    	JAG_BLURT 
		//if ( _lock ) _lock->writeLock();
		if ( _lock ) pthread_rwlock_wrlock( _lock );
    	JAG_OVER
		// printf("ctor of JagReadWriteMutex %0x  write called\n", _lock );
	}

	// prt(("s6377 JagReadWriteMutex lock=%0x type=%d gotlock\n", _lock, type ));
}

JagReadWriteMutex::~JagReadWriteMutex( )
{
	if ( _type == READ_LOCK ) {
		//if ( _lock ) _lock->readUnlock();
		if ( _lock ) pthread_rwlock_unlock( _lock );
		// printf("dtor of JagReadWriteMutex %0x  read called\n", _lock );
	} else if (  _type == WRITE_LOCK ) {
		//if ( _lock ) _lock->writeUnlock();
		if ( _lock ) pthread_rwlock_unlock( _lock );
		// printf("dtor of JagReadWriteMutex %0x  write called\n", _lock );
	} 

	// prt(("s6378 JagReadWriteMutex lock=%0x unlocked\n", _lock ));
}

void JagReadWriteMutex::readLock()
{
	_type = READ_LOCK;
	JAG_BLURT
	//if ( _lock ) _lock->readLock();
	if ( _lock ) pthread_rwlock_rdlock( _lock );
	JAG_OVER
}

void JagReadWriteMutex::readUnlock()
{
	_type = 0;
	//if ( _lock ) _lock->readUnlock();
	if ( _lock ) pthread_rwlock_unlock( _lock );
}


void JagReadWriteMutex::writeLock()
{
	_type = WRITE_LOCK;
	JAG_BLURT
	//if ( _lock ) _lock->writeLock();
	if ( _lock ) pthread_rwlock_wrlock( _lock );
	JAG_OVER
}

void JagReadWriteMutex::writeUnlock()
{
	_type = 0; 
	//if ( _lock ) _lock->writeUnlock();
	if ( _lock ) pthread_rwlock_unlock( _lock );
}

void JagReadWriteMutex::unlock()
{
	if ( ! _lock ) return;

	/**
	if ( WRITE_LOCK == _type ) _lock->writeUnlock();
	else _lock->readUnlock();
	**/
	pthread_rwlock_unlock( _lock );

	_type = 0; 
}

/**
void JagReadWriteMutex::upgradeToWriteLock()
{
	_type = WRITE_LOCK ;
	JAG_BLURT
	if ( _lock ) _lock->upgradeToWriteLock();
	JAG_OVER
}
**/


// class JagReadWriteLock 

#if 0
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
	JAG_BLURT
        pthread_mutex_lock(&class_mutex);
		// if there are writers, wait for them to give a read signal
        while(_numWriters>0) {
            pthread_cond_wait(&_readerCond, &class_mutex);
        }
        _numReaders++;        
        pthread_mutex_unlock(&class_mutex);
	JAG_OVER
}

void JagReadWriteLock::readUnlock()
{
        pthread_mutex_lock(&class_mutex);
        _numReaders--;

		// if i am last reader and there are writers waiting, give a write signal
        if(_numReaders==0 && _numWritersWaiting>0)
            pthread_cond_signal(&_writeCond);

        pthread_mutex_unlock(&class_mutex);
}

void JagReadWriteLock::writeLock()
{
	JAG_BLURT
        pthread_mutex_lock(&class_mutex);
        _numWritersWaiting++;

		// if there are any readers or writers, wait for write signal
        while(_numReaders>0 || _numWriters>0) {
            pthread_cond_wait(&_writeCond, &class_mutex);
        }

        _numWritersWaiting--; 
		_numWriters++;
        pthread_mutex_unlock(&class_mutex);
	JAG_OVER
}


void JagReadWriteLock::writeUnlock()
{
        pthread_mutex_lock(&class_mutex);
        _numWriters--;

		// if there are writers waiting, give a write signal
        if( _numWritersWaiting > 0)
            pthread_cond_signal(&_writeCond);

		// broadcast a read signal
        pthread_cond_broadcast(&_readerCond);
        pthread_mutex_unlock(&class_mutex);
}

// upgrade readlock to write lock
void JagReadWriteLock::upgradeToWriteLock()
{
	JAG_BLURT
        pthread_mutex_lock(&class_mutex);
		// i am done with reading
        _numReaders--;

		// i am waiting to write
        _numWritersWaiting++;

		// if there are any readers or writers, wait for write signal
        while(_numReaders>0 || _numWriters>0) {
            pthread_cond_wait(&_writeCond, &class_mutex);
        }

        _numWritersWaiting--; 
		_numWriters++;
        pthread_mutex_unlock(&class_mutex);
	JAG_OVER
}
#endif


pthread_rwlock_t *newJagReadWriteLock()
{
	pthread_rwlock_t *rwlock = new pthread_rwlock_t();
	pthread_rwlock_init(rwlock, NULL);
	return rwlock;
}

void deleteJagReadWriteLock( pthread_rwlock_t *lock )
{
	if ( NULL == lock ) return;
	pthread_rwlock_destroy(lock);
	delete lock;
}


