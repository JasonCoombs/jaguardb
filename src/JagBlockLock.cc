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
#include <JagBlockLock.h>
#include <JagUtil.h>

JagBlockLock::JagBlockLock()
{
	init();
}

JagBlockLock::~JagBlockLock()
{
	if ( _map ) delete _map;
	_map = NULL;
}

void JagBlockLock::init()
{
	_map = new JagHashMap<AbaxLong, AbaxLong2>(100);
  	pthread_mutex_init(& _mutex, (pthread_mutexattr_t *)0);
  	pthread_cond_init(&_condvar, (pthread_condattr_t *)0);
	_readers = _writers = 0;
}

bool JagBlockLock::regionOverlaps( jagint pos, bool isRead )
{
	// -1 lock look at number of readers and writers
   	if ( -1 == pos ) {
		if ( ! isRead ) {
       		if ( _readers > 0 ||  _writers > 0 ) {
       			return true;
       		} else {
    			return false;  // empty  no readers, no writers
    		}
		} else {
			if ( _writers > 0 ) {
				return true;   // has writers
			} else {
    			return false;  // no writers
			}
		}
   	}

	if ( ! isRead ) {
    	if ( _map->keyExist( -1 ) ) {
			// printf("s4804 write lock saw -1\n" );
    		return true;  // -1 exists
    	}
    
    	if ( _map->keyExist( pos ) ) {
			// printf("s4805 write lock keyExist() %lld\n", pos );
    		return true; // pos lock(0 or 1) exists
    	}
    
    	return false;
	} else {
		AbaxLong2 cn;
		if ( _map->getValue( -1, cn ) ) {
			if ( cn.data2 < 1 ) {
				return false;  // -1 had no writers, compatible
			} else {
				//printf("s4806 read lock  saw -1 and -1.writers=%d\n", cn.data2 );
				return true;  // -1 had writers lock, not compatible
			}
		}

		if ( _map->getValue( pos, cn ) ) {
			if ( cn.data2 < 1 ) {
				return false;  // pos had only read lock, compatible
			} else {
				//printf("s4806 read lock  pos=%lld exists and writers=%d\n", cn.data2 );
				return true;   // pos had write lock, not compatible
			}
		}

    	return false;
	}

	return false;
}

// v2: data1 is readers; data2 is writers
void JagBlockLock::writeLock( jagint pos )
{
	JAG_BLURT
    jaguar_mutex_lock( & _mutex);
    while ( regionOverlaps( pos, false ) ) {
      jaguar_cond_wait(&_condvar, & _mutex);
    }
    AbaxLong2 v2;
    _map->getValue( pos, v2 );
    ++ v2.data2;
	++ _writers;
    _map->setValue( pos, v2, true );
    jaguar_mutex_unlock(&_mutex);
	JAG_OVER
}
  
void JagBlockLock::writeUnlock( jagint pos )
{
    jaguar_mutex_lock(&_mutex);
    AbaxLong2 v2;
    _map->getValue( pos, v2 );
    if ( v2.data2 == 1 && v2.data1 == 0 ) {
		-- _writers;
    	_map->removeKey( pos );
    } else {
		if ( v2.data2 > 0 ) {
    		-- v2.data2;
			-- _writers;
    		_map->setValue( pos, v2, true );
		}
    }
    jaguar_cond_broadcast(&_condvar);
    jaguar_mutex_unlock(&_mutex);
}
  
void JagBlockLock::readLock( jagint pos )
{
	JAG_BLURT
    jaguar_mutex_lock( & _mutex);
    while ( regionOverlaps( pos, true ) ) {
      jaguar_cond_wait(&_condvar, & _mutex);
    }
    AbaxLong2 v2;
    _map->getValue( pos, v2 );
    ++ v2.data1;
	++ _readers;
    _map->setValue( pos, v2, true );
    jaguar_mutex_unlock(&_mutex);
	JAG_OVER
}

void JagBlockLock::readUnlock( jagint pos )
{
    jaguar_mutex_lock(&_mutex);
    AbaxLong2 v2;
    _map->getValue( pos, v2 );
    if ( v2.data1 == 1 && v2.data2 == 0 ) {
		-- _readers;
    	_map->removeKey( pos );
    } else {
		if ( v2.data1 > 0 ) {
    		-- v2.data1;
			-- _readers;
    		_map->setValue( pos, v2, true );
		}
    }
    jaguar_cond_broadcast(&_condvar);
    jaguar_mutex_unlock(&_mutex);
}

