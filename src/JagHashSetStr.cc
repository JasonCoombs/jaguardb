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
#include <JagHashSetStr.h>
#include <JagUtil.h>

// ctor
JagHashSetStr::JagHashSetStr()
{
	jag_hash_set_init( &_hash, 10 );
	_len = 0;
}

// dtor
JagHashSetStr::~JagHashSetStr()
{
	jag_hash_set_destroy( &_hash );
}

bool JagHashSetStr::addKey( const Jstr & key )
{
	if ( key.size() < 1 ) return 0;
	int rc = jag_hash_set_insert( &_hash, key.c_str() );
	if ( rc ) {
		++ _len;
		return true;
	} 
	return false;
}

void JagHashSetStr::removeKey( const Jstr & key )
{
	if ( key.size() < 1 ) return;
	int rc = jag_hash_set_delete( &_hash, key.c_str() );
	if ( rc ) {
		-- _len;
	}
}

bool JagHashSetStr::keyExist( const Jstr & key ) const
{
	if ( key.size() < 1 ) return false;
	return jag_hash_set_lookup( &_hash, key.c_str() );
}

void JagHashSetStr::reset()
{
	jag_hash_set_destroy( &_hash );
	jag_hash_set_init( &_hash, 10 );
}

// copy ctor 
JagHashSetStr::JagHashSetStr( const JagHashSetStr &o )
{
	jag_hash_set_init( &_hash, 10 );
	HashSetNodeT *node;
	for ( int i = 0; i < o._hash.size; ++i ) {
		node = o._hash.bucket[i];
		while ( node != NULL ) {
			jag_hash_set_insert( &_hash, node->key );
			node = node->next;
		}
	}
}

// assignment =
JagHashSetStr& JagHashSetStr:: operator= ( const JagHashSetStr &o )
{
	if ( this == &o ) return *this;
	reset();
	HashSetNodeT *node;
	for ( int i = 0; i < o._hash.size; ++i ) {
		node = o._hash.bucket[i];
		while ( node != NULL ) {
			jag_hash_set_insert( &_hash, node->key );
			node = node->next;
		}
	}

	return *this;
}

void JagHashSetStr::print()
{
	prt(("s2378 JagHashSetStr::print():\n" ));
	HashSetNodeT *node;
	for ( int i = 0; i < _hash.size; ++i ) {
		node = _hash.bucket[i];
		while ( node != NULL ) {
			prt(("key=[%s]\n", node->key ));
			node = node->next;
		}
	}
}

int JagHashSetStr::size()
{
	return _len;
}

void JagHashSetStr::removeAllKey()
{
	reset();
}

