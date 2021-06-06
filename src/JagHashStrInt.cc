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
#include <JagHashStrInt.h>
#include <JagUtil.h>

JagHashStrInt::JagHashStrInt()
{
	jag_hash_init( &_hash, 10 );
	_len = 0;
}

JagHashStrInt::~JagHashStrInt()
{
	jag_hash_destroy( &_hash );
}

bool JagHashStrInt::addKeyValue( const Jstr & key, int val )
{
	if ( key.size() < 1 ) return 0;
	int rc = jag_hash_insert_str_int( &_hash, key.c_str(), val );
	if ( rc ) {
		++ _len;
		return true;
	} 
	return false;
}

void JagHashStrInt::removeKey( const Jstr & key )
{
	if ( key.size() < 1 ) return;
	int rc = jag_hash_delete( &_hash, key.c_str() );
	if ( rc ) {
		-- _len;
	}
}

bool JagHashStrInt::keyExist( const Jstr & key ) const
{
	if ( key.size() < 1 ) return false;
	char *pval = jag_hash_lookup( &_hash, key.c_str() );
	if ( pval ) return true;
	return false;
}

int JagHashStrInt::getValue( const Jstr &key, bool &rc ) const
{
	int val = 0;
	rc = jag_hash_lookup_str_int( &_hash, key.c_str(), &val );
	return val;
}

bool JagHashStrInt::getValue( const Jstr &key, int &val ) const
{
	bool rc = jag_hash_lookup_str_int( &_hash, key.c_str(), &val );
	return rc;
}



void JagHashStrInt::reset()
{
	jag_hash_destroy( &_hash );
	jag_hash_init( &_hash, 10 );
	_len = 0;
}


JagHashStrInt::JagHashStrInt( const JagHashStrInt &o )
{
	jag_hash_init( &_hash, 10 );
	_len = 0;

	HashNodeT *node;
	for ( int i = 0; i < o._hash.size; ++i ) {
		node = o._hash.bucket[i];
		while ( node != NULL ) {
			jag_hash_insert( &_hash, node->key, node->value );
			node = node->next;
			++ _len;
		}
	}
}

JagHashStrInt& JagHashStrInt:: operator= ( const JagHashStrInt &o )
{
	if ( this == &o ) return *this;

	reset();

	HashNodeT *node;
	for ( int i = 0; i < o._hash.size; ++i ) {
		node = o._hash.bucket[i];
		while ( node != NULL ) {
			jag_hash_insert( &_hash, node->key, node->value );
			node = node->next;
			++ _len;
		}
	}

	return *this;
}

void JagHashStrInt::print()
{
	prt(("s2378 JagHashStrInt::print():\n" ));
	HashNodeT *node;
	for ( int i = 0; i < _hash.size; ++i ) {
		node = _hash.bucket[i];
		while ( node != NULL ) {
			prt(("231 key=[%s] ==> value=[%s]\n", node->key, node->value ));
			node = node->next;
		}
	}
}


JagVector<AbaxPair<Jstr,jagint>> JagHashStrInt::getStrIntVector()
{
	JagVector<AbaxPair<Jstr,jagint>> vec;

	HashNodeT *node;
	Jstr key;
	jagint num;
	for ( int i = 0; i < _hash.size; ++i ) {
		node = _hash.bucket[i];
		while ( node != NULL ) {
			key = node->key;
			num = atoi(node->value);
			AbaxPair<Jstr,jagint> pair(key, num);
			vec.append( pair );
			node = node->next;
		}
	}

	return vec;
}

int JagHashStrInt::size()
{
	return _len;
}

void JagHashStrInt::removeAllKey()
{
	reset();
}

