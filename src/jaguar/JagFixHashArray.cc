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

#include <JagFixHashArray.h>

#define NBT  '\0'
// ctor
JagFixHashArray::JagFixHashArray( int inklen, int invlen )
{
	klen = inklen;
	vlen = invlen;
	kvlen = klen + vlen;
	abaxint size = 256;
	_arr = jagmalloc(size*kvlen);
	_arrlen = size;
	memset( _arr, 0, _arrlen*kvlen );
	_elements = 0;
}

// dtor
void JagFixHashArray::destroy( )
{
	if ( _arr ) {
		free(_arr); 
	}
	_arr = NULL;
}

// dtor
JagFixHashArray::~JagFixHashArray( )
{
	destroy();
}

void JagFixHashArray::removeAll()
{
	destroy();

	abaxint size = 256;
	_arr = jagmalloc(size*kvlen);
	_arrlen = size;
	memset( _arr, 0, _arrlen*kvlen );
	_elements = 0;
}

void JagFixHashArray::reAllocDistribute()
{
	// read lock: read can gointo, write cannot get into
	// printf("realloc ...\n");
	reAlloc();

	// printf("c3097 redistribute ...\n");
	reDistribute();
	// printf("redistribute done \n");

	// print();
	// release read lock
}

void JagFixHashArray::reAllocDistributeShrink()
{
	// read lock: read can gointo, write cannot get into
	reAllocShrink();
	reDistributeShrink();
	// release read lock
}

void JagFixHashArray::reAlloc()
{
	abaxint i;
	_newarrlen = _GEO*_arrlen; 
	_newarr = jagmalloc(_newarrlen*kvlen);

	for ( i=0; i < _newarrlen; ++i) {
		_newarr[i*kvlen] = NBT;
	}
}

void JagFixHashArray::reAllocShrink()
{
	abaxint i;
	_newarrlen  = _arrlen/_GEO; 
	_newarr = jagmalloc(_newarrlen*kvlen);

	for ( i=0; i < _newarrlen; ++i) {
		_newarr[i*kvlen] = NBT;
	}
}

void JagFixHashArray::reDistribute()
{
	abaxint pos; 

	for ( abaxint i = _arrlen-1; i>=0; --i) {
		if ( _arr[i*kvlen] == NBT ) { continue; } 
		// pos = hashLocation( _arr[i], _newarr, _newarrlen );
		pos = hashLocation( _arr+i*kvlen, _newarr, _newarrlen );
		// _newarr[pos] = _arr[i];  // not null
		memcpy( _newarr+pos*kvlen, _arr+i*kvlen, kvlen );
	}

	// write lock
	if ( _arr ) {
		free(_arr);
	}
	_arrlen = _newarrlen;
	_arr = _newarr;
}

void JagFixHashArray::reDistributeShrink()
{
	abaxint pos; 

	for ( abaxint i = _arrlen-1; i>=0; --i) {
		if ( _arr[i*kvlen] == NBT ) { continue; } 
		// pos = hashLocation( _arr[i], _newarr, _newarrlen );
		pos = hashLocation( _arr+i*kvlen, _newarr, _newarrlen );
		// _newarr[pos] = _arr[i];
		memcpy( _newarr+pos*kvlen, _arr+i*kvlen, kvlen );
	}

	// write lock
	if ( _arr ) free(_arr);
	_arrlen = _newarrlen;
	_arr = _newarr;

	// release write lock
}

bool JagFixHashArray::insert( const char *newpair )
{
	bool rc;
	abaxint index;

	if ( *newpair == NBT  ) { 
		return 0; 
	}

	abaxint retindex;
	rc = exist( newpair, &retindex );
	if ( rc ) {
		return false;
	}

	if ( ( _GEO*_elements ) >=  _arrlen-4 ) {
		reAllocDistribute();
	}

	index = hashLocation( newpair, _arr, _arrlen ); 
	// _arr[index] = newpair;  
	memcpy( _arr+index*kvlen, newpair, kvlen );
	++_elements;

	// *retindex = index;
	return true;
}

bool JagFixHashArray::remove( const char *pair )
{
	abaxint index;
	bool rc = exist( pair, &index );
	if ( ! rc ) return false;

	// _arr[index*kvlen] = NBT;
	// -- _elements;
	rehashCluster( index );
	-- _elements;

	if ( _arrlen >= 64 ) {
    	int loadfactor  = 100 * _elements / _arrlen;
    	if (  loadfactor < 20 ) {
    		reAllocDistributeShrink();
    	}
	} 

	return true;
}

bool JagFixHashArray::exist( const char *search, abaxint *index )
{
    abaxint idx = hashKey( search, _arrlen );
	*index = idx;
    
   	//  hash to NULL, not found.
    if ( _arr[idx*kvlen] == NBT ) {
   		return 0;
    }
       
    // if ( search != _arr[idx] )  {
    if ( 0 != memcmp( search, _arr+idx*kvlen, klen ) ) {
   		idx = findProbedLocation( search, idx );
   		if ( idx < 0 ) {
   			return 0;
   		}
   	}
    
   	*index = idx;
    return 1;
}

bool JagFixHashArray::get(  char *pair )
{
	abaxint index;
	bool rc;

	rc = exist( pair, &index );
	if ( ! rc ) return false;

	// pair.value = _arr[index].value;
	memcpy( pair+klen, _arr+index*kvlen+klen, vlen ); // copy value part
	return true;
}

bool JagFixHashArray::get( const char *key, char *val )
{
	abaxint index;
	bool rc;

	rc = exist( key, &index );
	if ( ! rc ) return false;

	// pair.value = _arr[index].value;
	memcpy( val, _arr+index*kvlen+klen, vlen ); // copy value part
	return true;
}

bool JagFixHashArray::set( const char *pair )
{
	abaxint index;
	bool rc;

	rc = exist( pair, &index );
	if ( ! rc ) return false;

	// _arr[index].value = pair.value;
	memcpy( _arr+index*kvlen+klen,  pair+klen, vlen ); // set value part
	return true;
}

abaxint JagFixHashArray::hashLocation( const char *pair, const char *arr, abaxint arrlen )
{
	abaxint index = hashKey( pair, arrlen ); 
	if ( arr[index*kvlen] != NBT ) {
		index = probeLocation( index, arr, arrlen );
	}
	return index;
}


abaxint JagFixHashArray::probeLocation( abaxint hc, const char *arr, abaxint arrlen )
{
    while ( 1 ) {
    	hc = nextHC( hc, arrlen );
    	if ( arr[hc*kvlen] == NBT ) { return hc; }
   	}
   	return -1;
}
    
// retrieve the previously assigned probed location
abaxint JagFixHashArray::findProbedLocation( const char *search, abaxint idx ) 
{
   	while ( 1 ) {
   		idx = nextHC( idx, _arrlen );
  
   		if ( _arr[idx*kvlen] == NBT ) {
   			return -1; // no probed slot found
   		}
    
   		// if ( search == _arr[idx] ) {
   		if ( 0==memcmp( search, _arr+idx*kvlen, klen) ) {
   			return idx;
   		}
   	}
   	return -1;
}
    
    
// find [start, end] of island that contains hc  
// [3, 9]   [12, 3]
inline void JagFixHashArray::findCluster( abaxint hc, abaxint *start, abaxint *end )
{
	abaxint i;
	i = hc;
  	// find start
   	while (1 ) {
   		i = prevHC( i, _arrlen );
 		if ( _arr[i*kvlen] == NBT ) {
    		*start = nextHC( i, _arrlen );
    		break;
    	}
    }
    
    // find end
    i = hc;
    while (1 ) {
    	i = nextHC( i, _arrlen );
  
 		if ( _arr[i*kvlen] == NBT ) {
    		*end = prevHC( i, _arrlen );
    		break;
    	}
   	}
}

    
inline abaxint JagFixHashArray::prevHC ( abaxint hc, abaxint arrlen )
{
   	--hc; 
   	if ( hc < 0 ) hc = arrlen-1;
   	return hc;
}
    
inline abaxint JagFixHashArray::nextHC( abaxint hc, abaxint arrlen )
{
 	++ hc;
   	if ( hc == arrlen ) hc = 0;
   	return hc;
}


bool JagFixHashArray::aboveq( abaxint start, abaxint end, abaxint birthhc, abaxint nullbox )
{
		if ( start < end ) {
			return ( birthhc <= nullbox ) ? true : false;
		} else {
			if ( 0 <= birthhc  && birthhc <= end ) {
				birthhc += _arrlen;
			}

			if ( 0 <= nullbox  && nullbox <= end ) {
				nullbox += _arrlen;
			}

			return ( birthhc <= nullbox ) ? true : false;
		}
}

void JagFixHashArray::rehashCluster( abaxint hc )
{
	register abaxint start, end;
	register abaxint nullbox = hc;

	findCluster( hc, &start, &end );
	_arr[ hc*kvlen] = NBT;


	abaxint  birthhc;
	bool b;

	while ( 1 )
	{
		hc = nextHC( hc, _arrlen );
		if  ( _arr[hc*kvlen] == NBT ) {
			break;
		}

		// birthhc = hashKey( _arr[hc], _arrlen );
		birthhc = hashKey( _arr+hc*kvlen, _arrlen );
		if ( birthhc == hc ) {
			continue;  // birth hc
		}

		b = aboveq( start, end, birthhc, nullbox );
		if ( b ) {
			/***
			_arr[nullbox] = _arr[hc];
			_arr[hc] = Pair::NULLVALUE;
			***/
			memcpy( _arr+nullbox*kvlen, _arr+hc*kvlen, kvlen );
			_arr[hc*kvlen] = NBT;
			nullbox = hc;
		}
	}
}

    
void JagFixHashArray::print() const
{
	char buf[16];

	printf("JagFixHashArray: _arrlen=%lld _elements=%lld\n", _arrlen, _elements );
	for ( abaxint i=0; i < _arrlen; ++i)
	{
		sprintf( buf, "%08d", i );
		if ( _arr[i*kvlen] != NBT ) {
			printf(" %s   ", buf );
			for (int k=0; k <klen; ++k ) {
				printf("%c", _arr+i*kvlen+k );
			}
			printf("\n");
		} 
	}
	printf("\n");
}

abaxint JagFixHashArray::hashKey( const char *key, abaxint arrlen ) const
{
	unsigned int hash[4];
	unsigned int seed = 42;
	MurmurHash3_x64_128( (void*)key, klen, seed, hash);
	uint64_t res = ((uint64_t*)hash)[0];
	abaxint rc = res % arrlen;
	return rc;
}

bool JagFixHashArray::isNull( abaxint i ) const 
{
	if ( i < 0 || i >= _arrlen ) { return true; } 
	return ( _arr[i*kvlen] == NBT ); 
}

