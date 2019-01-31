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

#include <JagFixGapVector.h>
#include <JagUtil.h>

//////////////////////// vector code ///////////////////////////////////////////////
// ctor
JagFixGapVector::JagFixGapVector()
{
}
void JagFixGapVector::initWithKlen( int inklen )
{
	klen  = inklen;
	vlen  = 1;
	kvlen = klen + vlen;
	init( 32 );
}

JagFixGapVector::JagFixGapVector( int inklen )
{
	klen  = inklen;
	vlen  = 1;
	kvlen = klen + vlen;
	init( 32 );
}
void JagFixGapVector::init( int initSize )
{
	// _arr = new Pair[initSize];
	_arr = jagmalloc( initSize*kvlen );
	memset( (void*)_arr, 0, initSize*kvlen ); 
	_arrlen = initSize;
	_elements = 0;
	_last = 0;

}

/********
// copy ctor
template <class Pair>
template <class P>
JagFixGapVector::JagFixGapVector( const JagFixGapVector<P>& other )
{
	// printf("c38484 JagFixGapVector::JagFixGapVector( const JagFixGapVector<P>& other ) ...\n");
	// fflush ( stdout );

	_arrlen = other._arrlen;
	_elements = other._elements;
	_last = other._last;

	_arr = new Pair[_arrlen];
	for ( abaxint i = 0; i < _arrlen; ++i ) {
		_arr[i] = other._arr[i];
	}
}


// assignment operator
template <class Pair>
JagFixGapVector& JagFixGapVector::operator=( const JagFixGapVector& other )
{
	// printf("c8383 JagFixGapVector& JagFixGapVector::operator=() ...\n");
	// fflush ( stdout );
	if ( _arr == other._arr ) {
		return *this;
	}

	if ( _arr ) {
		delete [] _arr;
	}

	_arrlen = other._arrlen;
	_elements = other._elements;
	_last = other._last;

	_arr = new Pair[_arrlen];
	for ( abaxint i = 0; i < _arrlen; ++i ) {
		_arr[i] = other._arr[i];
	}

	return *this;
}

*******/

// dtor
void JagFixGapVector::destroy( )
{
	if ( ! _arr ) {
		return;
	}

	if ( _arr ) {
		free( _arr );
	}
	_arr = NULL;
}

// dtor
JagFixGapVector::~JagFixGapVector( )
{
	destroy();
}

void JagFixGapVector::reAlloc()
{
	abaxint i, j;
	_newarrlen = _arrlen + _arrlen/2;
	j = _newarrlen % JAG_BLOCK_SIZE;
	_newarrlen += JAG_BLOCK_SIZE - j;

	// _newarr = new Pair[_newarrlen];
	_newarr = jagmalloc( _newarrlen*kvlen );

	/***
	for ( i = 0; i < _arrlen; ++i) {
		_newarr[i] = _arr[i];
	}
	**/
	memcpy( _newarr, _arr, _arrlen*kvlen );


	for ( i = _arrlen; i < _newarrlen; ++i) {
		// _newarr[i] = Pair::NULLVALUE;
		_newarr[i*kvlen] = NBT;
		_newarr[i*kvlen+klen] = 0;
	}

	// if ( _arr ) delete [] _arr;
	if ( _arr ) free( _arr );
	_arr = _newarr;
	_newarr = NULL;
	_arrlen = _newarrlen;
}

void JagFixGapVector::reAllocShrink()
{
	abaxint i;

	_newarrlen  = _arrlen/_GEO; 

	// _newarr = new Pair[_newarrlen];
	_newarr = jagmalloc( _newarrlen*kvlen );
	/**
	for ( i = 0; i < _elements; ++i) {
		_newarr[i] = _arr[i];
	}
	for ( i = _elements; i < _newarrlen; ++i) {
		_newarr[i] = Pair::NULLVALUE;
	}
	**/
	memcpy( _newarr, _arr, _elements*kvlen );
	for ( i = _elements; i < _newarrlen; ++i) {
		// _newarr[i] = Pair::NULLVALUE;
		_newarr[i*kvlen] = NBT;
		_newarr[i*kvlen+klen] = 0;
	}

	// if ( _arr ) delete [] _arr;
	if ( _arr ) free(_arr);
	_arr = _newarr;
	_newarr = NULL;
	_arrlen = _newarrlen;
}


/***
bool JagFixGapVector::append( const char *newpair, abaxint *index )
{
	if ( _elements == _arrlen ) { reAlloc(); }
	*index = _elements;
	// _arr[_elements++] = newpair;
	// _arr[_elements] = newpair;
	memcpy( _arr+_elements*kvlen, newpair, kvlen );
	++_elements;
	return true;
}
***/

/**
bool JagFixGapVector::insertForce( const JagDBPair &pair, abaxint index )
{
	char buf[ pair.key.size() + pair.value.size() ];
	memcpy( buf, pair.key.c_str(), pair.key.size() );
	memcpy( buf + pair.key.size(), pair.value.c_str(), pair.value.size() );
	return insertForce( buf, index );
}
**/

bool JagFixGapVector::insertForce( const char *newpair, abaxint index )
{
	// char z = 0;
	while ( index >= _arrlen ) { 
		reAlloc(); 
	}

	/*****
	if ( _arr[index] == Pair::NULLVALUE ) {
		if ( newpair != Pair::NULLVALUE ) {
			++_elements;
			_arr[index] = newpair;
		}
	} else {
		if ( newpair == Pair::NULLVALUE ) {
			--_elements;
		}	
		_arr[index].key = newpair.key;
	}
	****/
	if ( _arr[index*kvlen] == NBT ) {
		if ( *newpair != NBT ) {
			++_elements;
			// _arr[index] = newpair;
			// memcpy(_arr + index*kvlen, newpair, kvlen );
			memcpy(_arr + index*kvlen, newpair, klen );
			// memcpy(_arr + index*kvlen+klen, &z, 1 );
		 	_arr[index*kvlen+klen] = 0;
		}
	} else {
		if ( *newpair == NBT ) {
			--_elements;
		}	
		// _arr[index].key = newpair.key;
		memcpy(_arr + index*kvlen, newpair, klen );
	}

	
	if ( index > _last ) {
		_last = index;
	}

	return true;
}

bool JagFixGapVector::insertLess( const char *newpair, abaxint index )
{
	bool rc = false;
	// char z = 0;
	while ( index >= _arrlen ) { 
		reAlloc(); 
	}

	if ( _arr[index*kvlen] == NBT ) {
		++_elements;
		// _arr[index] = newpair;
		// memcpy(_arr+index*kvlen, newpair, kvlen );
		memcpy(_arr+index*kvlen, newpair, klen );
		// memcpy(_arr+index*kvlen+klen, &z, 1 );
		 _arr[index*kvlen+klen] = 0;
		rc = true;
	} else {
		// if ( newpair < _arr[index] ) {
		if ( memcmp(newpair, _arr+index*kvlen, klen) < 0 ) {
			// _arr[index].key = newpair.key;
			memcpy(_arr+index*kvlen, newpair, klen );
			rc = true;
		}
	}

	if ( index > _last ) {
		_last = index;
	}

	return rc;
}

void JagFixGapVector::setValue( int val, bool isSet, abaxint index )
{
	// prt(("s4391 JagFixGapVector::setValue val=%d  isSet=%d index=%lld\n", val, isSet, index  ));

	while ( index >= _arrlen ) { 
		reAlloc(); 
	}

	/***
	if ( !isSet ) {
		if ( _arr[index].value.size() ) {
			val += *(_arr[index].value.c_str());
		}
		if ( val < 0 ) val = 0;
		else if ( val > JAG_BLOCK_SIZE ) val = JAG_BLOCK_SIZE;
	}
	***/
	if ( !isSet ) {
		if ( _arr[index*kvlen+klen] > 0 ) {
			// prt(("s3003 this=%0x val=%d index=%d v-part=%d\n", this, val, index,_arr[index*kvlen+klen] ));
			val += _arr[index*kvlen+klen];
		} else {
			// prt(("s3004 this=%0x index=%d v-part=%d is 0\n", this, index,_arr[index*kvlen+klen] ));
		}

		if ( val < 0 ) val = 0;
		else if ( val > JAG_BLOCK_SIZE ) val = JAG_BLOCK_SIZE;
	}

	/***
	char buf[2];
	char c = val;
	buf[0] = c;
	buf[1] = '\0';
	JagFixString value( buf, 1);
	// _arr[index].value = value;
	***/

	// prt(("s3022 set _arr[%d] to %d\n", index*kvlen+klen, val ));
	_arr[index*kvlen+klen] = val;
}		

bool JagFixGapVector::findLimitStart( abaxint &startlen, abaxint limitstart, abaxint &soffset )
{
	bool isEnd = false;
	int v;
	for ( abaxint i = 0; i < _arrlen; ++i ) {
		v = _arr[i*kvlen+klen];
		if (  v > 0 ) {
			startlen += v;
		}

		if ( startlen >= limitstart ) {
			isEnd = true;
			if ( v > 0 ) {
				startlen -= v;
			}
			soffset = i;
			break;
		}
	}
	// printf("s9999 end soffset=%lld, startlen=%lld\n", soffset, startlen);
	return isEnd;
}


// Slow, should not use this
/******
bool JagFixGapVector::remove( const char *pair )
{
	abaxint i, index;
	bool rc = exist( pair, &index );
	if ( ! rc ) return false;

	// shift left
	for ( i = index; i <= _elements-2; ++i ) {
		// _arr[i] = _arr[i+1];
		memcpy(_arr+i*kvlen, _arr+(i+1)*kvlen, kvlen );
	}
	-- _elements;

	if ( _arrlen >= 64 ) {
    	abaxint loadfactor  = 100 * (abaxint)_elements / _arrlen;
    	if (  loadfactor < 15 ) {
    		reAllocShrink();
    	}
	} 

	if ( index == _last ) {
		-- _last;
	}

	return true;
}
*******/

bool JagFixGapVector::setNull() 
{
	bool rc = false;
	if ( _elements > 0 ) {
		for ( abaxint i = 0; i < _arrlen; ++i ) {
	    	//_arr[i] = Pair::NULLVALUE;
	    	_arr[i*kvlen] = NBT;
		}	

		_elements = 0;
		_last = 0;
		rc = true;
	}
	return rc;
}

void JagFixGapVector::setNull( const char *pair, abaxint i )
{
	/***
	if ( pair != Pair::NULLVALUE && _arr[i] == pair ) {
		_arr[i] = Pair::NULLVALUE; 
		-- _elements; 
	}
	**/
	if ( *pair != NBT && 0==memcmp(_arr+i*kvlen, pair, klen) ) {
		_arr[i*kvlen] = NBT;
		-- _elements; 
	}
}

bool JagFixGapVector::isNull( abaxint i )  const
{
	if ( _arr[i*kvlen] == NBT ) {
		return true;
	} else {
		return false;
	}
}

abaxint JagFixGapVector::getPartElements( abaxint pos )  const
{
	// if ( pos <= _last && _arr[pos].value.size() ) return *(_arr[pos].value.c_str());
	if ( pos <= _last && _arr[pos*kvlen+klen] > 0 ) return _arr[pos*kvlen+klen];
	else return 0;
}

bool JagFixGapVector::cleanPartPair( abaxint pos ) 
{
	if( pos <= _last ) {
		_arr[pos*kvlen] = NBT;
		_arr[pos*kvlen+klen] = 0;
		--_elements;
		return true;
	}
	return false;
}

bool JagFixGapVector::deleteUpdateNeeded( const char *dpair, const char *npair, abaxint pos ) 
{
	if( pos <= _last ) {
		if ( memcmp(dpair, _arr+pos*kvlen, klen) <= 0 ) {
			if ( npair ) {
				memcpy(_arr+pos*kvlen, npair, klen );
			} else {
				_arr[pos*kvlen] = NBT;
				_arr[pos*kvlen+klen] = 1; // pay attention, need to set to 1; diskarray server remove will update it to 0
				--_elements;
			}
			return true;
		}
	}
	return false;
}

void JagFixGapVector::print() const
{
	printf("arrlen=%d, elements=%d, last=%d\n", _arrlen, _elements, _last);
	for ( abaxint i = 0; i <= _last; ++i ) {
		printf("i=%d   \n", i  );
		for ( int j=0; j < klen; ++j ) {
			if ( _arr[i*kvlen] == NBT ) break;
			printf("%c", *(_arr+i*kvlen+j) );
		}
		printf("-%d", *(_arr+i*kvlen+klen) );
		printf("\n"  );
	}	
}

/**
void JagFixGapVector::makeDBPair( abaxint i, JagDBPair &dbpair )
{
	dbpair.key = JagFixString( _arr+i*kvlen, klen );
	// dbpair.value = JagFixString( _arr+i*kvlen+klen, vlen );
}
**/


