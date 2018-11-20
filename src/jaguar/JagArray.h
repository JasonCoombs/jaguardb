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
#ifndef _jag_arjag_class_h_
#define _jag_arjag_class_h_
#include <JagBlock.h>
#include <JagGapVector.h>
#include <JagHotSpot.h>

template <class Pair>
class JagArray
{
	public:

		JagArray( int length=64, bool usehash = false );
		~JagArray();

		inline bool insert( const Pair &newpair ) { abaxint idx; return insert(newpair, &idx); }
		bool insert( const Pair &newpair, abaxint *index, bool isUpsert=false );
		inline bool upsert( const Pair &newpair ) { abaxint idx; return upsert(newpair, &idx); }
		bool upsert( const Pair &newpair, abaxint *index ) { return insert( newpair, index, true ); }

		inline bool exist( const Pair &pair ) { abaxint idx,hc; return exist(pair, &idx, &hc); }
		bool exist( const Pair &pair, abaxint *index, abaxint *hc );

		bool remove( const Pair &pair, AbaxDestroyAction action=ABAX_NOOP ); 
		bool removeFirst(  AbaxDestroyAction action=ABAX_NOOP ); 
		bool removeLast(  AbaxDestroyAction action=ABAX_NOOP ); 
		bool get( Pair &pair ); 
		bool set( const Pair &pair ); 
		inline Pair& exactAt( abaxint i ) const { return _arr[i]; }
		inline void setGetpos( abaxint spos ) {
			if ( spos < 0 ) spos = 0;
			else if ( spos >= _arrlen ) spos = _arrlen-1;
			_getpos = spos;
		}
		inline bool getNext( Pair &pair ) {
			_getpos = nextNonNull(_getpos);
			if ( _getpos >= _arrlen ) return false;
			pair = _arr[_getpos];
			++_getpos;
			return true;
		}
		inline bool getPrev( Pair &pair ) {
			_getpos = prevNonNull(_getpos);
			if ( _getpos < 0 ) return false;
			pair = _arr[_getpos];
			--_getpos;
			return true;
		}
		void destroy();
		void clean() { if ( _elements <1 ) return; destroy(); init(); }
		void reset( abaxint len ) { destroy(); init( len ); }
		inline  abaxint size() const { return _arrlen; }
		inline  const Pair* array() const { return (const Pair*)_arr; }
		inline abaxint first() const { return _first; }
		inline abaxint last() const { return _last; }
		inline abaxint nextNonNull( abaxint start) const { 
			if ( start < 0 ) start = 0;
			while ( start <= _arrlen-1 && _arr[start] == Pair::NULLVALUE ) ++start;
			return start; 
		}
		inline abaxint prevNonNull( abaxint start) const { 
			if ( start >= _arrlen ) start = _arrlen-1;
			while ( start >= 0 && _arr[start] == Pair::NULLVALUE ) --start;
			return start; 
		}
		inline bool isNull( abaxint i ) const { 
			if ( i < 0 || i >= _arrlen ) { return true; } return ( _arr[i] == Pair::NULLVALUE ); 
		}
		
		bool 	findPred( const Pair &key, abaxint *index, abaxint first, abaxint last );
		bool 	findPred( const Pair &key, abaxint *index );
		bool 	findSucc( const Pair &key, abaxint *index );
		const Pair* getPred( const Pair &pair );
		const Pair* getSucc( const Pair &pair );

		inline  const Pair &firstElement() const {
			abaxint next = nextNonNull( _first );
			return _arr[next];
		}

		abaxint elements() { return _elements; }
		abaxint getShifts() { return _shifts; }
		void   rehashCluster( abaxint hc );
		abaxint utime();
		void   printCheck();
		void   print( bool printEmpty = true );
		void   printKey();

		abaxint  	_elements;
		abaxint  	_arrlen;
		//abaxint     exactCopy( const JagArray &arr2 );
		abaxint  	_first;
		abaxint  	_last;
		abaxint		_getpos;

	protected:

		void 	init( int length=64, bool usehash=false );
		void 	reAllocDistribute();
		void 	reAlloc();
		void 	reDistribute();

		void 	reAllocShrink();
		void 	reAllocDistributeShrink();
		void 	reDistributeShrink();

		inline int 	getStep( abaxint i );
		inline abaxint 	hashKey( const Pair &key, abaxint arrlen ) { return key.hashCode() % arrlen; }
		inline bool 	insertHash( const Pair &key, abaxint pos, abaxint *hash, abaxint arrlen );
		inline void 	updateIndex( const Pair &key, abaxint pos, JagBlock<Pair> *aindex, bool force=false );
		inline bool 	updateHash( const Pair &key, abaxint oldpos, abaxint newpos );
    	inline abaxint 	probeLocation( abaxint hc, abaxint *hash, abaxint arrlen );
    	inline abaxint 	findProbedLocation( const Pair &search, abaxint hc ) ;
    	inline void 	findCluster( abaxint hc, abaxint *start, abaxint *end );
    	inline abaxint 	prevHC ( abaxint hc, abaxint arrlen );
    	inline abaxint 	nextHC( abaxint hc, abaxint arrlen );
		inline bool     aboveq( abaxint start, abaxint end, abaxint birthhc, abaxint nullbox );

		Pair   		*_arr;
		Pair   		*_newarr;
		abaxint  	_newarrlen;
		abaxint		*_hashcol;
		abaxint		*_newhashcol;
		Pair 		_old;
		abaxint 	_shifts;
		JagBlock<Pair>  *_aindex;
		JagBlock<Pair>  *_newaindex;

		static const int LOAD_F1  = 1;	
		static const int _GEO  = 2;	 // fixed

		bool _usehash;
		JagHotSpot<Pair>  *_hotspot;

};

template <class Pair>
bool binSearchPred( const Pair &key, abaxint *index, const Pair *arr, abaxint arrlen, abaxint first, abaxint last );

// ctor
template <class Pair> 
JagArray<Pair>::JagArray( int len, bool usehash )
{
	init( len, usehash );
}

// ctor
template <class Pair> 
void JagArray<Pair>::init( int len, bool usehash )
{
	// _arrlen = 256;
	_arrlen = len;
	_arr = new Pair[_arrlen];

	for ( abaxint i =0; i <_arrlen; ++i) {
		_arr[i] = Pair::NULLVALUE;
	}

	_hashcol = NULL;
	_usehash = usehash;
	if ( _usehash ) {
		_hashcol = new abaxint[_arrlen];
		for ( abaxint i =0; i <_arrlen; ++i) {
			_hashcol[i] = ABAX_HASHARRNULL;
		}
	}

	_elements = 0;
	_shifts = 0;
	_first = 0;
	_last = 0;
	_getpos = 0;
	// _ups = 0;
	// _downs = 0;

	_aindex = new JagBlock<Pair>();
	_hotspot = new JagHotSpot<Pair>(200);
}

// dtor
template <class Pair> 
void JagArray<Pair>::destroy( )
{
	if ( _arr ) {
		delete [] _arr;
		_arr = NULL;
	}

	if ( _usehash && _hashcol ) {
		delete [] _hashcol;
		_hashcol = NULL;
	}

	if ( _aindex ) {
		delete _aindex;
		_aindex = NULL;
	}

	if ( _hotspot ) {
		delete _hotspot;
		_hotspot = NULL;
	}
}

// dtor
template <class Pair> 
JagArray<Pair>::~JagArray( )
{
	destroy();
}


/**********
template <class Pair> 
int JagArray<Pair>::getStep( abaxint i)
{
	// assume _GEO==2

	// W/n = 4 - 3|alpha|
	abaxint d = _ups - _downs;
	if ( d < 0 ) d = 0-d;
	float omega = (double)d / (double)_elements;
	if ( omega < 0.1 ) 
	{
		return 4;
	}

	float epsilon = 0.1;
	float gap;

	// loadfactor 1/2
	gap = 4.0 - 2.0 * omega;
	int   igap = (int)gap;
	float rem = gap - (float)igap;  // 
	if ( rem < epsilon ) {
		return igap;
	}

	float factor = 1.0/rem;   // 1.0/0.2 = 5;  1/0.21 ==> 4.99 => 5  1.0/0.3 = 3.333 => 4
	int  ifactor = (int) factor;
	if ( ( factor - (float)ifactor ) >= 0.000001 ) {
		++ifactor;
	}

	if ( (i % ifactor) == 0 ) {
		++igap;
	} 

	return igap;
}
******/


template <class Pair> 
void JagArray<Pair>::reAllocDistribute()
{
	reAlloc();

	reDistribute();
}

template <class Pair> 
void JagArray<Pair>::reAllocDistributeShrink()
{
	// read lock: read can gointo, write cannot get into
	reAllocShrink();
	reDistributeShrink();
	// release read lock
}

template <class Pair> 
void JagArray<Pair>::reAlloc()
{
	abaxint i;
	_newarrlen = _GEO*_arrlen; 
	_newarr = new Pair[_newarrlen];

	for ( i=0; i < _newarrlen; ++i) {
		_newarr[i] = Pair::NULLVALUE;
	}

	if ( _usehash ) {
		_newhashcol = new abaxint[ _newarrlen];
		for ( i=0; i < _newarrlen; ++i) {
			_newhashcol[i] = ABAX_HASHARRNULL;
		}
	}

	_newaindex = new JagBlock<Pair>();

}

template <class Pair> 
void JagArray<Pair>::reAllocShrink()
{
	abaxint i;
	_newarrlen  = _arrlen/_GEO; 
	_newarr = new Pair[_newarrlen];

	for ( i=0; i < _newarrlen; ++i) {
		_newarr[i] = Pair::NULLVALUE;
	}

	if ( _usehash ) {
		_newhashcol = new abaxint[ _newarrlen];
		for ( i=0; i < _newarrlen; ++i) {
			_newhashcol[i] = ABAX_HASHARRNULL;
		}
	}

	_newaindex = new JagBlock<Pair>();

}

/***********
template <class Pair> 
void JagArray<Pair>::reDistribute()
{
	abaxint pos; 
	abaxint cnt = 0;
	int step;
	float endx;
	float omega = (double)(_ups - _downs ) / (double)_elements;
	abaxint d = _ups - _downs;
	abaxint first, last;
	bool rc;

	abaxint lastBlock = -1;
	abaxint lastPos = -1;
	bool  firsttime = 1;


	if ( d < 0 ) d = 0-d;
	float absomega = (double)d / (double)_elements;

	if ( absomega < 0.1 ) {
		pos = _newarrlen - 1;
	}
	else {
		endx = (float)_newarrlen* ( 1.0 - ( absomega + omega )/4.0 );
		pos = (int) endx -1;
	}

	last = pos;

	for ( abaxint i = _arrlen-1; i>=0; --i) {
		if ( _arr[i] == Pair::NULLVALUE ) { continue; } 

		if ( pos < 0 ) {
			printf("e3101 fatal _newarrlen=%d  _arrlen=%d  i=%d  pos=%d\n", _newarrlen, _arrlen, i, pos );
			printf("cnt=%d ups=%d dows=%d omega=%.4f\n", cnt,  _ups, _downs, omega );
			_arr = _newarr;
			_arrlen = _newarrlen;
			exit(1);
		}

		_newarr[pos] = _arr[i];  // not null

		insertHash( _newarr[pos], pos, _newhashcol, _newarrlen );
		if ( firsttime ) // first
		{
			updateIndex( _newarr[pos], pos, _newaindex );
			firsttime = 0;
		} else {
			if ( (pos/_aindex->BLOCK) < lastBlock ) {
				updateIndex( _newarr[lastPos], lastPos, _newaindex );
			}
		}

		lastBlock = pos/_aindex->BLOCK;
		lastPos = pos;

		step = getStep( i );
		pos -= step;  // todo adjust according to _GEO and LOAD

		++cnt;
	}

	// good  fix. most-left is smallest
	updateIndex( _newarr[lastPos], lastPos, _newaindex );


	// _first = pos;
	first = pos;
	if ( pos < 0 ) {
		first = 0;
	}

	// write lock
	if ( _arr ) {
		delete [] _arr;
	}

	if ( _usehash && _hashcol ) {
		delete [] _hashcol;
	}

	_arr = _newarr;
	_newarr = NULL;
	if ( _usehash ) {
		_hashcol = _newhashcol;
		_newhashcol = NULL;
	}
	_arrlen = _newarrlen;
	_last = last;
	_first = first;

	if ( _aindex ) {
		delete _aindex;
	}
	_aindex = _newaindex;
	_newaindex = NULL;

	// release write lock

}
********/

template <class Pair> 
void JagArray<Pair>::reDistribute()
{
	abaxint pos; 
	abaxint cnt = 0;
	int step;
	float endx;
	abaxint first, last;
	bool rc;

	abaxint lastBlock = -1;
	abaxint lastPos = -1;
	bool  firsttime = 1;

	pos = 0;
	first = pos;
	int goRight = _hotspot->goingRight();
	int avgstep = _newarrlen / _elements;

	for ( abaxint i = 0; i < _arrlen; ++i) {
		if ( _arr[i] == Pair::NULLVALUE ) { continue; } 

		if ( pos >= _newarrlen ) {
			printf("e3101 fatal _newarrlen=%d  _arrlen=%d  i=%d  pos=%d cnt=%d\n", _newarrlen, _arrlen, i, pos, cnt );
			exit(1);
		}

		_newarr[pos] = _arr[i];  // not null
		// printf("inresize: pos=%d/_newarrlen=%d\n", pos, _newarrlen );

		insertHash( _newarr[pos], pos, _newhashcol, _newarrlen );
		if ( (pos/_aindex->BLOCK) != lastBlock ) {
			updateIndex( _newarr[pos], pos, _newaindex );
		}

		lastBlock = pos/_aindex->BLOCK;
		lastPos = pos;

		if ( goRight ) {
			if ( ( i % 7) == 0 ) {
				pos = pos + 2;
			} else {
				pos = pos + 1;
			}
			//printf("iresize goright pos=%d\n", pos );
		} else {
			pos = pos + avgstep;
			//printf("iresize rand pos=%d avgstep=%d\n", pos, avgstep );
		}
		++cnt;
	}

	// good  fix. most-left is smallest
	// updateIndex( _newarr[lastPos], lastPos, _newaindex );

	// _first = pos;
	last = pos;

	// write lock
	if ( _arr ) {
		delete [] _arr;
	}

	if ( _usehash && _hashcol ) {
		delete [] _hashcol;
	}

	_arr = _newarr;
	_newarr = NULL;
	if ( _usehash ) {
		_hashcol = _newhashcol;
		_newhashcol = NULL;
	}
	_arrlen = _newarrlen;
	_last = last;
	_first = first;

	if ( _aindex ) {
		delete _aindex;
	}
	_aindex = _newaindex;
	_newaindex = NULL;

	// release write lock

}
// skip this first
template <class Pair> 
void JagArray<Pair>::reDistributeShrink()
{
	abaxint pos = _newarrlen - 1;
	abaxint first, last;
	bool rc;

	last = pos;
	for ( abaxint i = _arrlen-1; i>=0; --i) {
		if ( _arr[i] == Pair::NULLVALUE ) { continue; } 

		_newarr[pos] = _arr[i];

		rc = insertHash( _newarr[pos], pos, _newhashcol, _newarrlen );
		if ( rc ) updateIndex( _newarr[pos], pos, _newaindex );

		pos -= 3;  
	}

	first = pos;
	if ( pos < 0 ) {
		first = 0;
	}

	// write lock
	if ( _arr ) {
		delete [] _arr;
	}

	if ( _usehash && _hashcol ) {
		delete [] _hashcol;
	}

	_arr = _newarr;
	_newarr = NULL;
	if ( _usehash ) {
		_hashcol = _newhashcol;
		_newhashcol = NULL;
	}
	_arrlen = _newarrlen;
	_last = last;
	_first = first;

	if ( _aindex ) {
		delete _aindex;
	}
	_aindex = _newaindex;
	_newaindex = NULL;

	// release write lock

}


template <class Pair> 
const Pair* JagArray<Pair>::getPred( const Pair &pair ) 
{
	abaxint idx;
	bool rc  = findPred( pair, &idx );
	//prt(("s7501 getPred idx=%d rc=%d _arrlen=%d\n", idx, rc, _arrlen ));
	if ( idx < 0 ) return NULL;
	if  ( rc ) {
		// pair is in array
		idx = prevNonNull(idx-1);
		if ( idx < 0 ) return NULL;
		//prt(("s7502 getPred idx=%d rc=%d _arrlen=%d\n", idx, rc, _arrlen ));
		return (const Pair* )&( _arr[idx] );
	}

	//prt(("s7503 getPred idx=%d rc=%d _arrlen=%d printKey() pair.println()\n", idx, rc, _arrlen ));
	//printKey();
	//pair.println();
	if ( _arr[idx] == Pair::NULLVALUE ) {
		return NULL;
	}
	return (const Pair* )&( _arr[idx] );
}

template <class Pair> 
const Pair* JagArray<Pair>::getSucc( const Pair &pair ) 
{
	abaxint idx;
	bool rc = findSucc( pair, &idx );
	if ( rc ) {
		return (const Pair* )&( _arr[idx] );
	}
	return NULL;
}



// if exact item is found, return true; else false
// returns true if item actually exists in array
template <class Pair> 
bool JagArray<Pair>::findPred( const Pair &newpair, abaxint *retindex ) 
{
	bool rc;
	//abaxint i;
	abaxint index;

	abaxint first, last;

	rc = _aindex->findFirstLast( newpair, &first, &last );
	if ( ! rc ) {
		first = _first; last = _last;
	}
	else {
		if ( last > _arrlen -1 ) { last = _arrlen -1; }
		if ( last < 0 )   { last = 0; }
	}

	rc = findPred( newpair, &index, first, last );
	*retindex = index;
	if ( rc ) {
		return true;
	}

	return false;
}

// if successor of pair is found, return true; else false
template <class Pair> 
bool JagArray<Pair>::findSucc( const Pair &pair, abaxint *retindex ) 
{
	findPred( pair, retindex );
	*retindex = nextNonNull( *retindex + 1 );
	if ( _arrlen == *retindex ) return false;
	return true; 
}

// returns false if item actually exists in array
// otherwise returns true
template <class Pair> 
bool JagArray<Pair>::insert( const Pair &newpair, abaxint *retindex, bool isUpsert )
{
	bool rc;
	abaxint i;
	abaxint index;

	if ( ( 10*_GEO*_elements ) >= 8* _arrlen ) {
		reAllocDistribute();
	}

	if ( _elements < 1 ) {
		index = 1;
		index = _arrlen/2;
		_first = _last = index;
		_arr[index] = newpair;
		++ _elements;
		_old = newpair;
		*retindex = index;
		rc = insertHash( newpair, index, _hashcol, _arrlen );
		if ( rc ) { 
			updateIndex( newpair, index, _aindex );
		}
		return true;
	}

	abaxint first, last;
	rc = _aindex->findFirstLast( newpair, &first, &last );
	if ( ! rc ) {
		last = first + _aindex->BLOCK - 1;
	}
	else {
		if ( last > _arrlen -1 ) { last = _arrlen -1; }
		if ( last < 0 )   { last = 0; }
	}

	rc = findPred( newpair, &index, first, last );
	if ( rc ) {
		*retindex = index;
		if ( isUpsert ) {
			_arr[index] = newpair;
			return true;
		}
		return false;
	}

	// if insert with decrease-order, skip left one position
	if ( index+1 == _first && index-2 >=0  ) 
	{
		index -= 2;  
		_first = index;
		_arr[index] = newpair;  
		++_elements;
		_hotspot->insert( utime(), (double)(index)/(double)_arrlen, newpair );

		if ( index > _last ) { _last = index; }
		rc = insertHash( newpair, index, _hashcol, _arrlen );
		if ( rc ) { 
			updateIndex( newpair, index, _aindex );
			_hotspot->insert( utime(), (double)(index)/(double)_arrlen, newpair );
		}

		_old = newpair;
		*retindex = index;
		return true;
	}

	// if insert with increase order, skip to right two position
	if ( index == _last && index+2 < _arrlen ) {
		// printf("c5003 increasing order index=%d\n", index );
		// loadfactor 1/2
		index += 2; 
		_last = index;
		_arr[index] = newpair;  
		++_elements;
		_hotspot->insert( utime(), (double)(index)/(double)_arrlen, newpair );

		if ( index < _first ) { _first = index; }
		rc = insertHash( newpair, index, _hashcol, _arrlen );
		if ( rc ) { 
			updateIndex( newpair, index, _aindex );
		}

		_old = newpair;
		*retindex = index;
		return true;
	}

	if ( index >= 0 && _arr[index] == Pair::NULLVALUE ) 
	{
		_arr[index] = newpair;  
		++_elements;
		_hotspot->insert( utime(), (double)(index)/(double)_arrlen, newpair );

		if ( index < _first ) { _first = index; }
		if ( index > _last ) { _last = index; }

		rc = insertHash( newpair, index, _hashcol, _arrlen );
		if ( rc ) {
			updateIndex( newpair, index, _aindex );
		}
		_old = newpair;
		*retindex = index;
		return true;
	}

	abaxint left, right;
	left = right = index;   // pred position
	bool res = false;
	abaxint  newindex, pos, insertpoint;
	bool leftfound, rightfound;

	leftfound = rightfound = 0;
	while ( 1 )
	{
		if ( right >= _arrlen ) {
			--left;
		} else if ( left < 0 ) {
			++right;
		} else {
			--left;
			++right;
		}

		if ( right < _arrlen && _arr[right] == Pair::NULLVALUE ) {
			rightfound = true;
			break;
		}

		if ( left >=0 && _arr[left] == Pair::NULLVALUE ) {
			leftfound = true;
			break;
		}
	}

	if ( leftfound ) 
	{
			// shift left  [left,index-1]  <===  [left+1, index]
			// printf("c4401 leftshift shiftleft [%d %d]  <-- [%d %d]\n", left, index-1, left+1, index );
			for ( i = left; i <= index-1; ++i) {
				_arr[i] = _arr[i+1];
				rc = updateHash( _arr[i], i+1, i );
				if ( rc ) {
					if (  ( (i%_aindex->BLOCK) == 0 ) ) { 
						updateIndex( _arr[i], i, _aindex, true ); 
					} else if (  ( (i%_aindex->BLOCK) == _aindex->BLOCK-1 ) ) {
						updateIndex( _arr[i], i, _aindex ); 
					}
				}
			}

			// if N*0 -- left-1 all empty, update at left
			// code ok
			if ( ( left%_aindex->BLOCK) != 0 ) {
				abaxint block = left / _aindex->BLOCK;
				bool allempty = true;
				for ( i = left-1; i>=block*_aindex->BLOCK; --i) {
					if ( _arr[i] != Pair::NULLVALUE ) {
						allempty = false;
						break;
					}
				}
				if ( allempty ) {
					updateIndex( _arr[left], left, _aindex ); 
				}
			}

			if ( index-left>0 ) {
				_shifts += index-left;
			}

			if ( left < _first ) {
				_first = left;
			}

			if ( index < 0 ) {
				insertpoint = index+1;
			} else {
				insertpoint = index;
			}

			_arr[insertpoint] = newpair;
			*retindex = insertpoint;
			_hotspot->insert( utime(), (double)(insertpoint)/(double)_arrlen, newpair );

			insertHash( newpair, insertpoint, _hashcol, _arrlen );
			if ( ( insertpoint%_aindex->BLOCK) == 0 ) {
				updateIndex( newpair, insertpoint, _aindex, true );
			} else {
				updateIndex( newpair, insertpoint, _aindex );
			}
			if ( insertpoint < _first ) {
				_first = insertpoint;
			}
	}
	else if ( rightfound ) 
	{
			// printf("c4402 rightshift shiftright [%d %d]  --> [%d %d]\n", index+1, right-1, index+2, right );
			for ( i = right; i >= index+2; --i) {
				_arr[i] = _arr[i-1];
				rc = updateHash( _arr[i], i-1, i );
				if ( ( i%_aindex->BLOCK) == 0 ) {
					updateIndex( _arr[i], i, _aindex ); 
				}
			}

			if (right-index>1) {
				_shifts += right-index-1;
			}
			if ( right > _last ) {
				_last = right;
			}

			_arr[index+1] = newpair;  // right position
			*retindex = index+1;
			_hotspot->insert( utime(), (double)(index+1)/(double)_arrlen, newpair );

			rc = insertHash( newpair, index+1, _hashcol, _arrlen );
			if ( rc ) {
				updateIndex( newpair, index+1, _aindex );
			}

			if ( index+1 < _first ) {
				_first = index+1;
			}
	}

	++_elements;
	_old = newpair;

	return true;
}

// skip this first
/**
*   Remove a pair from the array
*
**/
template <class Pair> 
bool JagArray<Pair>::remove( const Pair &pair, AbaxDestroyAction action )
{
	abaxint index, hloc, i;
	bool rc = exist( pair, &index, &hloc );
	if ( ! rc ) return false;

	if ( action != ABAX_NOOP ) {
		_arr[index].valueDestroy( action ); 
	}

	Pair npair;
	setGetpos( index+1 );
	rc = getNext( npair );
	if ( !rc || _getpos-1 >= index/JAG_BLOCK_SIZE*JAG_BLOCK_SIZE+JAG_BLOCK_SIZE ) {
		i = index;
		npair = Pair::NULLVALUE;
	} else {
		i = _getpos-1;
	}
	i /= JAG_BLOCK_SIZE;
	_aindex->deleteIndex( pair, npair, i );
	_arr[index] = Pair::NULLVALUE;
	-- _elements;
	rehashCluster( hloc );

	if ( _arrlen >= 64 ) {
    	abaxint loadfactor  = 100 * (abaxint)_elements / _arrlen;
    	if (  loadfactor < 15 ) {
    		reAllocDistributeShrink();
    	}
	} 

	return true;
}

/**
*   Remove the first pair from the array
*
**/
template <class Pair> 
bool JagArray<Pair>::removeFirst( AbaxDestroyAction action )
{
	Pair &firstPair = _arr[_first];
	return remove( firstPair, action );
}

/**
*   Remove the last pair from the array
*
**/
template <class Pair> 
bool JagArray<Pair>::removeLast( AbaxDestroyAction action )
{
	Pair &pair = _arr[_last];
	return remove( pair, action );
}

template <class Pair> 
bool JagArray<Pair>::exist( const Pair &search, abaxint *index, abaxint *hloc )
{
	if ( _usehash ) {
        abaxint hc = hashKey( search, _arrlen );
        abaxint idx = _hashcol[ hc ];
    	*index = idx;
        
       	//  hash to NULL, not found.
        if ( ABAX_HASHARRNULL == idx ) {
       		return 0;
        }
           
        if ( search != _arr[idx] )  {
       		hc = findProbedLocation( search, hc );
       		if ( hc < 0 ) {
       			return 0;
       		}
    
    		idx = _hashcol[hc];
       	}
        
       	*index = idx;
    	*hloc = hc;
        return 1;
	}

	bool rc = findPred( search, index );
	*hloc = 0;
    return rc;
}


template <class Pair> 
inline bool JagArray<Pair>::get( Pair &pair )
{
	abaxint index;
	abaxint hloc;
	bool rc;

	rc = exist( pair, &index, &hloc );
	if ( ! rc ) {
		return false;
	}

	pair = _arr[index];
	return true;
}

template <class Pair> 
inline bool JagArray<Pair>::set( const Pair &pair )
{
	abaxint index;
	abaxint hloc;
	bool rc;

	rc = exist( pair, &index, &hloc );
	if ( ! rc ) return false;

	_arr[index].value = pair.value;
	return true;
}


// equal element or predecessor of desired
// return true if item desired exists actually exists in array
template <class Pair> 
bool JagArray<Pair>::findPred( const Pair &desired, abaxint *index, abaxint first, abaxint last ) 
{
    register abaxint mid; 
	bool found = 0;
	abaxint i;

	*index = -1;

	if ( _elements == 0 ) {
		return 0; //  not found
	}

    for ( i = first; i < _arrlen; ++i ) {
        if ( _arr[i] != Pair::NULLVALUE ) {
            break;
        }
    }
    if ( i == _arrlen ) { first = _arrlen-1; } else { first = i; }


	if ( _elements == 1 ) {
		// while ( _arr[first] == Pair::NULLVALUE ) ++first;
		if ( _arr[first] == desired ) {
			found = 1;
			*index = first;
		} else if ( _arr[first] < desired ) {
			*index = first;
		} else {
			*index = first-1;
		}

		return found;
	}

	// while ( first < _arrlen &&  _arr[first] == Pair::NULLVALUE ) ++first;
	if ( first < _first ) {
		_first = first;
	}
	
	found = binSearchPred( desired, index, _arr, _arrlen, first, last );
	return found;
}


// equal element or predecessor of desired
// aassume: _elements > 1
// return true if item desired actually exists in array
template <class Pair> 
bool binSearchPred( const Pair &desired, abaxint *index, const Pair *arr, abaxint arrlen, abaxint first, abaxint last )
{
	//for (int j = 0; j < arrlen; j++) printf("%s\n", (arr[j]).key.addr());
    register abaxint mid; 
	register int  rc1;
	bool found = 0;

	*index = -1;

	while ( arr[last] == Pair::NULLVALUE && last > first ) --last; 
	while ( arr[first] == Pair::NULLVALUE && first < last ) ++first;
	mid = (first + last) / 2;
	while ( arr[mid] == Pair::NULLVALUE && mid > first ) --mid;

	if ( desired < arr[first] ) {
		*index = first-1;
		return false;
	}

    while( first <= last ) {
		if (  arr[last] < desired ) {
			*index = last;
			break;
		}

		if ( arr[mid] == desired ) { 
			rc1 = 0; 
			found = 1;
			*index = mid;
			break;
		}

		if ( desired < arr[mid] ) { rc1 = -1; } 
		else { rc1 = 1; }

		if (  rc1 < 0 ) {
			last = mid - 1;
			while ( last >=0 && arr[last] == Pair::NULLVALUE ) --last; 

		} else {
			if ( last == mid +1 ) {
				if ( desired < arr[last] ) {
					*index = mid;
					break;
				}
			}

			first = mid + 1;
			while ( first < arrlen && arr[first] == Pair::NULLVALUE  ) ++first;
			if ( first == arrlen ) continue;

			// if mid is < desired, but first is bigger, then it is mid 
			if ( arr[first] > desired ) {
				*index = mid;
				break;
			}
		}
		
    	mid = (first + last) / 2;
		while ( mid >= first &&  arr[mid] == Pair::NULLVALUE ) --mid;

    }    // while( first <= last )

	return found;
}

template <class Pair> 
bool JagArray<Pair>::insertHash( const Pair &key, abaxint pos, abaxint* hash, abaxint arrlen )
{
	if ( ! _usehash ) return 1;  // must be 1

	if ( key == Pair::NULLVALUE ) { return 0; }

	abaxint hc = hashKey( key, arrlen ); 
	if ( hash[hc] != ABAX_HASHARRNULL ) {
		hc = probeLocation( hc, hash, arrlen );
	}
	hash[hc] = pos;
	return 1;
}

template <class Pair> 
inline void JagArray<Pair>::updateIndex( const Pair &key, abaxint pos,  JagBlock<Pair> *aindex, bool force )
{
	aindex->updateIndex( key, pos, force );
}

/****
template <class Pair> 
inline void JagArray<Pair>::setNull( const Pair &pair, abaxint pos,  JagBlock<Pair> *aindex )
{
	aindex->setNull( pair, pos );
}
***/

template <class Pair> 
bool JagArray<Pair>::updateHash( const Pair &key, abaxint oldpos, abaxint newpos )
{
	if ( ! _usehash ) return 1;  // must be 1

	if ( key == Pair::NULLVALUE ) { return 0; }

	abaxint hc = hashKey( key, _arrlen );
	if ( _hashcol[hc] != ABAX_HASHARRNULL && oldpos != _hashcol[hc] ) {
   		hc = findProbedLocation( key, hc );
		if ( hc < 0 ) {
			hc = probeLocation( hc, _hashcol, _arrlen );
		}
	}
	_hashcol[hc] = newpos;

	return 1;
}


template <class Pair> 
abaxint JagArray<Pair>::probeLocation( abaxint hc, abaxint* hash, abaxint arrlen )
{
	int cnt = 0;

    while ( 1 ) {
    	hc = nextHC( hc, arrlen );
    	if ( hash[hc] == ABAX_HASHARRNULL ) { return hc; }

		++cnt;
		if ( cnt > 1000000 ) {
			printf("e9492 error probe exit\n");
			exit(1);
		}
   	}
   	return -1;
}
    
// retrieve the previously assigned probed location
// inline int findProbedLocation( int index, const PairType &search, int idx ) 
template <class Pair> 
abaxint JagArray<Pair>::findProbedLocation( const Pair &search, abaxint hc ) 
{
   	while ( 1 ) {
   		hc = nextHC( hc, _arrlen );
  
 		if ( _hashcol[hc] >= _arrlen ) {
   			_hashcol[hc] = ABAX_HASHARRNULL;
   		}
    
   		if ( ABAX_HASHARRNULL == _hashcol[hc] ) {
   			return -1; // no probed slot found
   		}
    
   		if ( search == _arr[_hashcol[hc]] ) {
   			return hc;
   		}
   	}
   	return -1;
}
    
    
// _hashcol[pos] must not be equal to ABAX_HASHARRNULL
// find [start, end] of island that contains hc  
// start and end [0, _hashcol*DEPTH-1].  start can be greater than end
// [3, 9]   [12, 3]
template <class Pair> 
inline void JagArray<Pair>::findCluster( abaxint hc, abaxint *start, abaxint *end )
{
   	abaxint i;
   	i = hc;
   
  	// find start
   	while (1 ) {
   		i = prevHC( i, _arrlen );
 		if ( ABAX_HASHARRNULL == _hashcol[i] ) {
    		*start = nextHC( i, _arrlen );
    		break;
    	}
    }
    
    // find end
    i = hc;
    while (1 ) {
    	i = nextHC( i, _arrlen );
  
 		if ( ABAX_HASHARRNULL == _hashcol[i] ) {
    		*end = prevHC( i, _arrlen );
    		break;
    	}
   	}
}
    
template <class Pair> 
inline abaxint JagArray<Pair>::prevHC ( abaxint hc, abaxint arrlen )
{
   	--hc; 
   	if ( hc < 0 ) hc = arrlen-1;
   	return hc;
}
    
template <class Pair> 
inline abaxint JagArray<Pair>::nextHC( abaxint hc, abaxint arrlen )
{
 	++ hc;
   	if ( hc == arrlen ) hc = 0;
   	return hc;
}

template <class Pair> 
inline bool JagArray<Pair>::aboveq( abaxint start, abaxint end, abaxint birthhc, abaxint nullbox )
{
		if ( start < end ) {
			return ( birthhc <= nullbox ) ? true : false;
		} else {
			if ( 0 <= birthhc  && birthhc <= end ) {
				// birthhc += HASHCOLDEPTH;
				birthhc += _arrlen;
			}

			if ( 0 <= nullbox  && nullbox <= end ) {
				// nullbox += HASHCOLDEPTH;
				nullbox += _arrlen;
			}

			return ( birthhc <= nullbox ) ? true : false;
		}
}

template <class Pair> 
inline void JagArray<Pair>::rehashCluster( abaxint hc )
{
	if ( ! _usehash ) return;

		abaxint start, end;
		abaxint nullbox = hc;

		// findCluster( hashcol, hc, &start, &end );
		findCluster( hc, &start, &end );

		abaxint  birthhc;
		bool b;
		abaxint idx;

		_hashcol[hc] = ABAX_HASHARRNULL;

		while ( 1 )
		{
			hc = nextHC( hc, _arrlen );
			idx = _hashcol[hc]; 
			if ( ABAX_HASHARRNULL == idx ) {
				break;
			}

			birthhc = hashKey( _arr[idx], _arrlen );
			if ( birthhc == hc ) {
				continue;  // birth hc
			}

			b = aboveq( start, end, birthhc, nullbox );
			if ( b ) {
				_hashcol[nullbox] = idx;
				// hashback[ idx ] = nullbox;
				_hashcol[hc] = ABAX_HASHARRNULL;
				nullbox = hc;
			}
		}
}


template <class Pair> 
void JagArray<Pair>::printCheck()
{
    abaxint i;
	Pair old = Pair::NULLVALUE;
    for ( i = 1; i < _arrlen; ++i ) {
        if ( _arr[i] != Pair::NULLVALUE && _arr[i] <= old  ) {
            printf("verify error at i=%d\n", i); 
			print();
            exit(1);
        }
    }
}

template <class Pair> 
void JagArray<Pair>::print( bool printEmpty )
{
    for ( abaxint i = 0; i < _arrlen; ++i ) {
		if (  _arr[i] != Pair::NULLVALUE ) {
			printf("i=%d  key=%s ==> value=%s\n", i, _arr[i].key.c_str(), _arr[i].value.c_str() ); 
		} else {
			if ( printEmpty ) {
				printf("i=%d  NULL\n", i ); 
			}
		}
	}
}

template <class Pair> 
void JagArray<Pair>::printKey( )
{
    for ( abaxint i = 0; i < _arrlen; ++i ) {
		if (  _arr[i] != Pair::NULLVALUE ) {
			printf("i=%d ", i );
			 _arr[i].print();
			printf("\n" );
		} 
	}
	printf("\n" );
}

template <class Pair> 
abaxint JagArray<Pair>::utime()
{
	struct timeval now;
	gettimeofday( &now, NULL );
	return (1000000*(abaxint)now.tv_sec + now.tv_usec);
}

typedef JagArray<AbaxPair<AbaxString,AbaxBuffer> >  JagObjectArray;

/***
template <class Pair> 
abaxint JagArray<Pair>::exactCopy( const JagArray &arr2 )
{
	for ( abaxint i =0; i <_arrlen; ++i) {
		_arr[i] = arr2.exactAt(i);
	}
	_elements = arr2._elements;
	_first = arr2._first;
	_last = arr2._last;
	return _arrlen;
}
***/

#endif
