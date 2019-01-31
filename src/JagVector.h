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
#ifndef _jag_simple_vector_class_h_
#define _jag_simple_vector_class_h_

#include <abax.h>

template <class Pair>
class JagVector
{
	public:

		JagVector( int initSize=4 );

		// template <class P> JagVector( const JagVector<P>& other );
		JagVector<Pair>& operator=( const JagVector<Pair>& other );
		JagVector<Pair>( const JagVector<Pair>& other );

		void init( int sz );
		void destroy();
		void clean();
		~JagVector();
		inline bool append( const Pair &newpair, abaxint sz=0 ) { abaxint idx; return append(newpair, &idx, sz); }
		inline const Pair & operator[] ( abaxint i ) const { return _arr[i]; }
		inline Pair & operator[] ( abaxint i ) { return _arr[i]; }

		inline bool exist( const Pair &pair ) { abaxint idx; return exist(pair, &idx); }
		bool exist( const Pair &pair, abaxint *index );

		bool remove( const Pair &pair, AbaxDestroyAction action=ABAX_NOOP ); 
		bool removepos( abaxint posindex, AbaxDestroyAction action=ABAX_NOOP ); 
		bool get( Pair &pair ); 
		bool set( const Pair &pair ); 
		inline  abaxint capacity() const { return _arrlen; }
		inline  abaxint size() const { return _elements; }
		inline  abaxint length() const { return _elements; }
		// inline  abaxint last() const { return _last; }
		inline  const Pair* array() const { return (const Pair*)_arr; }
		bool append( const Pair &newpair, abaxint *index, abaxint sz );
		bool empty() { if ( _elements == 0 ) return true; else return false; }
		abaxint bytes() const { return _bytes; }

		void print() const {
			printf("JagVector::print() _elements=%d :\n", _elements );
			for ( abaxint i = 0; i < _elements; ++i ) {
				printf("i=%d: ", i );
				_arr[i].print();
			}	
			printf("\n" );
			fflush(stdout);
		}

		void printString() const {
			for ( abaxint i = 0; i < _elements; ++i ) {
				printf("i=%d   [%s]\n", i, _arr[i].c_str() );
			}	
		}

		Jstr asString() const {
			Jstr str;
			for ( abaxint i = 0; i < _elements; ++i ) {
				if (  0 == i ) {
					str =  _arr[i];
				} else {
					str += Jstr("|") +  _arr[i];
				}
			}
			return str;
		}


		void 	reAlloc();
		void 	reAllocShrink();
		abaxint 	_elements;
		abaxint   _bytes;


	protected:

		Pair   		*_arr;
		abaxint  	_arrlen;

		// temp vars
		Pair   		*_newarr;
		abaxint  	_newarrlen;

		// abaxint  	_last;

		static const int _GEO  = 2;	 // fixed

};

//////////////////////// vector code ///////////////////////////////////////////////
// ctor
template <class Pair> 
JagVector<Pair>::JagVector( int initSize )
{
	// printf("c7383 default ctor this=%0x JagVector( int initSize=%d ) ...\n", this, initSize );

	init( initSize );
}
template <class Pair> 
void JagVector<Pair>::init( int initSize )
{
	_arr = new Pair[initSize];
	_arrlen = initSize;
	_elements = 0;
	// _last = 0;
	_bytes = 0;

}

// copy ctor
/***
template <class Pair>
template <class P>
JagVector<Pair>::JagVector( const JagVector<P>& other )
{
	// printf("c38484 this=%0x JagVector( const JagVector<P>& other ) size=%d ...\n", this, other._arrlen );

	_arrlen = other._arrlen;
	_elements = other._elements;
	// _last = other._last;

	_arr = new Pair[_arrlen];
	for ( abaxint i = 0; i < _arrlen; ++i ) {
		_arr[i] = other._arr[i];
	}
}
***/
template <class Pair>
JagVector<Pair>::JagVector( const JagVector<Pair>& other )
{
	// printf("c38484 this=%0x JagVector( const JagVector<P>& other ) size=%d ...\n", this, other._arrlen );

	_arrlen = other._arrlen;
	_elements = other._elements;
	// _last = other._last;

	_arr = new Pair[_arrlen];
	for ( abaxint i = 0; i < _arrlen; ++i ) {
		_arr[i] = other._arr[i];
	}
}


// assignment operator
template <class Pair>
JagVector<Pair>& JagVector<Pair>::operator=( const JagVector<Pair>& other )
{
	// printf("c8383 JagVector<Pair>& JagVector<Pair>::operator=() ...\n");
	// fflush ( stdout );
	if ( _arr == other._arr ) {
		return *this;
	}

	if ( _arr ) {
		delete [] _arr;
	}

	_arrlen = other._arrlen;
	_elements = other._elements;
	// _last = other._last;

	_arr = new Pair[_arrlen];
	for ( abaxint i = 0; i < _arrlen; ++i ) {
		_arr[i] = other._arr[i];
	}

	return *this;
}


template <class Pair> 
void JagVector<Pair>::clean( )
{
	destroy();
	init(4);
}

// dtor
template <class Pair> 
void JagVector<Pair>::destroy( )
{
	if ( ! _arr ) {
		// printf("c6183 JagVector destroy _arr is NULL, return\n");
		// fflush( stdout );
		return;
	}

	// debug
	// printString();
	// printf("c6383 JagVector this=%0x destroy delete [] _arr _arrlen=%d ...\n", this, _arrlen );

	if ( _arr ) delete [] _arr; 
	_arr = NULL;

	_bytes = 0;
}

// dtor
template <class Pair> 
JagVector<Pair>::~JagVector( )
{
	destroy();
}


template <class Pair> 
void JagVector<Pair>::reAlloc()
{
	abaxint i;
	_newarrlen  = _GEO*_arrlen; 

	_newarr = new Pair[_newarrlen];
	for ( i = 0; i < _elements; ++i) {
		_newarr[i] = _arr[i];
	}

	// printf("s8383 JagVector<Pair>::reAlloc() \n");

	if ( _arr ) delete [] _arr;
	_arr = _newarr;
	_newarr = NULL;
	_arrlen = _newarrlen;
}

template <class Pair> 
void JagVector<Pair>::reAllocShrink()
{
	abaxint i;

	_newarrlen  = _arrlen/_GEO; 

	_newarr = new Pair[_newarrlen];
	for ( i = 0; i < _elements; ++i) {
		_newarr[i] = _arr[i];
	}

	if ( _arr ) delete [] _arr;
	_arr = _newarr;
	_newarr = NULL;
	_arrlen = _newarrlen;
}


template <class Pair> 
bool JagVector<Pair>::append( const Pair &newpair, abaxint *index, abaxint bytes )
{
	if ( _elements == _arrlen ) { 
		// printf("s9002 this=%0x _elements %d == %d realloc\n", this, _elements, _arrlen );
		reAlloc();
	}

	*index = _elements;
	_arr[_elements++] = newpair;
	// printf("s0303 this=%0x append _elements=%d _arrlen=%d\n", this, _elements, _arrlen );
	_bytes += bytes;
	return true;
}


// Slow, should not use this
template <class Pair> 
bool JagVector<Pair>::remove( const Pair &pair, AbaxDestroyAction action )
{
	abaxint i, index;
	bool rc = exist( pair, &index );
	if ( ! rc ) return false;

	if ( action != ABAX_NOOP ) {
		_arr[index].valueDestroy( action ); 
	}

	// shift left
	for ( i = index; i <= _elements-2; ++i ) {
		_arr[i] = _arr[i+1];
	}
	-- _elements;

	if ( _arrlen >= 64 ) {
    	abaxint loadfactor  = 100 * (abaxint)_elements / _arrlen;
    	if (  loadfactor < 15 ) {
    		reAllocShrink();
    	}
	} 

	/***
	if ( index == _last ) {
		-- _last;
	}
	***/

	return true;
}

// Slow, should not use this
template <class Pair> 
bool JagVector<Pair>::removepos( abaxint posindex, AbaxDestroyAction action )
{
	abaxint i;

	// shift left
	for ( i = posindex; i <= _elements-2; ++i ) {
		_arr[i] = _arr[i+1];
	}
	-- _elements;

	if ( _arrlen >= 64 ) {
    	abaxint loadfactor  = 100 * (abaxint)_elements / _arrlen;
    	if (  loadfactor < 15 ) {
    		reAllocShrink();
    	}
	} 

	/***
	if ( posindex == _last ) {
		-- _last;
	}
	***/

	return true;
}

// scan search  slow
template <class Pair> 
bool JagVector<Pair>::exist( const Pair &search, abaxint *index )
{
    abaxint i; 

	for ( i = 0; i < _elements; ++i ) {
		if ( _arr[i] == search ) {
			*index = i;
			return true;
		}
	}
    
    return 0;
}


template <class Pair> 
inline bool JagVector<Pair>::get( Pair &pair )
{
	abaxint index;
	bool rc;

	rc = exist( pair, &index );
	if ( ! rc ) return false;

	pair.value = _arr[index].value;
	return true;
}

template <class Pair> 
inline bool JagVector<Pair>::set( const Pair &pair )
{
	abaxint index;
	bool rc;

	rc = exist( pair, &index );
	if ( ! rc ) return false;

	_arr[index].value = pair.value;
	return true;
}


#endif
