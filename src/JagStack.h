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
#ifndef _jag_stack_h_
#define _jag_stack_h_

template <class Pair>
class JagStack
{
	public:
		JagStack( int initSize = 4 );
		JagStack( const JagStack<Pair> &str );
		JagStack<Pair>& operator=( const JagStack<Pair> &s2 );
		~JagStack();

		void	clean( int initSize = 4 );
		void 	push( const Pair &pair );
		void 	pop();
		const Pair &top() const;
		const Pair & operator[](int i) const;
		inline  abaxint size() const { return _last+1; }
		inline  abaxint capacity() const { return _arrlen; }
		void 	destroy();
		void 	print();
		void 	reAlloc();
		void 	reAllocShrink();
		void    concurrent( bool flag = true );
		inline bool    empty() { if ( _last >=0 ) return false; else return true; }

	protected:

		Pair   		*_arr;
		abaxint  	_arrlen;
		abaxint  	_last;
		static const int _GEO  = 2;
};

// ctor
template <class Pair> 
JagStack<Pair>::JagStack( int initSize )
{
	//_arr = new Pair[initSize];
	_arr = (Pair*)calloc( initSize, sizeof( Pair ) );
	_arrlen = initSize;
	_last = -1;
}

// copy ctor
template <class Pair> 
JagStack<Pair>::JagStack( const JagStack<Pair> &str )
{
	if ( _arr == str._arr ) return;
	if ( _arr ) free( _arr );

	_arrlen = str._arrlen;
	_last = str._last;
	_arr = (Pair*)calloc( _arrlen, sizeof( Pair ) ); 
	for ( int i = 0; i < _arrlen; ++i ) {
		_arr[i] = str._arr[i];
	}
}

// assignment operator
template <class Pair> 
JagStack<Pair>& JagStack<Pair>::operator=( const JagStack<Pair> &str )
{
	if ( _arr == str._arr ) return *this;
	if ( _arr ) free( _arr );

	_arrlen = str._arrlen;
	_last = str._last;
	_arr = (Pair*)calloc( _arrlen, sizeof( Pair ) ); 
	for ( int i = 0; i < _arrlen; ++i ) {
		_arr[i] = str._arr[i];
	}
	return *this;
}

// dtor
template <class Pair> 
void JagStack<Pair>::destroy( )
{
	if ( ! _arr ) return;
	if ( _last < 0 ) {
		// delete [] _arr;
		if ( _arr ) free ( _arr );
		_arrlen = 0;
		_last = -1;
		_arr = NULL;
		return;
	}

	// delete [] _arr;
	if ( _arr ) free ( _arr );
	_arrlen = 0;
	_last = -1;
	_arr = NULL;
}

// dtor
template <class Pair> 
JagStack<Pair>::~JagStack( )
{
	destroy();
}

template <class Pair>
void JagStack<Pair>::clean( int initSize )
{
	destroy();
	_arr = (Pair*)calloc( initSize, sizeof( Pair ) );
	_arrlen = initSize;
	_last = -1;
}

template <class Pair> 
void JagStack<Pair>::reAlloc()
{
	abaxint i;
	abaxint newarrlen  = _GEO*_arrlen; 
	Pair *newarr;

	// newarr = new Pair[newarrlen];
	newarr = (Pair*)calloc( newarrlen, sizeof( Pair ) );
	for ( i = 0; i <= _last; ++i) {
		newarr[i] = _arr[i];
	}
	// delete [] _arr;
	if ( _arr ) free( _arr );
	_arr = newarr;
	newarr = NULL;
	_arrlen = newarrlen;
}

template <class Pair> 
void JagStack<Pair>::reAllocShrink()
{
	abaxint i;
	Pair *newarr;

	abaxint newarrlen  = _arrlen/_GEO; 
	// newarr = new Pair[newarrlen];
	newarr = (Pair*)calloc( newarrlen, sizeof( Pair ) ); 
	for ( i = 0; i <= _last; ++i) {
		newarr[i] = _arr[i];
	}

	// delete [] _arr;
	if ( _arr ) free( _arr );
	_arr = newarr;
	newarr = NULL;
	_arrlen = newarrlen;
}

template <class Pair> 
void JagStack<Pair>::push( const Pair &newpair )
{
	if ( _last == _arrlen-1 ) { reAlloc(); }

	++ _last;
	_arr[_last] = newpair;
}

// back: add end (enqueue end)
template <class Pair> 
const Pair &JagStack<Pair>::top() const
{
	if ( _last < 0 ) {
		printf("s5004 stack empty, error top()\n");
		//exit(1);
		throw 2920;
	} 
	return _arr[ _last ];
}


template <class Pair> 
void JagStack<Pair>::pop()
{
	if ( _last < 0 ) {
		return;
	} 

	if ( _arrlen >= 64 ) {
    	abaxint loadfactor  = (100 * _last) / _arrlen;
    	if (  loadfactor < 20 ) {
    		reAllocShrink();
    	}
	} 

	-- _last;
}


template <class Pair> 
void JagStack<Pair>::print()
{
	abaxint i;
	printf("c3012 JagStack this=%0x _arrlen=%d _last=%d \n", this, _arrlen, _last );
	for ( i = 0; i  <= _last; ++i) {
		printf("%09d  %d\n", i, _arr[i] );
	}
	printf("\n");
}

template <class Pair> 
const Pair& JagStack<Pair>::operator[](int i) const 
{ 
	if ( i<0 ) { i=0; }
	else if ( i > _last ) { i = _last; }
	return _arr[i];
}

#endif
