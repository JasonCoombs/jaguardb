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
#ifndef _jag_bounded_arrray_
#define _jag_bounded_arrray_

#include <JagArray.h>

#define JAG_ORDER_MAX 100
#define JAG_ORDER_MIN 1

template <class Pair>
class JagBoundedArray
{
    public:
		JagBoundedArray( abaxint limit, int order=JAG_ORDER_MAX );
		~JagBoundedArray();

		bool insert( const Pair &newpair ); 
		bool exist( const Pair &pair ) { abaxint idx,hc; return _arr->exist(pair, &idx, &hc); }
		bool remove( const Pair &pair, AbaxDestroyAction action=ABAX_NOOP ); 
		bool removeFirst(  AbaxDestroyAction action=ABAX_NOOP ) { return _arr->removeFirst(action); }
		bool get( Pair &pair ) { return _arr->get( pair ); }
		bool set( const Pair &pair )  { return _arr->set( pair ); }
		inline Pair& at( abaxint i ) const { return _arr->at(i); }
		void destroy() { _arr->destroy(); }
		void clean() { destroy(); init(); }
		abaxint size() const { return _arr->size(); }
		const Pair* array() const { return (const Pair*)_arr->array(); }
		abaxint first() const { return _arr->first(); }
		bool isNull( abaxint i ) const { return _arr->isNull( i ); }
		const Pair &firstElement() const { return _arr->firstElement(); }
		abaxint elements() { return _arr->_elements(); }
		void   print( bool printEmpty = true ) { _arr->print( printEmpty ); }
		void   init();

  protected:
  	JagArray<Pair>  *_arr;
	int		_order;
	abaxint _limit;

};


template <class Pair>
JagBoundedArray<Pair>::JagBoundedArray( abaxint limit, int order )
{
	_limit = limit;
	_order = order;
	init();
}

template <class Pair>
void JagBoundedArray<Pair>::init( )
{
	_arr = new JagArray<Pair>( _limit, false );
}

template <class Pair>
JagBoundedArray<Pair>::~JagBoundedArray( )
{
	delete _arr;
}

// true if insert oK
// false if item is already in the array
template <class Pair>
bool JagBoundedArray<Pair>::insert( const Pair &newpair ) 
{ 
	abaxint idx;
	bool rc = _arr->insert(newpair, &idx); 
	if ( rc ) {
    	if ( _arr->_elements > _limit ) {
    	    if ( _order == JAG_ORDER_MAX ) {
				_arr->removeFirst();
    	    } else {
				_arr->removeLast();
    	    }
    	}
	}

	return rc;
}

#endif
