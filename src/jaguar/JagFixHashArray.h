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
#ifndef _jag_fixhasharr_h_
#define _jag_fixhasharr_h_
#include <stdio.h>
#include <abax.h>
#include <JagUtil.h>

///////////////////////////////// fix hash array class ///////////////////////////////////
class JagFixHashArray
{
	public:
		int klen;
		int vlen;
		int kvlen;

		JagFixHashArray( int klen, int vlen );
		~JagFixHashArray();

		bool insert( const char* newpair );
		bool exist( const char *pair, abaxint *index );
		bool remove( const char* pair );
		void removeAll();
		bool get( char *pair ); 
		bool get( const char *key, char *val ); 
		bool set( const char *pair ); 
		void destroy();
		abaxint size() const { return _arrlen; }
		abaxint arrayLength() const { return _arrlen; }
		const char* array() const { return (const char*)_arr; }
		abaxint elements() { return _elements; }
		bool isNull( abaxint i ) const;
		void print() const;

	protected:

		void 	reAllocDistribute();
		void 	reAlloc();
		void 	reDistribute();

		void 	reAllocShrink();
		void 	reAllocDistributeShrink();
		void 	reDistributeShrink();

		abaxint	hashKey( const char *key, abaxint arrlen ) const;
    	inline abaxint 	probeLocation( abaxint hc, const char *arr, abaxint arrlen );
    	inline abaxint 	findProbedLocation( const char *search, abaxint hc ) ;
    	inline void 	findCluster( abaxint hc, abaxint *start, abaxint *end );
    	inline abaxint 	prevHC ( abaxint hc, abaxint arrlen );
    	inline abaxint 	nextHC( abaxint hc, abaxint arrlen );
		inline abaxint  hashLocation( const char *pair, const char *arr, abaxint arrlen );
		inline void 	rehashCluster( abaxint hc );
		inline bool 	aboveq( abaxint start, abaxint end, abaxint birthhc, abaxint nullbox );

		char   		*_arr;
		abaxint  	 _arrlen;
		char   		*_newarr;
		abaxint  	 _newarrlen;
		abaxint  	    _elements;
		static const int _GEO  = 2;	 // fixed
};


#endif
