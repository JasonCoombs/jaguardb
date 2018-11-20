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
#ifndef _jag_fixvector_class_h_
#define _jag_fixvector_class_h_

#include <abax.h>
#include <JagDef.h>

class JagDBPair;

class JagFixGapVector
{
	public:

		JagFixGapVector( );
		void initWithKlen( int klen );
		
		JagFixGapVector( int klen );

		// template <class P> JagFixGapVector( const JagFixGapVector<P>& other );
		// JagFixGapVector<Pair>& operator=( const JagFixGapVector<Pair>& other );

		void init( int sz );
		~JagFixGapVector();

		// inline const Pair & operator[] ( abaxint i ) const { return _arr[i]; }
		const char * operator[] ( abaxint i ) const { return _arr+i*kvlen; }
		void setNull( const char *pair, abaxint i );
		bool isNull( abaxint i )  const;
		bool exist( const char *pair ) { abaxint idx; return exist(pair, &idx); }
		bool exist( const char *pair, abaxint *index );

		// bool remove( const char *pair ); 
		bool get( char *pair ); 
		bool set( const char *pair ); 
		void destroy();
		inline  abaxint capacity() const { return _arrlen; }
		inline  abaxint size() const { return _elements; }
		inline  abaxint last() const { return _last; }
		inline  const char* array() const { return (const char*)_arr; }
		// inline bool append( const char *newpair ) { abaxint idx; return append(newpair, &idx); }
		// bool append( const char *newpair, abaxint *index );
		// bool insertForce( const JagDBPair &pair, abaxint index );
		bool insertForce( const char *newpair, abaxint index );
		bool insertLess( const char *newpair, abaxint index );
		void setValue( int val, bool isSet, abaxint index );
		bool findLimitStart( abaxint &startlen, abaxint limitstart, abaxint &soffset );
		bool setNull();
		abaxint getPartElements( abaxint pos ) const;
		bool cleanPartPair( abaxint pos );
		bool deleteUpdateNeeded( const char *dpair, const char *npair, abaxint pos );
		void print() const;
		// void makeDBPair( abaxint i, JagDBPair &dbpair );

		void 	reAlloc();
		void 	reAllocShrink();

		int         klen, vlen, kvlen;
		char   		*_arr;

	protected:
		abaxint  	_arrlen;

		char   		*_newarr;
		abaxint  	_newarrlen;

		abaxint  	_elements;
		abaxint  	_last;
		static const int _GEO  = 2;	 // fixed
};


#endif
