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
#ifndef _jag_dbpair_h_
#define _jag_dbpair_h_

#include <math.h>
#include "JagHashMap.h"

class JagDBPair
{
    friend abaxstream& operator<< ( abaxstream &os, const JagDBPair & pair );

    public:
        JagFixString  key;
        JagFixString  value;
		//bool           upsertFlag;
		static  JagDBPair  NULLVALUE;
		
		JagDBPair( ) { /*upsertFlag = 0;*/ } 
        JagDBPair( const JagFixString &k ) : key(k) { /*upsertFlag = 0;*/ }
        JagDBPair( const JagFixString &k, const JagFixString &v ) : key(k), value(v) { /*upsertFlag = 0;*/ } 
		void point( const JagFixString &k, const JagFixString &v ) {
			key.point(k.addr(), k.size() );
			value.point( v.addr(), v.size() );
		}

		void point( const JagFixString &k ) {
			key.point(k.addr(), k.size() );
		}


		void point( const char *k, jagint klen ) {
			key.point( k, klen);
		}

        JagDBPair( const JagFixString &k, const JagFixString &v, bool ref ) 
		{
			//upsertFlag = 0;
			key.point(k.addr(), k.size() );
			value.point( v.addr(), v.size() );
		}

        JagDBPair( const JagFixString &k, bool point ) 
		{
			//upsertFlag = 0;
			key.point(k.addr(), k.size() );
		}

        JagDBPair( const char *kstr, jagint klen, const char *vstr, jagint vlen ) 
		{
			//upsertFlag = 0;
			//key = JagFixString(kstr, klen);
			//value = JagFixString(vstr, vlen);

			key = JagFixString(kstr, klen, klen);
			value = JagFixString(vstr, vlen, vlen);
		}

        JagDBPair( const char *kvstr, jagint klen, jagint vlen ) 
		{
			//upsertFlag = 0;
			//key = JagFixString(kvstr, klen);
			//value = JagFixString(kvstr+klen, vlen);
			key = JagFixString(kvstr, klen, klen );
			value = JagFixString(kvstr+klen, vlen, vlen );
		}

		void point( const char *k, jagint klen,  const char *v, jagint vlen ) {
			key.point( k, klen);
			value.point( v, vlen );
		}

        JagDBPair( const char *kstr, jagint klen, const char *vstr, jagint vlen, bool point ) 
		{
			//upsertFlag = 0;
			key.point(kstr, klen);
			value.point(vstr, vlen);
		}

        JagDBPair( const char *kstr, jagint klen ) 
		{
			//key = JagFixString(kstr, klen);
			key = JagFixString(kstr, klen, klen );
		}

        JagDBPair( const char *kstr, jagint klen, bool point ) 
		{
			key.point(kstr, klen);
		}

        int compareKeys( const JagDBPair &d2 ) const {
			if ( key.addr() == NULL || key.addr()[0] == '\0' ) {
				if ( d2.key.size()<1 || d2.key.addr() == NULL || d2.key.addr()[0] == '\0' ) {
					return 0;
				} else {
					return -1;
				}
			} else {
				if ( d2.key.addr() == NULL || d2.key.addr()[0] == '\0' ) {
					return 1;
				} else {
					return ( memcmp(key.addr(), d2.key.addr(), key.size() ) );
				}
			}
		}

		// operators
        inline int operator< ( const JagDBPair &d2 ) const {
    		return ( compareKeys( d2 ) < 0 ? 1:0 );
		}
        inline int operator<= ( const JagDBPair &d2 ) const {
    		return ( compareKeys( d2 ) <= 0 ? 1:0 );
		}
		
        inline int operator> ( const JagDBPair &d2 ) const {
    		return ( compareKeys( d2 ) > 0 ? 1:0 );
		}

        inline int operator >= ( const JagDBPair &d2 ) const
        {
    		return ( compareKeys( d2 ) >= 0 ? 1:0 );
        }
        inline int operator== ( const JagDBPair &d2 ) const
        {
    		return ( compareKeys( d2 ) == 0 ? 1:0 );
        }
        inline int operator!= ( const JagDBPair &d2 ) const
        {
    		return ( ! compareKeys( d2 ) == 0 ? 1:0 );
        }

        inline JagDBPair&  operator= ( const JagDBPair &d3 )
        {
            key = d3.key;
            value = d3.value;
			//upsertFlag = d3.upsertFlag;
            return *this;
        }

		inline void valueDestroy( AbaxDestroyAction action ) {
			value.destroy( action );
		}

		inline jagint hashCode() const {
			return key.hashCode();
		}

		inline void print() {
			// printf("s0293 JagDBPair key=%s/%d  value=%s/%d\n", key.c_str(), key.size(), value.c_str(), value.size() ); 
			int i;
			printf("K: ");
			for (i=0; i<key.size(); ++i ) {
				printf("%d:%c ", i, key.c_str()[i] );
			}
			printf(" V: ");
			for (i=0; i<value.size(); ++i ) {
				printf("%d:%c ", i, value.c_str()[i] );
			}
			printf("\n");
		}

		inline jagint size() { return (key.size() + value.size()); }
		void toBuffer(char *buffer) const {
			memcpy(buffer, key.c_str(), key.size() );
			memcpy(buffer + key.size(), value.c_str(), value.size() );
		}

};


#endif
