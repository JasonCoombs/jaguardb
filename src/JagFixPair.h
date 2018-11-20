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
#ifndef _jag_fixpair_h_
#define _jag_fixpair_h_

class JagFixPair
{
    // friend abaxstream& operator<< ( abaxstream &os, const JagFixPair & pair );

    public:

		char *addr;
		int  klen;
		int  vlen;

		static  JagFixPair  NULLVALUE;

        int compareKeys( const JagFixPair &d2 ) const 
		{
			if ( addr == NULL || *addr == '\0' ) {
				if ( d2.klen<1 || d2.addr == NULL || *d2.addr == '\0' ) {
					return 0;
				} else {
					return -1;
				}
			} else {
				if ( d2.addr == NULL || *d2.addr == '\0' ) {
					return 1;
				} else {
					return ( memcmp(addr, d2.addr, keylen ) );
				}
			}
		}
		
		JagFixPair( ) 
		{
			addr = NULL; klen = vlen = 0;
		}

        JagFixPair( const  char *str, int inklen, int invlen )
		{
			if ( str=NULL || *str == '\0' || inklen<=0 ) {
				addr = NULL; klen = vlen = 0;
				return;
			}

			addr = jmalloc(klen+vlen);
			klen = inklen;
			vlen = invlen;

		}

        JagFixPair( const AbaxFixPair &p )
		{
			if ( p.addr=NULL || *p.addr == '\0' || p.klen<=0 ) {
				if ( addr ) free( addr );
				addr = NULL; klen = vlen = 0;
				return;
			}

			free( addr );
			klen = p.klen;
			vlen = p.vlen;
			addr = jmalloc( p.klen + p.vlen );
			memcpy( addr, p.addr,  p.klen + p.vlen );
		}

		// operators
        inline int operator< ( const JagFixPair &d2 ) const {
    		return ( compareKeys( d2 ) < 0 ? 1:0 );
		}
		
        inline int operator> ( const JagFixPair &d2 ) const {
    		return ( compareKeys( d2 ) > 0 ? 1:0 );
		}

        inline int operator >= ( const JagFixPair &d2 ) const
        {
    		return ( compareKeys( d2 ) >= 0 ? 1:0 );
        }
        inline int operator== ( const JagFixPair &d2 ) const
        {
    		return ( compareKeys( d2 ) == 0 ? 1:0 );
        }
        inline int operator!= ( const JagFixPair &d2 ) const
        {
    		return ( ! compareKeys( d2 ) == 0 ? 1:0 );
        }

        JagFixPair&  operator= ( const JagFixPair &p )
        {
			if ( addr == p.addr ) return *this;
			if ( p.addr=NULL || *p.addr == '\0' || p.klen<=0 ) {
				if ( addr ) free( addr );
				addr = NULL; klen = vlen = 0;
				return;
			}

			free( addr );
			klen = p.klen;
			vlen = p.vlen;
			addr = jmalloc( p.klen + p.vlen );
			memcpy( addr, p.addr,  p.klen + p.vlen );
        }

		inline void valueDestroy( AbaxDestroyAction action ) { }

		long hashCode() const {
			return key.hashCode();
		}

		void print() {
			// printf("s0293 JagFixPair key=%s/%d  value=%s/%d\n", key.c_str(), key.size(), value.c_str(), value.size() ); 
			int i;
			printf("K: ");
			for (i=0; i<klen; ++i ) {
				printf("%d:%c ", i, addr[i] );
			}
			printf(" V: ");
			for (i=0; i<vlen; ++i ) {
				printf("%d:%c ", i, addr[klen+i] );
			}
			printf("\n");
		}

		inline abaxint size() { return klen+vlen; }
};


#endif
