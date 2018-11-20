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
#ifndef _jaglocal_disk_hash_h_
#define _jaglocal_disk_hash_h_

#include <JagDBPair.h>
#include <JagSingleBuffReader.h>
#include <JagSingleBuffWriter.h>
#include <JagSchemaRecord.h>


class JagLocalDiskHash
{
	public:

		JagLocalDiskHash( const AbaxDataString &filepath, int keylength=16, int vallength=16, int arrlength=32 );
		// JagLocalDiskHash( const AbaxDataString &filepath, JagSchemaRecord *record, int arrlength=32 );
		~JagLocalDiskHash();

		inline bool	insert( const JagDBPair &pair ) { return _insertHash( pair, 1); }
		bool get( JagDBPair &pair ); 
		bool set( const JagDBPair &pair );
		bool remove( const JagDBPair &pair ); 
		bool setforce( const JagDBPair &pair );
		inline bool exist( const JagDBPair &pair ) { abaxint hc; return _exist( 1, pair, &hc ); }

		void    setConcurrent( bool flag );
		void 	drop();

		void destroy();
		abaxint size() const { return _arrlen; }
		int getFD() const { return _fdHash; }
		abaxint keyLength() const { return KEYLEN; }
		abaxint valueLength() const { return VALLEN; }
		abaxint keyValueLength() const { return KVLEN; }
		AbaxDataString getName() { return _hashname; }

		abaxint elements() { return _elements; }
		void print();
		void printnew();
		
	protected:
		void    init( const AbaxDataString &fileName, int arrlength );
		void 	reAllocDistribute();
		void 	reAllocShrink();
		// bool 	updateHash( const JagDBPair &pair );
		void 	rehashCluster( abaxint hc );
		abaxint countCells( );
		bool _insertAt(int fdHash, const JagDBPair& pair, abaxint hloc);

		char  *makeKeyValueBuffer( const JagDBPair &pair );
		bool  _exist( int current, const JagDBPair &pair, abaxint *hc );
		bool  _insertHash( const JagDBPair &pair, int current );

		abaxint 	hashKey( const JagDBPair &key, abaxint arrlen ) { return key.hashCode() % arrlen; }

    	abaxint 	probeLocation( abaxint hc, const int fdHash, abaxint arrlen );
    	abaxint 	findProbedLocation( int fdHash, abaxint arrlen, const JagDBPair &search, abaxint hc ) ;
    	void 		findCluster( abaxint hc, abaxint *start, abaxint *end );
    	abaxint 	prevHC ( abaxint hc, abaxint arrlen );
    	abaxint 	nextHC( abaxint hc, abaxint arrlen );
		bool     	aboveq( abaxint start, abaxint end, abaxint birthhc, abaxint nullbox );

		abaxint  	_arrlen;
		abaxint  	_newarrlen;

		abaxint  	_elements;
		AbaxDataString _hashname;
		AbaxDataString _newhashname;
		
		abaxint KEYLEN; 
		abaxint VALLEN;
		abaxint KVLEN;

		char		*_NullKeyValBuf;
		int 		_fdHash;
		int			_fdHash2;
		JagSchemaRecord  *_schemaRecord;

		static const int _GEO  = 2;	 // fixed

		// RayReadWriteLock    *_lock;
		bool				_doLock;

};

#endif
