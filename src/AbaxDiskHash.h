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
#ifndef _abax_disk_hash_h_
#define _abax_disk_hash_h_

#include <stdio.h>
#include <fcntl.h>
#include <sys/types.h>
#include <unistd.h>
#include <string.h>
#include <limits.h>
#include <pthread.h>

#include <AbaxCfg.h>
#include <AbaxArray.h>
#include <AbaxBuffReader.h>
#include <AbaxBuffWriter.h>
#include <RayMutex.h>
#include <RayDBServer.h>
//#include <JDFS.h>
//#include <JDFSMgr.h>

////////////////////////////////////////// array class ///////////////////////////////////

template <class Pair>
class AbaxDiskHash
{
	public:

		AbaxDiskHash( RayDBServer *servobj, const AbaxDataString &filename, int keylength=16, int vallength=16, int arrlength=32 );
		void init( const AbaxDataString &fileName, int arrlength );
		~AbaxDiskHash();
		// JDFS  		*_jdfs;

		inline bool	insert( const Pair &pair ) {
			// return insertHash( pair, _jdfs, _arrlen );
			return insertHash( pair, _arrlen );
		}
		bool get( Pair &pair ); 
		bool set( const Pair &pair );
		bool setforce( const Pair &pair );
		inline bool exist( const Pair &pair ) { abaxint hc; return exist(pair, &hc ); }
		bool remove( const Pair &pair ); 

		// inline bool 	insertHash( const Pair &pair, JDFS *jdfs, abaxint arrlen );
		inline bool 	insertHash( const Pair &pair, abaxint arrlen );
		inline bool 	updateHash( const Pair &pair );
		inline void 	rehashCluster( abaxint hc );
		void 			drop();
		void setConcurrent( bool flag );
		void destroy();
		inline  abaxint size() const { return _arrlen; }
		inline int filedesc() const { return fdHash; }
		inline int keyLength() const { return KEYLEN; }
		inline int valueLength() const { return VALLEN; }
		inline int keyValueLength() const { return KEYVALLEN; }
		AbaxDataString getName() { return _hashname; }

		abaxint elements() { return _elements; }
		abaxint countCells( );
		inline Pair get_pair(char keybuf[], char valbuf[], char buffer[], int keylength, int vallength) const 
		{
			memcpy(keybuf, buffer, keylength);
			memcpy(valbuf, buffer+keylength, vallength);
			AbaxString sk( keybuf );
			AbaxString sv( valbuf );
			Pair pair(sk, sv);
			return pair;
		}

		inline Pair get_pair_key(char keybuf[], char buffer[], int keylength ) const 
		{
			memcpy(keybuf, buffer, keylength);
			AbaxString sk( keybuf );
			AbaxString sv;
			Pair pair(sk, sv);
			return pair;
		}
		
	protected:

		void 	reAllocDistribute();
		void 	reAllocShrink();

		char  *makeKeyValueBuffer( const Pair &pair );
		bool exist( const Pair &pair, abaxint *hc );

		inline abaxint 	hashKey( const Pair &key, abaxint arrlen ) { 
			return key.hashCode() % arrlen; 
		}

    	// inline abaxint 	probeLocation( abaxint hc, JDFS *jdfs, abaxint arrlen );
    	inline abaxint 	probeLocation( abaxint hc, abaxint arrlen );
    	inline abaxint 	findProbedLocation( const Pair &search, abaxint hc ) ;
    	inline void 	findCluster( abaxint hc, abaxint *start, abaxint *end );
    	inline abaxint 	prevHC ( abaxint hc, abaxint arrlen );
    	inline abaxint 	nextHC( abaxint hc, abaxint arrlen );
		inline bool     aboveq( abaxint start, abaxint end, abaxint birthhc, abaxint nullbox );

		abaxint  	_arrlen;
		abaxint  	_newarrlen;
		abaxint  	_elements;
		AbaxDataString _hashname;
		AbaxDataString _newhashname;
		
		int KEYLEN; 
		int VALLEN;
		int KEYVALLEN;

		char		*_NullKeyValBuf;
		int 		fdHash;
		int			fdHash2;

		static const int _GEO  = 2;	 // fixed

		RayReadWriteLock    *_lock;
		bool				_doLock;
		RayDBServer *_servobj;

};


// ctor
template <class Pair> 
AbaxDiskHash<Pair>::AbaxDiskHash( RayDBServer *servobj, const AbaxDataString &filename, int keylength, int vallength, int arrlength )
{
	_servobj = servobj;
	KEYLEN = keylength;
	VALLEN = vallength;
	KEYVALLEN = KEYLEN+VALLEN;
	_NullKeyValBuf = (char*)malloc(KEYVALLEN+1);
	init( filename, arrlength );
	// setConcurrent( true );
}

// todo
template <class Pair> 
void AbaxDiskHash<Pair>::drop( )
{
	::unlink(_hashname.c_str());
}

// ctor
template <class Pair> 
void AbaxDiskHash<Pair>::init( const AbaxDataString &fileName, int length )
{
	_lock = NULL; 

	memset(_NullKeyValBuf, 0, KEYVALLEN+1);
	Pair t_pair = Pair::NULLVALUE;
	
	AbaxDataString dk_hash = ".hdb";
	AbaxDataString dk_hash2 = "Hash7392938.hdb";

	_hashname = fileName; 
	_hashname += dk_hash;
	_newhashname = fileName; 
	_newhashname += dk_hash2;


	// _arrlen = 32;
	// size_t bytes = _arrlen*KEYVALLEN;
	fdHash = -1;


	int exist = 0;
	if ( 0 ==  access( _hashname.c_str(), R_OK|W_OK  ) ) { exist = 1; }
	fdHash = open((char *)_hashname.c_str(), O_CREAT|O_RDWR, S_IRWXU);
	if ( ! exist  ) {
		// printf("s3849 not exist %s\n", _hashname.c_str() );
		_arrlen = length;
		size_t bytesHash = _arrlen*KEYVALLEN;
   		posix_fallocate(fdHash, 0, bytesHash);
		_jdfs = new JDFS( _servobj, _hashname, KEYVALLEN, _arrlen );
		_jdfs->fallocate( 0, bytesHash, true );
		_elements = 0;
		fsetXattr( fdHash, _hashname.c_str(), "user.arrlen", _arrlen );
		fsetXattr( fdHash, _hashname.c_str(), "user.elements", _elements );

	} else {
		// printf("s3859 exist %s countCells...\n", _hashname.c_str() );
		_arrlen = countCells();
		if ( !_arrlen ) _arrlen = length;
		_jdfs = new JDFS( _servobj, _hashname, KEYVALLEN, _arrlen );
	}

}

template <class Pair> 
void  AbaxDiskHash<Pair>::setConcurrent( bool flag )
{
	if ( flag ) {
		if ( ! _lock ) {
			_lock = new RayReadWriteLock();
			printf("AbaxDiskHash _lock=%0x _hashname=[%s]\n", _lock, _hashname.c_str() );
		}
	} else {
		if ( _lock ) {
			delete  _lock;
			_lock = NULL;
			printf("AbaxDiskHash _lock=%0x _hashname=[%s]\n", _lock, _hashname.c_str() );
		}
	}
}


// dtor
template <class Pair> 
void AbaxDiskHash<Pair>::destroy( )
{
	if ( fdHash > 0 ) {
		close (fdHash);
		fdHash = -1;
	}

	if ( _lock ) {
		delete _lock;
		_lock = NULL;
	}
}

// dtor
template <class Pair> 
AbaxDiskHash<Pair>::~AbaxDiskHash( )
{
	destroy();
	free( _NullKeyValBuf );
}




template <class Pair> 
void AbaxDiskHash<Pair>::reAllocDistribute()
{
	abaxint i;
	size_t bytesHash2;

	char *keyvalbuf = (char*)malloc(KEYVALLEN+1);
	char *keybuf = (char*)malloc(KEYLEN+1);
	memset( keyvalbuf, 0, KEYVALLEN+1 );
	memset( keybuf, 0, KEYLEN+1 );

	_newarrlen = _GEO*_arrlen;
	bytesHash2 = _newarrlen*KEYVALLEN;

	fdHash2 = open((char *)_newhashname.c_str(), O_CREAT|O_RDWR, S_IRWXU );
	JDFS *_jdfs2 = new JDFS( _servobj, _newhashname, KEYVALLEN, _newarrlen );
	_jdfs2->fallocate( 0, bytesHash2, false );

	posix_fallocate(fdHash2, 0, bytesHash2 );
	/***
	AbaxBuffWriter<Pair> buffWriter( fdHash2, KEYVALLEN );
	for ( i=0; i < _newarrlen; ++i) {
		buffWriter.writeit( i, (const char*)_NullKeyValBuf, KEYVALLEN );
	}
	buffWriter.flushBuffer(); 
	***/

	// pick from old and insert into new
	Pair t_pair;
	AbaxBuffReader navig( _jdfs, _arrlen, KEYLEN, VALLEN );
	while ( navig.getNext( keyvalbuf, KEYVALLEN, i ) ) {
		t_pair = get_pair_key(keybuf, keyvalbuf, KEYLEN );
		insertHash(t_pair, _jdfs, _newarrlen);
	}

	close(fdHash);
	close(fdHash2);

	free( keyvalbuf );
	free( keybuf );


	::unlink(_hashname.c_str());
	
	rename(_newhashname.c_str(), _hashname.c_str());
	fdHash = open((char *)_hashname.c_str(), O_CREAT|O_RDWR, S_IRWXU );
	_jdfs->rename( _hashname );

	_arrlen = _newarrlen;
	fsetXattr( fdHash, _hashname.c_str(), "user.arrlen", _arrlen );
}


template <class Pair> 
void AbaxDiskHash<Pair>::reAllocShrink()
{
	abaxint i;
	size_t bytesHash2; 
	
	char *keyvalbuf = (char*)malloc(KEYVALLEN+1);
	char *keybuf = (char*)malloc(KEYLEN+1);
	memset( keyvalbuf, 0, KEYVALLEN+1 );
	memset( keybuf, 0, KEYLEN+1 );

	_newarrlen  = _arrlen/_GEO;
	bytesHash2 = _newarrlen*KEYVALLEN;

	fdHash2 = open((char *)_newhashname.c_str(), O_CREAT|O_RDWR, S_IRWXU);
	JDFS *jdfs2 = new JDFS( _servobj,  _newhashname, KEYVALLEN, _newarrlen );
	jdfs2->fallocate( 0, bytesHash2, false );

	posix_fallocate(fdHash2, 0, bytesHash2 );
	/***
	AbaxBuffWriter<Pair> buffWriter( fdHash2, KEYVALLEN );
	for ( i=0; i < _newarrlen; ++i) {
		buffWriter.writeit( i, (const char*)_NullKeyValBuf, KEYVALLEN );
	}
	buffWriter.flushBuffer(); 
	***/

	Pair t_pair;
	// pick from old and insert into new
	AbaxBuffReader navig( _jdfs, _arrlen, KEYLEN, VALLEN );
	while ( navig.getNext( keyvalbuf, KEYVALLEN, i ) ) {
		t_pair = get_pair_key(keybuf, keyvalbuf, KEYLEN );
		insertHash(t_pair, jdfs2, _newarrlen);
	}

	close(fdHash);
	close(fdHash2);

	free( keyvalbuf );
	free( keybuf );

	::unlink(_hashname.c_str());
	
	rename(_newhashname.c_str(), _hashname.c_str());
	fdHash = open((char *)_hashname.c_str(), O_CREAT|O_RDWR, S_IRWXU );
	_jdfs->rename( _hashname );

	_arrlen = _newarrlen;
	fsetXattr( fdHash, _hashname.c_str(), "user.arrlen", _arrlen );

	delete jdfs2;
}


// skip this first
/**
*   Remove a pair from the array
*
**/
template <class Pair> 
bool AbaxDiskHash<Pair>::remove( const Pair &pair )
{
	abaxint  hloc;
	bool rc = exist( pair, &hloc );
	if ( ! rc ) return false;

	rehashCluster( hloc );

	if ( _arrlen >= 64 ) {
    	long loadfactor  = 100 * (long)_elements / _arrlen;
    	if (  loadfactor < 15 ) {
    		reAllocShrink();
    	}
	} 

	return true;
}


template <class Pair> 
bool AbaxDiskHash<Pair>::exist( const Pair &search, abaxint *hloc )
{
	char keyvalbuf[ KEYVALLEN+1];
	char keybuf[KEYLEN+1];
	memset( keyvalbuf, 0, KEYVALLEN+1 );
	memset( keybuf, 0, KEYLEN+1 );

	if ( _arrlen < 1 ) {
		printf("AbaxDiskHash::exist() _arrlen<1  _hashname=[%s] this=%0x\n", _hashname.c_str(), this );
		// exit(1);
	}
    abaxint hc = hashKey( search, _arrlen );
	raypread( fdHash, (char *)keyvalbuf, KEYVALLEN,  hc*KEYVALLEN   );

	// printf("s7490 keyvalbuf=[%s]\n", keyvalbuf );
       
    //  hash to NULL, not found.
    if ( 0 == strcmp( keyvalbuf, _NullKeyValBuf ) ) { 
    	return 0;
    }
           
	Pair t_pair = get_pair_key(keybuf, keyvalbuf, KEYLEN );
    if ( search != t_pair  ) {
       	hc = findProbedLocation( search, hc );
       	if ( hc < 0 ) {
       		return 0;
       	}
    }
        
    *hloc = hc;
    return 1;
}


template <class Pair> 
inline bool AbaxDiskHash<Pair>::get( Pair &pair )
{
	abaxint hloc;
	bool rc;

	rc = exist( pair, &hloc );
	if ( ! rc ) {
		return false;
	}

	char keyvalbuf[ KEYVALLEN+1];
	char keybuf[ KEYLEN+1];
	char valbuf[ VALLEN+1];
	memset( keyvalbuf, 0, KEYVALLEN+1 );
	memset( keybuf, 0, KEYLEN+1 );
	memset( valbuf, 0, VALLEN+1 );
	raypread( fdHash, keyvalbuf, KEYVALLEN, hloc*KEYVALLEN );
	pair = get_pair(keybuf, valbuf, keyvalbuf, KEYLEN, VALLEN);
	return true;
}

template <class Pair> 
inline bool AbaxDiskHash<Pair>::set( const Pair &pair )
{
	abaxint index;
	abaxint hloc;
	bool rc;

	rc = exist( pair, &hloc );
	if ( ! rc ) return false;

	char valbuf[VALLEN+1];
	memset( valbuf, 0, VALLEN+1 );

	const char *ptr = pair.value.addr();
	strncpy( valbuf, ptr, VALLEN );
	raypwrite( fdHash, valbuf, VALLEN, ( hloc*KEYVALLEN)+KEYLEN );
	return true;
}

template <class Pair> 
inline bool AbaxDiskHash<Pair>::setforce( const Pair &pair )
{
	abaxint index;
	abaxint hloc;
	bool rc;

	char valbuf[VALLEN+1];
	memset( valbuf, 0, VALLEN+1 );

	const char *ptr = pair.value.addr();
	strncpy( valbuf, ptr, VALLEN );
	raypwrite( fdHash, valbuf, VALLEN, ( hloc*KEYVALLEN)+KEYLEN );
	return true;
}


template <class Pair> 
bool AbaxDiskHash<Pair>::insertHash( const Pair &pair, JDFS *jdfs, abaxint arrlen )
{

	char keybuf[KEYLEN+1];
	memset( keybuf, 0, KEYLEN+1 );

	Pair null_pair = get_pair_key(keybuf, _NullKeyValBuf, KEYLEN );
	if ( pair.key == null_pair.key ) { free( keybuf); return 0; }

	bool rc = exist( pair );
	if (  rc ) {
		return false;
	}

	char keyvalbuf[KEYVALLEN+1];
	memset( keyvalbuf, 0, KEYVALLEN+1 );
	abaxint hc = hashKey( pair, arrlen );
	jdfpread( jdfs, (char *)keyvalbuf, KEYVALLEN, hc*KEYVALLEN );

	if ( 0 != strcmp( keyvalbuf, _NullKeyValBuf) ) {
		hc = probeLocation( hc, jdfs, arrlen );
	}

	char *newkvbuf = makeKeyValueBuffer( pair );
	jdfpwrite( jdfs, (char*)newkvbuf, KEYVALLEN, hc*KEYVALLEN );
	free( newkvbuf );
	++ _elements;

	int fdHash = _servobj->jdfsMgr->open( _hashname );
	if ( fdHash < 0 ) { return 0; }
	fsetXattr( fdHash, _hashname.c_str(), "user.elements", _elements );

	if ( ( 10*_GEO*_elements ) >=  8*_arrlen ) {
		reAllocDistribute();
	}

	return 1;
}

template <class Pair> 
bool AbaxDiskHash<Pair>::updateHash( const Pair &pair )
{
	char keybuf[ KEYLEN+1];
	memset( keybuf, 0, KEYLEN+1 );
	Pair null_pair = get_pair_key(keybuf, _NullKeyValBuf, KEYLEN );
	if ( pair.key == null_pair.key  ) { return 0; }
	
	char keyvalbuf[ KEYVALLEN+1];
	memset( keyvalbuf, 0, KEYVALLEN+1 );

	abaxint hc = hashKey( pair, _arrlen );
	jdfpread( _jdfs, (char *)keyvalbuf, KEYVALLEN, hc*KEYVALLEN  );

	if ( 0 != strcmp( keyvalbuf, _NullKeyValBuf ) ) { 
   		hc = findProbedLocation( pair, hc );
		if ( hc < 0 ) {
			hc = probeLocation( hc, _jdfs, _arrlen );
		}
	}
	
	char *newkeyvalbuf = makeKeyValueBuffer( pair );
	raypwrite(fdHash, (char *)newkeyvalbuf, KEYVALLEN, hc*KEYVALLEN );
	free( newkeyvalbuf );
	return 1;
}

template <class Pair> 
abaxint AbaxDiskHash<Pair>::probeLocation( abaxint hc, JDFS *jdfs, abaxint arrlen )
{
	int cnt = 0;
	char keyvalbuf[ KEYVALLEN+1];
	memset( keyvalbuf, 0, KEYVALLEN+1 );

    while ( 1 ) {
    	hc = nextHC( hc, arrlen );
		raypread( jdfs, (char *)keyvalbuf, KEYVALLEN, hc*KEYVALLEN );
		if ( 0 == strcmp( keyvalbuf, _NullKeyValBuf ) ) {
			return hc;
		}

		++cnt;
		if ( cnt > 100000 ) {
			printf("e5492 error probe exit\n");
			exit(1);
		}
   	}
   	return -1;
}
    
// retrieve the previously assigned probed location
// Input: fdHash
template <class Pair> 
abaxint AbaxDiskHash<Pair>::findProbedLocation( const Pair &search, abaxint hc ) 
{
	char keybuf[ KEYLEN+1];
	char keyvalbuf[ KEYVALLEN+1];
	memset( keyvalbuf, 0, KEYVALLEN+1 );
	memset( keybuf, 0, KEYLEN+1 );

   	while ( 1 ) {
   		hc = nextHC( hc, _arrlen );
		raypread(fdHash, (char *)keyvalbuf, KEYVALLEN, hc*KEYVALLEN ); 		

   		if ( 0==strcmp( keyvalbuf, _NullKeyValBuf ) ) { 
   			return -1; // no probed slot found
   		}
    
		memcpy(keybuf, keyvalbuf, KEYLEN);
   		if ( 0 == strncmp( keybuf, search.key.addr(), KEYLEN ) ) { 
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
inline void AbaxDiskHash<Pair>::findCluster( abaxint hc, abaxint *start, abaxint *end )
{
   	abaxint i;
   	i = hc;

	char keyvalbuf[KEYVALLEN+1];
	memset( keyvalbuf, 0, KEYVALLEN+1 );
   
  	// find start
   	while ( 1 ) {
   		i = prevHC( i, _arrlen );
		raypread(fdHash, (char *)keyvalbuf, KEYVALLEN, i*KEYVALLEN );
 		if ( 0 == strcmp( keyvalbuf, _NullKeyValBuf ) ) {
    		*start = nextHC( i, _arrlen );
    		break;
    	}
    }
    
    // find end
    i = hc;
    while ( 1 ) {
    	i = nextHC( i, _arrlen );
  		
		raypread(fdHash, (char *)keyvalbuf, KEYVALLEN, i*KEYVALLEN );
 		if ( 0 == strcmp( keyvalbuf, _NullKeyValBuf ) ) {
    		*end = prevHC( i, _arrlen );
    		break;
    	}

   	}
}
    
template <class Pair> 
inline abaxint AbaxDiskHash<Pair>::prevHC ( abaxint hc, abaxint arrlen )
{
   	--hc; 
   	if ( hc < 0 ) hc = arrlen-1;
   	return hc;
}
    
template <class Pair> 
inline abaxint AbaxDiskHash<Pair>::nextHC( abaxint hc, abaxint arrlen )
{
 	++ hc;
   	if ( hc == arrlen ) hc = 0;
   	return hc;
}

template <class Pair> 
inline bool AbaxDiskHash<Pair>::aboveq( abaxint start, abaxint end, abaxint birthhc, abaxint nullbox )
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
inline void AbaxDiskHash<Pair>::rehashCluster( abaxint hc )
{
		abaxint start, end;
		abaxint nullbox = hc;
		findCluster( hc, &start, &end );
		abaxint  birthhc;
		bool b;
		abaxint idx;

		// set data at hc to NULLVALUE
		raypwrite(fdHash, (char *)_NullKeyValBuf, KEYVALLEN, hc*KEYVALLEN );

		char keybuf[ KEYLEN+1];
		char keyvalbuf[KEYVALLEN+1];
		memset( keyvalbuf, 0, KEYVALLEN+1 );
		memset( keybuf, 0, KEYLEN+1 );
		Pair t_pair;

		while ( 1 ) {
			hc = nextHC( hc, _arrlen );
			raypread(fdHash, (char *)keyvalbuf, KEYVALLEN, hc*KEYVALLEN );
			if ( 0 == strcmp( keyvalbuf, _NullKeyValBuf ) ) { 
				break;
			}

			t_pair = get_pair_key(keybuf, keyvalbuf, KEYLEN );
			birthhc = hashKey( t_pair, _arrlen );
			if ( birthhc == hc ) {
				continue;  // birth hc
			}

			b = aboveq( start, end, birthhc, nullbox );
			if ( b ) {
				raypwrite(fdHash, (char *)_NullKeyValBuf, KEYVALLEN, hc*KEYVALLEN );
				nullbox = hc;
			}
		}
}

template <class Pair>
char* AbaxDiskHash<Pair>::makeKeyValueBuffer( const Pair &pair )
{
	// KEYLEN  VALLEN
	char *mem = (char*) malloc( KEYLEN + VALLEN );
	memset( mem, 0, KEYLEN + VALLEN );
	memcpy( mem, pair.key.addr(), pair.key.addrlen() );
	memcpy( mem+KEYLEN, pair.value.addr(), pair.value.addrlen() );
	return mem;
}


template <class Pair>
abaxint AbaxDiskHash<Pair>::countCells()
{
	int fdHash = _servobj->jdfsMgr->open( _hashname );
	// printf("s4928 countCells fdHash=%d\n", fdHash );
	if ( fdHash < 0 ) {
		//printf("s4920 return 0\n");
		return 0;
	}
	_elements = fgetXattr( fdHash, _hashname.c_str(), "user.elements" );
	//printf("s4921 countCells _elements=%d\n", _elements );
	return fgetXattr( fdHash, _hashname.c_str(), "user.arrlen" );

	/**
	abaxint i = 0;
	int  n;

	_elements = 0;
	char buf[ KEYLEN + VALLEN ];
	i = 0;
	n = 0;
	while ( true ) {
		printf("s4591 raypread n=%d\n", n );
		fflush( stdout );
		n += raypread(fdHash, buf,  KEYLEN + VALLEN, n );
		if ( n <= 0 ) {
			break;
		}

		if ( buf[0] != '\0' ) {
			++ _elements;
		} 
		++i;
	}

	return i;
	**/
}


#endif
