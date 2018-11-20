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
#include <JagGlobalDef.h>

#include <stdio.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <fcntl.h>

#include <JagLocalDiskHash.h>
#include <JagSingleBuffReader.h>
#include <JagSingleBuffWriter.h>
#include <JagUtil.h>
		
// ctor
JagLocalDiskHash::JagLocalDiskHash( const AbaxDataString &filepath, int keylength, int vallength, int arrlength )
{
	KEYLEN = keylength;
	VALLEN = vallength;
	KVLEN = KEYLEN+VALLEN;
	_NullKeyValBuf = (char*)jagmalloc(KVLEN+1);
	_schemaRecord = NULL;
	init( filepath, arrlength );

	// setConcurrent( true );
}

/**
// ctor with schema record
JagLocalDiskHash::JagLocalDiskHash( const AbaxDataString &filepath, JagSchemaRecord *record, int arrlength )
{
	KEYLEN = record->keyLength;
	VALLEN = record->valueLength;
	KVLEN = KEYLEN+VALLEN;
	_NullKeyValBuf = (char*)jagmalloc(KVLEN+1);
	_schemaRecord = record;
	init( filepath, arrlength );
}
***/


void JagLocalDiskHash::drop( )
{
	jagunlink(_hashname.c_str());
}

// ctor
void JagLocalDiskHash::init( const AbaxDataString &filePathPrefix, int length )
{
	// _lock = NULL; 

	memset(_NullKeyValBuf, 0, KVLEN+1);
	JagDBPair t_JagDBPair = JagDBPair::NULLVALUE;
	
	_hashname = filePathPrefix + ".hdb"; 
	_newhashname = filePathPrefix + "_tmpajskeurhd.hdb"; 

	_fdHash = -1;
	int exist = 0;
	if ( 0 == jagaccess( _hashname.c_str(), R_OK|W_OK  ) ) { exist = 1; }
	_fdHash = jagopen((char *)_hashname.c_str(), O_CREAT|O_RDWR, S_IRWXU);
	if ( ! exist  ) {
		_arrlen = length;
		size_t bytesHash = _arrlen*KVLEN;
   		jagftruncate( _fdHash, bytesHash);
		_elements = 0;
	} else {
		_arrlen = countCells();
	}
}

void  JagLocalDiskHash::setConcurrent( bool flag )
{
	/***
	if ( flag ) {
		if ( ! _lock ) {
			_lock = new RayReadWriteLock();
			printf("JagLocalDiskHash _lock=%0x _hashname=[%s]\n", _lock, _hashname.c_str() );
		}
	} else {
		if ( _lock ) {
			delete  _lock;
			_lock = NULL;
			printf("JagLocalDiskHash _lock=%0x _hashname=[%s]\n", _lock, _hashname.c_str() );
		}
	}
	***/
}

// dtor
void JagLocalDiskHash::destroy( )
{
	if ( _fdHash > 0 ) {
		jagclose(_fdHash);
		_fdHash = -1;
	}

	/**
	if ( _lock ) {
		delete _lock;
		_lock = NULL;
	}
	**/
}

// dtor
JagLocalDiskHash::~JagLocalDiskHash( )
{
	destroy();
	free( _NullKeyValBuf );
}

void JagLocalDiskHash::reAllocDistribute()
{
	abaxint i;
	size_t bytesHash2;

	// AbaxLock lckhash(fdHash, 0, _arrlen*KVLEN);
	// lckhash.readLock();
	// RayReadWriteMutex mutex( _lock, RayReadWriteMutex::WRITE_LOCK );

	char *kvbuf = (char*)jagmalloc(KVLEN+1);
	memset( kvbuf, 0, KVLEN+1 );

	_newarrlen = _GEO*_arrlen;
	bytesHash2 = _newarrlen*KVLEN;

	_fdHash2 = jagopen((char *)_newhashname.c_str(), O_CREAT|O_RDWR, S_IRWXU );
	// AbaxLock lckhash2(fdHash2, 0, _arrlen*KVLEN);
	// lckhash2.writeLock();

	jagftruncate(_fdHash2, bytesHash2 );

	// pick from old and insert into new
	JagSingleBuffReader navig( _fdHash, _arrlen, KEYLEN, VALLEN );
	while ( navig.getNext( kvbuf, KVLEN, i ) ) {
		JagDBPair pair( kvbuf, KEYLEN, kvbuf+KEYLEN, VALLEN );
		int rc2= _insertHash( pair, 0 );
	    // printf("s2266 i=%d kvbuf=[%s] rc2=%d\n", i, kvbuf, rc2 );
		/**
		if ( 0 == rc2 ) {
			printnew();
			print();
		}
		**/
	}

	// lckhash.unLock();
	// lckhash2.unLock();

	jagclose(_fdHash);
	jagclose(_fdHash2);

	free( kvbuf );

	jagunlink(_hashname.c_str());
	
	jagrename(_newhashname.c_str(), _hashname.c_str());
	_fdHash = jagopen((char *)_hashname.c_str(), O_CREAT|O_RDWR, S_IRWXU );

	_arrlen = _newarrlen;

}


void JagLocalDiskHash::reAllocShrink()
{
	abaxint i;
	size_t bytesHash2; 
	
	// AbaxLock lckhash(fdHash, 0, _arrlen*KVLEN);
	// lckhash.readLock();

	// RayReadWriteMutex mutex( _lock, RayReadWriteMutex::WRITE_LOCK );

	char *kvbuf = (char*)jagmalloc(KVLEN+1);
	memset( kvbuf, 0, KVLEN+1 );

	_newarrlen  = _arrlen/_GEO;
	bytesHash2 = _newarrlen*KVLEN;

	_fdHash2 = jagopen((char *)_newhashname.c_str(), O_CREAT|O_RDWR, S_IRWXU);
	jagftruncate( _fdHash2, bytesHash2 );

	JagSingleBuffReader navig( _fdHash, _arrlen, KEYLEN, VALLEN );
	while ( navig.getNext( kvbuf, KVLEN, i ) ) {
		JagDBPair pair( kvbuf, KEYLEN, kvbuf+KEYLEN, VALLEN );
		// insertHash( pair, _fdHash2, _newarrlen);
		_insertHash( pair, 0 );
	}

	// lckhash.unLock();
	// lckhash2.unLock();

	jagclose(_fdHash);
	jagclose(_fdHash2);

	free( kvbuf );

	jagunlink(_hashname.c_str());
	
	jagrename(_newhashname.c_str(), _hashname.c_str());
	_fdHash = jagopen((char *)_hashname.c_str(), O_CREAT|O_RDWR, S_IRWXU );

	_arrlen = _newarrlen;

}


/**
*   Remove a JagDBPair from the array
*   If it does not exists, return; else set it to NULL,
**/
bool JagLocalDiskHash::remove( const JagDBPair &jagDBPair )
{
	// RayReadWriteMutex mutex( _lock, RayReadWriteMutex::WRITE_LOCK );

	abaxint  hloc;
	bool rc = _exist( 1, jagDBPair, &hloc );
	if ( ! rc ) {
		return false;
	}
	// printf("s3349 in remove hloc=%d\n", hloc );

	/*** bug: causes find cluster fail
	char *kvbuf = (char*)jagmalloc(KVLEN+1);
	memset( kvbuf, 0, KVLEN+1 );
	raysafepwrite( _fdHash, kvbuf, KVLEN, hloc*KVLEN );
	free( kvbuf );
	***/

	-- _elements;
	// printf("s3549 before rehashCluster hloc=%d ...\n", hloc );
	//print();
	rehashCluster( hloc );
	//printf("s3549 after rehashCluster hloc=%d ...\n", hloc );
	//print();

	if ( _arrlen >= 64 ) {
    	abaxint loadfactor  = 100 * (abaxint)_elements / _arrlen;
    	if (  loadfactor < 15 ) {
			//printf("s6644 loadfactor=%d _elements=%d _arrlen=%d reAllocShrink\n", loadfactor, _elements, _arrlen );
			//print();
    		reAllocShrink();
			// print();
    	}
	} 

	return true;
}


// current: 1 use _fdHash and _arrlen    0: use _fdHash2 and _newarrlen
bool JagLocalDiskHash::_exist( int current,  const JagDBPair &search, abaxint *hloc )
{
	//AbaxLock lckhash(fdHash, 0, _arrlen*KVLEN);
	//lckhash.readLock();

	// RayReadWriteMutex mutex( _lock, RayReadWriteMutex::READ_LOCK );
	int fdHash;
	abaxint arrlen;
	if ( current ) {
		// printf("s7734 use _fdHash ...\n");
		fdHash = _fdHash;
		arrlen = _arrlen;
	} else {
		// printf("s7735 use _fdHash2=%lld (_fdHash=%lld) ...\n", _fdHash2, _fdHash );
		fdHash = _fdHash2;
		arrlen = _newarrlen;
	}

	char *kvbuf = (char*)jagmalloc(KVLEN+1);
	memset( kvbuf, 0, KVLEN+1 );
	/**
	printf("s7073 JagLocalDiskHash exist _hashname=%s  READ_LOCK ...\n", _hashname.c_str() );
	RayReadWriteMutex mutex( _lock, RayReadWriteMutex::READ_LOCK );
	printf("s7073 JagLocalDiskHash exist _hashname=%s  READ_LOCK obtained \n", _hashname.c_str() );
	**/

	if ( arrlen < 1 ) {
		printf("JagLocalDiskHash::exist() arrlen<1  this=%0x\n", this );
		free( kvbuf );
		return 0;
	}

    abaxint hc = hashKey( search, arrlen );
	// printf("s8132 exist() hashKey of (%s) is hc=%d\n", search.key.c_str(), hc );
	// raysafepread( _fdHash, (char *)kvbuf, KVLEN,  hc*KVLEN );
	ssize_t n = raysafepread( fdHash, (char *)kvbuf, KVLEN,  hc*KVLEN );
	if ( n <= 0 ) {  free( kvbuf ); return 0; }

    //  hash to NULL, not found.
    if ( '\0' == *kvbuf ) {
		// printf("s2903 kvbuf=[%s] hc=%lld arrlen=%lld firstbyte NULL return 0\n", kvbuf, hc, arrlen );
		free( kvbuf );
    	return 0;
    }
           
	JagDBPair t_JagDBPair(kvbuf, KEYLEN );
    if ( search != t_JagDBPair  ) {
       	hc = findProbedLocation( fdHash, arrlen, search, hc );
       	if ( hc < 0 ) {
			free( kvbuf );
			// printf("s2303 findprobedlocation hc=%d return 0\n", hc );
       		return 0;
       	}
    }
        
    *hloc = hc;
	free( kvbuf );
    return 1;
}


bool JagLocalDiskHash::get( JagDBPair &jagDBPair )
{
	abaxint hloc;
	bool rc;

	// RayReadWriteMutex mutex( _lock, RayReadWriteMutex::READ_LOCK );

	rc = _exist( 1, jagDBPair, &hloc );
	if ( ! rc ) {
		return false;
	}

	char *kvbuf = (char*)jagmalloc(KVLEN+1);
	memset( kvbuf, 0, KVLEN+1 );
	ssize_t n = raysafepread( _fdHash, kvbuf, KVLEN, hloc*KVLEN );
	if ( n <= 0 ) return 0;
	jagDBPair = JagDBPair( kvbuf, KEYLEN, kvbuf+KEYLEN, VALLEN);
	free( kvbuf );
	return true;
}

// if jagDBPair does not exist, return false; else update the new pair
bool JagLocalDiskHash::set( const JagDBPair &jagDBPair )
{
	abaxint hloc;
	bool rc;

	// RayReadWriteMutex mutex( _lock, RayReadWriteMutex::WRITE_LOCK );
	if ( jagDBPair.value.size() < 1 ) { return false; }

	rc = _exist( 1, jagDBPair, &hloc );
	if ( ! rc ) return false;

	char *valbuf = (char*)jagmalloc(VALLEN+1);
	memset( valbuf, 0, VALLEN+1 );
	const char *pval = (const char*)jagDBPair.value.addr();
	memcpy( valbuf, pval, VALLEN );
	raysafepwrite( _fdHash, valbuf, VALLEN, (hloc*KVLEN)+KEYLEN );
	free( valbuf );
	return true;
}

// if jagDBPair does not exist, insert ; else update the new pair
bool JagLocalDiskHash::setforce( const JagDBPair &jagDBPair )
{
	if ( jagDBPair.value.size() < 1 ) { return false; }

	// RayReadWriteMutex mutex( _lock, RayReadWriteMutex::WRITE_LOCK );
	abaxint hloc;
	bool rc;
	rc = _exist( 1, jagDBPair, &hloc );
	if ( ! rc ) {
		rc = _insertAt( _fdHash, jagDBPair, hloc );
		return rc;
	}

	char *valbuf = (char*)jagmalloc(VALLEN+1);
	memset( valbuf, 0, VALLEN+1 );
	const char *ptr = jagDBPair.value.addr();
	memcpy( valbuf, ptr, VALLEN );
	raysafepwrite( _fdHash, valbuf, VALLEN, ( hloc*KVLEN)+KEYLEN );
	free( valbuf );

	return true;
}

// bool JagLocalDiskHash::insertHash( const JagDBPair &jagDBPair, const int fd_Hash, abaxint arrlen )
// current 1: use _fdHash _arrlen  0: use _fdhash2 _newarrlen
bool JagLocalDiskHash::_insertHash( const JagDBPair &jagDBPair, int current )
{
	const char *p = jagDBPair.key.c_str();
	if ( *p == '\0' ) {
		// prt(("s3337 return 0\n"));
		return 0;
	}

	abaxint hloc;
	bool rc = _exist( current, jagDBPair, &hloc );
	if (  rc ) {
		// prt(("s3437 current=%d key=[%s] exists return false\n", current, jagDBPair.key.addr()  ));
		return false;
	}

	abaxint arrlen = _arrlen;
	int  fdHash = _fdHash;
	if ( ! current ) {
		arrlen = _newarrlen;
		fdHash = _fdHash2;
	}

	// RayReadWriteMutex mutex( _lock, RayReadWriteMutex::WRITE_LOCK );

	char *kvbuf = (char*)jagmalloc(KVLEN+1);
	memset( kvbuf, 0, KVLEN+1 );
	abaxint hc = hashKey( jagDBPair, arrlen );
	// printf("s8122 insertHash() hashKey of (%s) is %d\n", jagDBPair.key.c_str() );
	ssize_t n = raysafepread( fdHash, (char *)kvbuf, KVLEN, hc*KVLEN );
	if ( n <= 0 ) { free( kvbuf ); return 0; }

	// if ( 0 != strcmp( kvbuf, _NullKeyValBuf) ) {
	if ( 0 != *kvbuf ) {
		hc = probeLocation( hc, fdHash, arrlen );
	}
	free( kvbuf );

	char *newkvbuf = makeKeyValueBuffer( jagDBPair );
	raysafepwrite(fdHash, (char*)newkvbuf, KVLEN, hc*KVLEN );
	free( newkvbuf );

	if ( ! current ) {
		return 1;
	}

	++ _elements;

	// if ( ( 10*_GEO*_elements ) >=  8*_arrlen ) {
	if ( ( 10*_GEO*_elements ) >=  7*_arrlen ) {
		reAllocDistribute();
	}

	return 1;
}

abaxint JagLocalDiskHash::probeLocation( abaxint hc, const int fd_Hash, abaxint arrlen )
{
	int cnt = 0;
	char *kvbuf = (char*)jagmalloc(KVLEN+1);
	memset( kvbuf, 0, KVLEN+1 );
	ssize_t n;
    while ( 1 ) {
    	hc = nextHC( hc, arrlen );
		raysafepread(fd_Hash, (char *)kvbuf, KVLEN, hc*KVLEN );
		if ( n <= 0 ) { free( kvbuf ); return -1; }
		if ( 0 == *kvbuf ) {
			free( kvbuf );
			return hc;
		}

		++cnt;
		if ( cnt > 100000 ) {
			printf("e5492 error probe exit\n");
			exit(1);
		}
   	}
	free( kvbuf );
   	return -1;
}
    
// retrieve the previously assigned probed location
// Input: fdHash
abaxint JagLocalDiskHash::findProbedLocation( int fdHash, abaxint arrlen, const JagDBPair &search, abaxint hc ) 
{
	char *kvbuf = (char*)jagmalloc(KVLEN+1);
	memset( kvbuf, 0, KVLEN+1 );
	ssize_t n;

   	while ( 1 ) {
   		// hc = nextHC( hc, _arrlen );
   		hc = nextHC( hc, arrlen );
		n = raysafepread( fdHash, (char *)kvbuf, KVLEN, hc*KVLEN ); 		
		if ( n <= 0 ) { free( kvbuf ); return -1; }

   		if ( *kvbuf == '\0' ) {
			free( kvbuf );
			// prt(("s3949 no probed slot found  return -1\n"));
   			return -1; // no probed slot found
   		}
    
   		if ( 0 == strncmp( kvbuf, search.key.addr(), KEYLEN ) ) { 
			free( kvbuf );
   			return hc;
   		}
   	}

	free( kvbuf );
   	return -1;
}
    
    
// _hashcol[pos] must not be equal to ABAX_HASHARRNULL
// find [start, end] of island that contains hc  
// start and end [0, _hashcol*DEPTH-1].  start can be greater than end
// [3, 9]   [12, 3]
void JagLocalDiskHash::findCluster( abaxint hc, abaxint *start, abaxint *end )
{
   	abaxint i;
   	i = hc;

	char *kvbuf = (char*)jagmalloc(KVLEN+1);
	memset( kvbuf, 0, KVLEN+1 );
	ssize_t n;
   
  	// find start
   	while ( 1 ) {
   		i = prevHC( i, _arrlen );
		n = raysafepread( _fdHash, (char *)kvbuf, KVLEN, i*KVLEN );
		if ( n <= 0 ) {  *start = -1; free(kvbuf); return;} 
 		if ( 0 == *kvbuf ) {
    		*start = nextHC( i, _arrlen );
    		break;
    	}
    }
    
    // find end
    i = hc;
    while ( 1 ) {
    	i = nextHC( i, _arrlen );
		n = raysafepread( _fdHash, (char *)kvbuf, KVLEN, i*KVLEN );
		if ( n <= 0 ) {  *start = -1; free(kvbuf); return;} 
 		if ( 0 == *kvbuf ) {
    		*end = prevHC( i, _arrlen );
    		break;
    	}

   	}

	free( kvbuf );
}
    
abaxint JagLocalDiskHash::prevHC ( abaxint hc, abaxint arrlen )
{
   	--hc; 
   	if ( hc < 0 ) hc = arrlen-1;
   	return hc;
}
    
abaxint JagLocalDiskHash::nextHC( abaxint hc, abaxint arrlen )
{
 	++ hc;
   	if ( hc == arrlen ) hc = 0;
   	return hc;
}

 bool JagLocalDiskHash::aboveq( abaxint start, abaxint end, abaxint birthhc, abaxint nullbox )
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

// consolidate cluster
void JagLocalDiskHash::rehashCluster( abaxint hc )
{
		abaxint start, end;
		abaxint nullbox = hc;

		findCluster( hc, &start, &end );
		if ( start < 0 ) return;

		abaxint  birthhc;
		bool b;
		ssize_t n;

		// set data at hc to NULLVALUE
		raysafepwrite( _fdHash, (char *)_NullKeyValBuf, KVLEN, hc*KVLEN );

		char *kvbuf = (char*)jagmalloc(KVLEN+1);
		memset( kvbuf, 0, KVLEN+1 );
		while ( 1 ) {
			hc = nextHC( hc, _arrlen );
			n = raysafepread( _fdHash, (char *)kvbuf, KVLEN, hc*KVLEN );
			if ( n <= 0 ) break;
			if ( *kvbuf == '\0' ) { break; }
			JagDBPair t_JagDBPair(kvbuf, KEYLEN );
			birthhc = hashKey( t_JagDBPair, _arrlen );
			if ( birthhc == hc ) {
				continue;  // birth hc
			}

			b = aboveq( start, end, birthhc, nullbox );
			if ( b ) {
				// nullbox <-- hc data
				raysafepwrite( _fdHash, (char *)kvbuf, KVLEN, nullbox*KVLEN );

				raysafepwrite( _fdHash, (char *)_NullKeyValBuf, KVLEN, hc*KVLEN );
				// printf("s7723 nullify hc=%d\n", hc );
				nullbox = hc;
			} else {
				// printf("s7763 aboveq false start=%d end=%d birthhc=%d nullbox=%d\n", start, end, birthhc, nullbox);
			}
		}

		free( kvbuf );
}


char* JagLocalDiskHash::makeKeyValueBuffer( const JagDBPair &jagDBPair )
{
	char *mem = (char*) jagmalloc( KVLEN );
	memset( mem, 0, KVLEN );
	memcpy( mem, jagDBPair.key.addr(), jagDBPair.key.size() );
	memcpy( mem+KEYLEN, jagDBPair.value.addr(), jagDBPair.value.size() );
	return mem;
}


abaxint JagLocalDiskHash::countCells()
{
	struct stat     statbuf;
	if (  fstat( _fdHash, &statbuf) < 0 ) {
		return 0;
	}

	_arrlen = statbuf.st_size/KVLEN;
	_elements = 0;
   	char *kvbuf = (char*)jagmalloc( KVLEN+1 );
	memset( kvbuf, 0,  KVLEN + 1 );
	// abaxint rlimit = getBuffReaderWriterMemorySize( _arrlen*KVLEN/1024/1024 );
	JagSingleBuffReader navig( _fdHash, _arrlen, KEYLEN, VALLEN );
	while ( navig.getNext( kvbuf ) ) {
		++_elements;
	}
	free( kvbuf );
	jagmalloc_trim( 0 );

	return _arrlen;
}

void JagLocalDiskHash::print()
{
	abaxint i;
	char *kvbuf = (char*)jagmalloc(KVLEN+1);
	memset( kvbuf, 0, KVLEN+1 );

	JagSingleBuffReader navig( _fdHash, _arrlen, KEYLEN, VALLEN );
	int num = 0;
	printf("_fdHash:\n");
	while ( navig.getNext( kvbuf, KVLEN, i ) ) {
		JagDBPair pair( kvbuf, KEYLEN, kvbuf+KEYLEN, VALLEN );
		printf("%03d %03d [%s] --> [%s]\n", num, i, pair.key.addr(), pair.value.addr() );
		++num;
	}

	free( kvbuf );
}

void JagLocalDiskHash::printnew()
{
	abaxint i;
	char *kvbuf = (char*)jagmalloc(KVLEN+1);
	memset( kvbuf, 0, KVLEN+1 );

	JagSingleBuffReader navig( _fdHash2, _newarrlen, KEYLEN, VALLEN );
	int num = 0;
	printf("_fdHash2:\n");
	while ( navig.getNext( kvbuf, KVLEN, i ) ) {
		JagDBPair pair( kvbuf, KEYLEN, kvbuf+KEYLEN, VALLEN );
		printf("%03d %03d [%s] --> [%s]\n", num, i, pair.key.addr(), pair.value.addr() );
		++num;
	}

	free( kvbuf );
}

bool JagLocalDiskHash::_insertAt( int fdHash, const JagDBPair &jagDBPair, abaxint hloc )
{
	char *newkvbuf = makeKeyValueBuffer( jagDBPair );
	raysafepwrite(fdHash, (char*)newkvbuf, KVLEN, hloc*KVLEN );
	free( newkvbuf );
	return 1;
}

