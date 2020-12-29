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
#ifndef _jag_data_aggregate_h_
#define _jag_data_aggregate_h_
#include <atomic>

#include <abax.h>
#include <JagDef.h>
#include <JagVector.h>

class JagCfg;
class JagReadWriteLock;
class JagSchemaRecord;
class JagSingleMergeReader;
class JagFileMgr;
class JagRequest;
class JagFSMgr;

template <class K, class V> class JagHashMap;

class JagDBPairFile
{
  public:
	int fd;
	AbaxString  fpath;

	jagint memstart;  // writebuf start for each server
	jagint memoff;    // offset from start for each server
	jagint mempos;    // end position for each server
	jagint memlen;    // max offset

	jagint diskpos;   // pointer position
	jagint disklen;   // size of file in bytes
	JagFixString kv;  // last-read data (DBPair, kv fix string)
	
	JagDBPairFile() {
		fd = -1;
		memstart = memoff = mempos = memlen = diskpos = disklen = 0;
	}
};

class JagDataAggregate
{
  public:
	JagDataAggregate( bool isserv=true );
	~JagDataAggregate();
	
	inline void setdatalen( jagint len ) { if ( _datalen <= 0 ) _datalen = len; }
	inline jagint getdatalen() { return _datalen; }
	inline jagint elements( jagint onelen=-1 ) { 
		if ( onelen < 0 ) {
			if ( _datalen <= 0 ) return 0;
			return _totalwritelen/_datalen;
		} else if ( onelen == 0 ) return 0;
		return _totalwritelen/onelen; 
	}
	inline jagint getnumwrites() { return _numwrites; }
	inline jagint gettotbytes() { return _totalwritelen; }
	inline bool hasSetWrite() { return _isSetWriteDone; }
	inline bool hasFlushWrite() { return _isFlushWriteDone; }

	void clean();
	void setwrite( const JagVector<Jstr> &hostlist );
	void setwrite( jagint num );
	void setwrite( const Jstr &mapstr, const Jstr &filestr, int keepFile=0 );
	bool writeit( int hosti, const char *buf, jagint len, 
				  const JagSchemaRecord *rec=NULL, bool noUnlock=false, jagint membytes=0 );
	bool flushwrite();
	bool readit( JagFixString &buf );
	bool backreadit( JagFixString &buf );
	void setread( jagint start, jagint readcnt );
	int  readRangeOffsetLength( JagFixString &data, jagint offset, jagint length );
	char *readBlock( jagint &len );

	void beginJoinRead( int keylen );
	int joinReadNext( JagVector<JagFixString> &vec );
	void endJoinRead();

	void setMemoryLimit( jagint maxLimitBytes );
	void shuffleSQLMemAndFlush();

	void sendDataToClient( jagint cnt, const JagRequest &req );
	void initWriteBuf();
	void cleanWriteBuf();

  protected:
	JagCfg *_cfg;
	JagFSMgr *_jfsMgr;
	pthread_rwlock_t *_lock;
	
	std::atomic<bool> _useDisk;
	std::atomic<bool> _isSetWriteDone;
	std::atomic<bool> _isFlushWriteDone;
	bool 	_isServ;
	//char 	*_writebuf;
	char 	*_writebuf[JAG_MAX_HOSTS];
	bool    _writebufHasData;
	char 	*_readbuf;
	int 	_numHosts;
	int 	_numIdx;
	jagint 	_maxLimitBytes;
	jagint 	_totalwritelen;
	jagint 	_numwrites;
	jagint 	_datalen;
	int     _keylen;
	int     _vallen;
	int		_keepFile;
	jagint 	_readpos;
	jagint 	_readlen;
	jagint 	_readmaxlen;
	AbaxString _defpath;
	Jstr 	_dirpath;
	Jstr 	_dbobj;
	JagVector<JagDBPairFile> _dbPairFileVec;
	JagVector<jagint> 		_pallreadpos;
	JagVector<jagint> 		_pallreadlen;
	JagHashMap<AbaxString, jagint> *_hostToIdx;
	short 		_goNext[12800];
	jagint  	_endcnt;
	JagSingleMergeReader  *_mergeReader;

	Jstr *_sqlarr;
	
	jagint readNextBlock();
	jagint backreadNextBlock();
	//jagint pallreadNextBlock();
	jagint getUsableMemory();
	int   _getNextDataOfHostFromMem( int i  );
	int   _joinReadFromMem( JagVector<JagFixString> &vec );
	int   _joinReadFromDisk( JagVector<JagFixString> &vec );


};

#endif
