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
class JDFSMgr;
class JagReadWriteLock;
class JagSchemaRecord;
class JagSingleMergeReader;
class JagFileMgr;
class JagRequest;

template <class K, class V> class JagHashMap;

class JagDBPairFile
{
  public:
	int fd;
	AbaxString fpath;

	abaxint memstart;  // writebuf start for each server
	abaxint memoff;    // offset from start for each server
	abaxint mempos;    // end position for each server
	abaxint memlen;    // max offset

	abaxint diskpos;   // pointer position
	abaxint disklen;   // size of file in bytes
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
	
	inline void setdatalen( abaxint len ) { if ( _datalen <= 0 ) _datalen = len; }
	inline abaxint getdatalen() { return _datalen; }
	inline abaxint elements( abaxint onelen=-1 ) { 
		if ( onelen < 0 ) {
			if ( _datalen <= 0 ) return 0;
			return _totalwritelen/_datalen;
		} else if ( onelen == 0 ) return 0;
		return _totalwritelen/onelen; 
	}
	inline abaxint getnumwrites() { return _numwrites; }
	inline abaxint gettotbytes() { return _totalwritelen; }
	inline bool hasSetWrite() { return _isSetWriteDone; }
	inline bool hasFlushWrite() { return _isFlushWriteDone; }

	void clean();
	void setwrite( const JagVector<Jstr> &hostlist );
	void setwrite( abaxint num );
	void setwrite( const Jstr &mapstr, const Jstr &filestr, int keepFile=0 );
	bool writeit( const Jstr &host, const char *buf, abaxint len, 
				  const JagSchemaRecord *rec=NULL, bool noUnlock=false, abaxint membytes=0 );
	bool flushwrite();
	bool readit( JagFixString &buf );
	bool backreadit( JagFixString &buf );
	bool pallreadit( JagFixString &buf );
	void setread( abaxint start, abaxint readcnt );
	int  readRangeOffsetLength( JagFixString &data, abaxint offset, abaxint length );
	char *readBlock( abaxint &len );

	void beginJoinRead( int keylen );
	int joinReadNext( JagVector<JagFixString> &vec );
	void endJoinRead();

	void setMemoryLimit( abaxint maxLimitBytes );
	void shuffleSQLMemAndFlush();

	void sendDataToClient( abaxint cnt, const JagRequest &req );
	//void appendJSData( const Jstr &line );

  protected:
	JagCfg *_cfg;
	JDFSMgr *_jmgr;
	JagFileMgr *_fmgr;
	JagReadWriteLock *_lock;
	
	std::atomic<bool> _useDisk;
	std::atomic<bool> _isSetWriteDone;
	std::atomic<bool> _isFlushWriteDone;
	bool _isserv;
	char *_writebuf;
	char *_readbuf;
	int _numHosts;
	int _numIdx;
	abaxint _maxLimitBytes;
	abaxint _totalwritelen;
	abaxint _numwrites;
	abaxint _datalen;
	int     _keylen;
	int     _vallen;
	int		_keepFile;
	abaxint _readpos;
	abaxint _readlen;
	abaxint _readmaxlen;
	AbaxString _defpath;
	Jstr _dirpath;
	Jstr _dbobj;
	// JagVector<JagDBPairFile> _writeattr;
	JagVector<JagDBPairFile> _dbPairFileVec;
	JagVector<abaxint> _pallreadpos;
	JagVector<abaxint> _pallreadlen;
	JagHashMap<AbaxString, abaxint> *_hostToIdx;
	short _goNext[12800];
	abaxint  _endcnt;
	JagSingleMergeReader  *_mergeReader;

	Jstr *_sqlarr;
	
	abaxint readNextBlock();
	abaxint backreadNextBlock();
	abaxint pallreadNextBlock();
	abaxint getUsableMemory();
	int   _getNextDataOfHostFromMem( int i  );
	int   _joinReadFromMem( JagVector<JagFixString> &vec );
	int   _joinReadFromDisk( JagVector<JagFixString> &vec );



};

#endif
