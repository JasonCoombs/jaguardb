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

#include <JagDataAggregate.h>
#include <JagHashMap.h>
#include <JagMutex.h>
#include <JagFSMgr.h>
#include <JagUtil.h>
#include <JagCfg.h>
#include <JagSingleMergeReader.h>
#include <JagSchemaRecord.h>
#include <JagFileMgr.h>
#include <JagTable.h>
#include <JagRequest.h>

JagDataAggregate::JagDataAggregate( bool isserv ) 
{
	_useDisk = 0;
	_keepFile = 0;
	_isSetWriteDone = 0;
	_isFlushWriteDone = 0;
	_numHosts = 0;
	_numIdx = 0;
	_datalen = 0;
	_readpos = 0;
	_readlen = 0;
	_readmaxlen = 0;
	_maxLimitBytes = 0;
	_totalwritelen = 0;
	_numwrites = 0;
	initWriteBuf();
	_readbuf = NULL;
	_sqlarr = NULL;
	_hostToIdx = new JagHashMap<AbaxString, jagint>();

	if ( isserv ) {
		_lock = NULL; 
	} else {
		_lock = newJagReadWriteLock();
	}

	_jfsMgr = new JagFSMgr();
	if ( isserv ) {
		_cfg = new JagCfg( JAG_SERVER );
	} else {
		_cfg = new JagCfg( JAG_CLIENT );
	}
	_isServ = isserv;
	_defpath =  jaguarHome() + "/tmp/";
	_mergeReader = NULL;
}

JagDataAggregate::~JagDataAggregate()
{
	clean();
	if ( _hostToIdx ) {
		delete _hostToIdx;
		_hostToIdx = NULL;
	}

	if ( _lock ) {
		deleteJagReadWriteLock( _lock );
	}

	if ( _jfsMgr ) {
		delete _jfsMgr;
		_jfsMgr = NULL;
	}

	if ( _cfg ) {
		delete _cfg;
		_cfg = NULL;
	}
}

void JagDataAggregate::clean()
{
	cleanWriteBuf();
	
	if ( _readbuf ) {
		free( _readbuf );
		_readbuf = NULL;
	}

	if ( _sqlarr ) {
		delete [] _sqlarr;
		_sqlarr = NULL;
	}
	
	for ( int i = 0; i < _dbPairFileVec.size(); ++i ) {
		_jfsMgr->closefd( _dbPairFileVec[i].fpath );
		if ( _keepFile != 1 && _keepFile != 3 ) {
			_jfsMgr->remove( _dbPairFileVec[i].fpath );
		}
	}

	_dbPairFileVec.clean();
	_pallreadpos.clean();
	_pallreadlen.clean();
	_hostToIdx->removeAllKey();
	
	_dbobj = "";
	_useDisk = 0;
	_keepFile = 0;
	_isSetWriteDone = _isFlushWriteDone = 0;
	_numHosts = _numIdx = _datalen = _readpos = _readlen = _readmaxlen = _totalwritelen = _numwrites = 0;
	_maxLimitBytes = 0;

	if ( _mergeReader ) {
		delete _mergeReader;
		_mergeReader = NULL;
	}

}

// method to open files for write and init part of data members of _dbPairFileVec
// followed by writeit, done by main thread
// get ready for write, initialization
void JagDataAggregate::setwrite( const JagVector<Jstr> &hostlist )
{
	prt(("s765521 setwrite JagVector<Jstr> &hostlist\n"));
	clean();
	JagDBPairFile dbpfile;
	_numHosts = hostlist.size();
	for ( jagint i = 0; i < _numHosts; ++i ) {
		dbpfile.fpath = _defpath + longToStr( THREADID ) + "_" + hostlist[i];
		prt(("a111028228 setwrite openfd( %s ) ...\n", dbpfile.fpath.s() ));
		dbpfile.fd = _jfsMgr->openfd( dbpfile.fpath, true );
		if ( dbpfile.fd < 0 ) {
			prt(("s302831 dbpfile.fd=%d error\n", dbpfile.fd ));
		}

		_dbPairFileVec.append( dbpfile );
		_pallreadpos.append( 0 );
		_pallreadlen.append( 0 );
		_hostToIdx->addKeyValue( hostlist[i], i );
	}

	_isSetWriteDone = 1;	

}

// another method to set write files, if num of files given instead of vector
void JagDataAggregate::setwrite( jagint numHosts )
{
	clean();
	JagDBPairFile dbpfile;
	_numHosts = numHosts;
	for ( jagint i = 0; i < _numHosts; ++i ) {
		dbpfile.fpath = _defpath + longToStr( THREADID ) + "_" + longToStr( i );
		_dbPairFileVec.append( dbpfile );
		_pallreadpos.append( 0 );
		_pallreadlen.append( 0 );
		_hostToIdx->addKeyValue( longToStr( i ), i );
	}
	_isSetWriteDone = 1;
}

// the third method to set write file ( one file only ); if the file path has some special meaning
// if flag is true, use filestr + .dbp for file name, and use /jaguar/export/ for path, also keep this file;
void JagDataAggregate::setwrite( const Jstr &mapstr, const Jstr &filestr, int keepFile )
{
	//prt(("s222727 in setwrite mapstr=[%s] filestr=[%s] keepFile=%d\n", mapstr.s(), filestr.s(), keepFile ));

	clean();
	JagDBPairFile dbpfile;
	_numHosts = 1;
	if ( keepFile == 1 ) {
		// for export file keep
		_dirpath = jaguarHome() + "/export/" + filestr + "/";
		dbpfile.fpath = _dirpath + filestr + ".sql";
		_keepFile = 1;
		_dbobj = filestr;
		JagFileMgr::rmdir( _dirpath );
		JagFileMgr::makedirPath( _dirpath, 0755 );
	} else if ( keepFile == 2 || keepFile == 3 ) {
		// for given path file keep
		dbpfile.fpath = filestr;
		_keepFile = keepFile;
	} else {
		dbpfile.fpath = _defpath + longToStr( THREADID ) + "_" + filestr;
	}

	_dbPairFileVec.append( dbpfile );
	_pallreadpos.append( 0 );
	_pallreadlen.append( 0 );
	_hostToIdx->addKeyValue( AbaxString(mapstr), 0 );  // hosti is 0
	_isSetWriteDone = 1;

}

void JagDataAggregate::setMemoryLimit( jagint maxLimitBytes )
{
	_maxLimitBytes = maxLimitBytes;
}

// from server get dbpair, write to writebuf, then flush to disk
// if keep file, write each line of data to sql file for export, format to insert cmd mode instead of dbpair mode
// need 4th argument for parse
bool JagDataAggregate::writeit( int hosti, const char *buf, jagint len, 
								const JagSchemaRecord *rec, bool noUnlock, jagint membytes )
{
	prt(("s202930 writeit hosti=[%d] buf=[%s] len=%d noUnlock=%d membytes=%d _keepFile=%d membytes=%d\n", 
			 hosti, buf, len, noUnlock, membytes, _keepFile, membytes ));

	if ( !_isSetWriteDone ) {
		prt(("a727272 return 0\n"));
		return 0;
	}

	if ( !_writebuf[hosti] && _keepFile != 1 ) {
		//prt(("s227721 here init _writebuf[%d] \n", hosti ));
		if ( _datalen <= 0 ) _datalen = len;
		jagint maxbytes = 10*1024;
		if ( membytes > 0 && membytes < maxbytes ) maxbytes = membytes;
		if ( _datalen < 1 ) {
			prt(("a7611717 return 0\n"));
			return 0;
		}

		maxbytes /= _datalen;
		if ( 0 == maxbytes ) ++maxbytes;
		maxbytes *= _datalen;  // int blocks for numhosts
		jagint onemaxbytes = maxbytes;

		_writebuf[hosti] = (char*)jagmalloc( maxbytes );
		memset( _writebuf[hosti], 0, maxbytes );
		_writebufHasData = true;
		
		_dbPairFileVec[hosti].memstart = _dbPairFileVec[hosti].memoff = _dbPairFileVec[hosti].mempos = 0;
		_dbPairFileVec[hosti].memlen = onemaxbytes;
	}
	
	jagint clen, wlen;

	// export to sql commands
	if ( _keepFile == 1 ) {
		cleanWriteBuf();

		if ( !_sqlarr ) {
			if ( _datalen <= 0 ) _datalen = len;
			jagint maxlens = jagatoll(_cfg->getValue("SHUFFLE_MEM_SIZE_MB", "256").c_str())*1024*1024/_datalen;
			_sqlarr = new Jstr[maxlens];
			_dbPairFileVec[0].memlen = maxlens-1;
			_dbPairFileVec[0].mempos = 0;
			_dbPairFileVec[0].memstart = 0;
			_dbPairFileVec[0].memoff = 0;
		}

		if ( !rec ) {
			prt(("a8282727 return 0\n"));
			return 0;
		}

		jagint offset, length;
		Jstr type;
		Jstr outstr, cmd = Jstr("insert into ") + _dbobj + "(";
		bool isLast = 0;
		for ( int i = 0; i < rec->columnVector->size(); ++i ) {
			if ( i == rec->columnVector->size()-2 ) {
				if ( (*(rec->columnVector))[i+1].name == "spare_" ) { isLast = 1; }
			} else if ( i==rec->columnVector->size()-1 ) { 
				isLast = 1; 
			}

			if ( ! isLast ) {
				cmd += Jstr(" ") + (*(rec->columnVector))[i].name.c_str() + ",";
			} else {
				cmd += Jstr(" ") + (*(rec->columnVector))[i].name.c_str() + " ) values (";
			}
			if ( isLast ) break;
		}

		isLast = 0;
		for ( int i = 0; i < rec->columnVector->size(); ++i ) {
			if ( i == rec->columnVector->size()-2 ) {
				if ( (*(rec->columnVector))[i+1].name == "spare_" ) { isLast = 1; }
			} else if ( i==rec->columnVector->size()-1 ) { 
				isLast = 1; 
			}
			
			offset = (*(rec->columnVector))[i].offset;
			length = (*(rec->columnVector))[i].length;
			type = (*(rec->columnVector))[i].type;

			outstr = formOneColumnNaturalData( buf, offset, length, type );
			prt(("s222093 formOneColumnNaturalData outstr=[%s]\n", outstr.s() ));
			// include /export

			if ( ! isLast ) {
				cmd += Jstr(" '") + outstr + "',";
			} else {
				cmd += Jstr(" '") + outstr + "' );\n";
			}
			if ( isLast ) break;
		}

		if ( _dbPairFileVec[0].mempos > _dbPairFileVec[0].memlen ) {
			shuffleSQLMemAndFlush();
			_dbPairFileVec[0].mempos = 0;
		}
		_sqlarr[_dbPairFileVec[0].mempos++] = cmd;	
		_totalwritelen += len;
		++_numwrites;
		prt(("s22203876 return true _numwrites=%d\n", _numwrites ));
		return true;
	}

	// flush to disk
	//prt(("s2110224 flush to disk ...\n"));
	if ( _dbPairFileVec[hosti].mempos + len > _dbPairFileVec[hosti].memlen ) { 
		wlen = _dbPairFileVec[hosti].mempos - _dbPairFileVec[hosti].memoff;
		//prt(("s222820 pwrite data len=%d wlen=%d hosti=%d fd=%d\n", len, wlen, hosti, _dbPairFileVec[hosti].fd ));
		if ( _dbPairFileVec[hosti].fd < 0 ) {
			_dbPairFileVec[hosti].fd = _jfsMgr->openfd( _dbPairFileVec[hosti].fpath, true );
			//prt(("s293804 _useDisk is set to 1\n"));
			_useDisk = 1;
		}

		clen = _jfsMgr->pwrite( _dbPairFileVec[hosti].fd, _writebuf[hosti]+_dbPairFileVec[hosti].memoff, wlen, _dbPairFileVec[hosti].diskpos );
		if ( clen < wlen ) {
			clean();
			prt(("s102928 return false clen=%d wlen=%d\n", clen, wlen ));
			return false;
		}
		_useDisk = 1;
		//prt(("s299227 done pwrite memoff=%d clen=%d wlen=%d diskpos=%d\n", _dbPairFileVec[hosti].memoff, clen, wlen, _dbPairFileVec[hosti].diskpos ));

		//memset( _writebuf+_dbPairFileVec[hosti].memoff, 0, wlen );
		memset( _writebuf[hosti]+_dbPairFileVec[hosti].memoff, 0, wlen );
		_dbPairFileVec[hosti].mempos = _dbPairFileVec[hosti].memoff;
		_dbPairFileVec[hosti].disklen += clen;
		_dbPairFileVec[hosti].diskpos = _dbPairFileVec[hosti].disklen;
	}
	
	//prt(("s77521 host_i=%d len=%d >? memlen=%d - memoff=%d (diff=%d)\n", hosti, len, _dbPairFileVec[hosti].memlen, _dbPairFileVec[hosti].memoff, _dbPairFileVec[hosti].memlen - _dbPairFileVec[hosti].memoff ));
	if ( len > _dbPairFileVec[hosti].memlen - _dbPairFileVec[hosti].memoff ) { 
	    // if one len is too large, flush directly to disk, usually not happen
		//prt(("s442827 flush directly to disk, no more room in memory hosti=%d fd=%d\n", hosti, _dbPairFileVec[hosti].fd ));
		if ( _dbPairFileVec[hosti].fd < 0 ) {
			_dbPairFileVec[hosti].fd = _jfsMgr->openfd( _dbPairFileVec[hosti].fpath, true );
			//prt(("s82273 _useDisk = 1\n"));
			_useDisk = 1;
		}

		clen = _jfsMgr->pwrite( _dbPairFileVec[hosti].fd, buf, len, _dbPairFileVec[hosti].diskpos );
		if ( clen < len ) {
			clean();
			prt(("s761100 clen=%d len=%d ret false\n", clen, len ));
			return false;
		}
		_useDisk = 1;
		//prt(("s761101 clen=%d len=%d pwrite OK diskpos=%d\n", clen, len, _dbPairFileVec[hosti].diskpos ));
		_dbPairFileVec[hosti].disklen += clen;
		_dbPairFileVec[hosti].diskpos = _dbPairFileVec[hosti].disklen;		
	} else { // otherwise, put buf into memory
		//prt(("s13376 put buf into memory _writebuf i=%d mempos=%d len=%d\n", hosti, _dbPairFileVec[hosti].mempos, len ));
		memcpy( _writebuf[hosti]+_dbPairFileVec[hosti].mempos, buf, len );
		_dbPairFileVec[hosti].mempos += len;
	}

	_totalwritelen += len;
	++_numwrites;
	//prt(("s411224 writeit buf=[%.50s] len=%ld done, true\n", buf, len));
	return true;
}

// method to flush rest possible memory data to disk, done by main thread
// writeit are all done, flush.  it is end. setwrite is begin.
bool JagDataAggregate::flushwrite()
{
	//prt(("s3333020 flushwrite\n"));
	jagint clen, wlen;
	if ( _keepFile == 1 ) {
		if ( _dbPairFileVec[0].mempos != _dbPairFileVec[0].memoff ) {
			shuffleSQLMemAndFlush();
			_dbPairFileVec[0].mempos = 0;
		}
		cleanWriteBuf();
		if ( _sqlarr ) delete [] _sqlarr;
		_sqlarr = NULL;
	} else if ( _useDisk || _keepFile == 3 ) {
		for ( int i = 0; i < _numHosts; ++i ) {
			if ( _dbPairFileVec[i].mempos != _dbPairFileVec[i].memoff ) {
				wlen = _dbPairFileVec[i].mempos - _dbPairFileVec[i].memoff;
				if ( _dbPairFileVec[i].fd < 0 ) {
					_dbPairFileVec[i].fd = _jfsMgr->openfd( _dbPairFileVec[i].fpath, true );
				}
				clen = _jfsMgr->pwrite( _dbPairFileVec[i].fd, _writebuf[i]+_dbPairFileVec[i].memoff, 
									  wlen, _dbPairFileVec[i].diskpos );
				if ( clen < wlen ) {
					clean();
					return false;
				}
				_dbPairFileVec[i].mempos = _dbPairFileVec[i].memoff;
				_dbPairFileVec[i].disklen += clen;
			}
			_dbPairFileVec[i].diskpos = 0;
		}
		//free( _writebuf );
		//_writebuf = NULL;
		cleanWriteBuf();
	}

	_isFlushWriteDone = 1;
	return true;
}

// method to read data one by one of _datalen, done by main thread
// read from write buf, if thhere is disk, freeed writebuf. readbuf from disk
bool JagDataAggregate::readit( JagFixString &buf )
{
	//prt(("s2222100 readit  _numIdx=%d _numHosts=%d _datalen=%d _useDisk=%d\n", _numIdx, _numHosts, _datalen, (int)_useDisk ));
	//prt(("s23760 readit dtalen=%d _isFlushWriteDone=%d\n", _datalen, (int)_isFlushWriteDone ));
	buf = "";
	if ( 0 == _datalen || !_isFlushWriteDone ) {
		clean();
		//prt(("s23763 readit dtalen=%d _isFlushWriteDone=%d return false\n", _datalen, (int)_isFlushWriteDone ));
		return false;
	}

	if ( !_useDisk ) {
		while ( true ) {
			//prt(("s26355 _numIdx=%d/%d\n", _numIdx, _numHosts ));
			if ( _numIdx >= _numHosts ) {
				break;
			}

			if ( _dbPairFileVec[_numIdx].memoff + _datalen > _dbPairFileVec[_numIdx].mempos ) {
				++_numIdx;
			} else {
				buf = JagFixString( _writebuf[_numIdx]+_dbPairFileVec[_numIdx].memoff, _datalen, _datalen );
				//prt(("c33391 _dbPairFileVec[%d].memoff=%d break while true loop\n", _numIdx, _dbPairFileVec[_numIdx].memoff ));
				//buf.dump();
				_dbPairFileVec[_numIdx].memoff += _datalen;
				break;
			}
		}
		
		if ( _numIdx >= _numHosts ) {
			clean();
			//prt(("s029290 not _useDisk readit return false\n"));
			return false;
		}
		//prt(("s029293 not _useDisk readit return true\n"));
		return true;
	} else {
		// read from disk
		jagint rc;
		if ( !_readbuf ) {
			jagint maxbytes = 10 * ONE_MEGA_BYTES;
			maxbytes /= _datalen;
			if ( 0 == maxbytes ) ++maxbytes;
			maxbytes *= _datalen;
			_readmaxlen = maxbytes;
			_readbuf = (char*)jagmalloc( maxbytes );
			memset( _readbuf, 0, maxbytes );
			rc = readNextBlock();
			if ( rc < 0 ) {
				clean();
				return false;
			}
		}
		
		if ( _readpos + _datalen > _readlen ) { 
			// this block is used up, read next block
			rc = readNextBlock();
			if ( rc < 0 ) {
				clean();
				return false;
			}
		}

		// otherwise, get data from memory buffer
		buf = JagFixString( _readbuf+_readpos, _datalen, _datalen );
		// prt(("c003835 got buf _datalen=%d   _readbuf+_readpos=[%s]\n", _datalen, _readbuf+_readpos ));
		//buf.dump();
		_readpos += _datalen;
		return true;
	}
}

// method to read data one by one of _datalen, done by main thread
bool JagDataAggregate::backreadit( JagFixString &buf )
{
	buf = "";
	if ( 0 == _datalen || !_isFlushWriteDone ) {
		clean();
		return false;
	}

	if ( !_useDisk ) {
		while ( _numIdx < _numHosts ) {
			if ( _dbPairFileVec[_numHosts-_numIdx-1].memoff + _datalen > _dbPairFileVec[_numHosts-_numIdx-1].mempos ) ++_numIdx;
			else {
				//buf = JagFixString( _writebuf+(_dbPairFileVec[_numHosts-_numIdx-1].mempos-_dbPairFileVec[_numHosts-_numIdx-1].memoff-_datalen+_dbPairFileVec[_numHosts-_numIdx-1].memstart), _datalen );
				buf = JagFixString( _writebuf[_numIdx]+(_dbPairFileVec[_numHosts-_numIdx-1].mempos-_dbPairFileVec[_numHosts-_numIdx-1].memoff-_datalen+_dbPairFileVec[_numHosts-_numIdx-1].memstart), _datalen, _datalen );
				_dbPairFileVec[_numHosts-_numIdx-1].memoff += _datalen;
				break;
			}
		}
		
		if ( _numIdx >= _numHosts ) {
			clean();
			return false;
		}
		return true;
	} else {
		jagint rc;
		if ( !_readbuf ) {
			jagint maxbytes = getUsableMemory();
			maxbytes /= _datalen;
			if ( 0 == maxbytes ) ++maxbytes;
			maxbytes *= _datalen;
			_readmaxlen = maxbytes;
			_readbuf = (char*)jagmalloc( maxbytes );
			memset( _readbuf, 0, maxbytes );
			rc = backreadNextBlock();
			if ( rc < 0 ) {
				clean();
				return false;
			}
		}
		
		if ( _readpos + _datalen > _readlen ) { // this block is used up, read next block
			rc = backreadNextBlock();
			if ( rc < 0 ) {
				clean();
				return false;
			}
		}
		// otherwise, get data from memory buffer
		//buf = JagFixString( _readbuf+(_readlen-_readpos-_datalen), _datalen );
		buf = JagFixString( _readbuf+(_readlen-_readpos-_datalen), _datalen, _datalen );
		_readpos += _datalen;
		return true;
	}
	
}

// method to set read start position and end position in number idx, used by server
void JagDataAggregate::setread( jagint start, jagint readcnt )
{
	// _numIdx should be 0
	if ( _useDisk ) {
		_dbPairFileVec[_numIdx].diskpos = start * _datalen;
		_dbPairFileVec[_numIdx].disklen = _dbPairFileVec[_numIdx].diskpos + readcnt * _datalen;
	} else {
		_dbPairFileVec[_numIdx].memoff = start * _datalen;
		_dbPairFileVec[_numIdx].mempos = _dbPairFileVec[_numIdx].memoff + readcnt * _datalen;
	}
}

// method to read data block by block, done by main thread, used by server
char *JagDataAggregate::readBlock( jagint &len )
{
	if ( 0 == _datalen || !_isFlushWriteDone ) {
		clean();
		len = -1;
		return NULL;
	}

	if ( !_useDisk ) {
		if ( _numIdx >= _dbPairFileVec.size() || _dbPairFileVec[_numIdx].memoff >= _dbPairFileVec[_numIdx].mempos ) {
			clean();
			len = -1;
			return NULL;
		} 
		char *ptr = _writebuf[_numIdx]+_dbPairFileVec[_numIdx].memoff;
		len = _dbPairFileVec[_numIdx].mempos - _dbPairFileVec[_numIdx].memoff;
		_dbPairFileVec[_numIdx].memoff += len;
		++_numIdx;
		return ptr;
	} else {
		jagint rc;
		if ( !_readbuf ) {
			jagint maxbytes = 10*ONE_MEGA_BYTES;
			maxbytes /= _datalen;
			if ( 0 == maxbytes ) ++maxbytes;
			maxbytes *= _datalen;
			_readmaxlen = maxbytes;
			_readbuf = (char*)jagmalloc( maxbytes );
			memset( _readbuf, 0, maxbytes );
			rc = readNextBlock();
			if ( rc < 0 ) {
				clean();
				len = -1;
				return NULL;
			}
		}
	
		if ( _readpos + _datalen > _readlen ) { // this block is used up, read next block
			rc = readNextBlock();
			if ( rc < 0 ) {
				clean();
				len = -1;
				return NULL;
			}
		}
	
		// otherwise, get data from memory buffer
		_readpos = _readlen;
		len = _readlen;
		return _readbuf;
	}
}

// protected method to read next disk block to memeory, called by readit
jagint JagDataAggregate::readNextBlock()
{
	memset( _readbuf, 0, _readlen );
	_readpos = _readlen = 0;
	
	if ( _numIdx >= _numHosts ) { // to the end, nothing more
		return -1;
	}
	
	jagint mrest, drest, clen;
	while( true ) {
		if ( _numIdx >= _numHosts || _readmaxlen == _readlen ) break;
		mrest = _readmaxlen - _readlen;
		drest = _dbPairFileVec[_numIdx].disklen - _dbPairFileVec[_numIdx].diskpos;
		
		if ( 0 == _dbPairFileVec[_numIdx].disklen ) ++_numIdx;
		else {
			if ( drest > mrest ) {
				clen = _jfsMgr->pread( _dbPairFileVec[_numIdx].fd, _readbuf+_readpos, 
									 mrest, _dbPairFileVec[_numIdx].diskpos );
				if ( clen < mrest ) {
					return -1;
				}
				_dbPairFileVec[_numIdx].diskpos += clen;
				_readlen += clen;
				_readpos += clen;
			} else {
				clen = _jfsMgr->pread( _dbPairFileVec[_numIdx].fd, _readbuf+_readpos, 
									drest, _dbPairFileVec[_numIdx].diskpos );
				if ( clen < drest ) {
					return -1;
				}
				++_numIdx;
				_readlen += clen;
				_readpos += clen;
			}
		}
	}
	
	_readpos = 0;
	return _readlen;
}

jagint JagDataAggregate::backreadNextBlock()
{
	memset( _readbuf, 0, _readlen );
	_readpos = _readlen = 0;
	
	if ( _numIdx >= _numHosts ) { // to the end, nothing more
		return -1;
	}
	
	jagint mrest, drest, clen;
	while( true ) {
		if ( _numIdx >= _numHosts || _readmaxlen == _readlen ) break;
		mrest = _readmaxlen - _readlen;
		drest = _dbPairFileVec[_numHosts-_numIdx-1].disklen - _dbPairFileVec[_numHosts-_numIdx-1].diskpos;
		
		if ( 0 == _dbPairFileVec[_numHosts-_numIdx-1].disklen ) ++_numIdx;
		else {
			if ( drest > mrest ) {
				clen = _jfsMgr->pread( _dbPairFileVec[_numHosts-_numIdx-1].fd, 
									 _readbuf+_readpos, mrest, drest-mrest );
				if ( clen < mrest ) {
					return -1;
				}
				_dbPairFileVec[_numHosts-_numIdx-1].diskpos += clen;
				_readlen += clen;
				_readpos += clen;
			} else {
				clen = _jfsMgr->pread( _dbPairFileVec[_numHosts-_numIdx-1].fd, _readbuf+_readpos, drest, 0 );
				if ( clen < drest ) {
					return -1;
				}
				++_numIdx;
				_readlen += clen;
				_readpos += clen;
				break;
			}
		}
	}
	
	_readpos = 0;
	return _readlen;
}

jagint JagDataAggregate::getUsableMemory()
{
	jagint maxbytes, callCounts = -1, lastBytes = 0;
	int mempect = atoi(_cfg->getValue("DBPAIR_MEM_PERCENT", "5").c_str());

	if ( mempect < 2 ) mempect = 2;
	else if ( mempect > 50 ) mempect = 50;

	maxbytes = availableMemory( callCounts, lastBytes )*mempect/100;

	if ( maxbytes < 100000000 ) {
		maxbytes = 100000000;
	}

	if ( _isServ && maxbytes > _maxLimitBytes ) {
		maxbytes = _maxLimitBytes;
	}

	if ( !_isServ ) {
		_maxLimitBytes = 50*ONE_MEGA_BYTES;
		if ( maxbytes > _maxLimitBytes ) {
			maxbytes = _maxLimitBytes;
		}
	}

	return maxbytes;
}

// join-read the dbpair files with matching keys.
// returns 1 if valid data is read; 0 if no more data (vec.length() should be zero)
int JagDataAggregate::joinReadNext( JagVector<JagFixString> &vec )
{
	if ( 0 == _datalen || ! _isFlushWriteDone ) {
		clean();
		return 0;
	}

	if ( ! _useDisk ) {
		return _joinReadFromMem( vec );
	} else {
		return _joinReadFromDisk( vec );
	}
}

void JagDataAggregate::beginJoinRead( int klen )
{
	_keylen = klen;
	_vallen = _datalen - klen;

	for ( int i = 0; i < _numHosts; ++i ) {
		_goNext[i] = 1;
	}

	if ( NULL == _mergeReader ) {
		JagVector<OnefileRangeFD> vec;
		jagint memmax = 1024/_numHosts;
		memmax = getBuffReaderWriterMemorySize( memmax );
		for ( int i = 0; i < _numHosts; ++i ) {
			OnefileRangeFD fr;
			fr.fd = _dbPairFileVec[i].fd;
			fr.startpos = 0;
			fr.readlen = _dbPairFileVec[i].disklen/_datalen;
			fr.memmax = memmax;
			vec.append( fr );
		}
		_mergeReader = new JagSingleMergeReader( vec, _numHosts, _keylen, _vallen );
	}

	_endcnt = 0;
}

void JagDataAggregate::endJoinRead()
{
	clean();
}


// read from _writebuf
int JagDataAggregate::_joinReadFromMem( JagVector<JagFixString> &vec )
{
	int rc, cnt = 0, minpos = -1;
	
	for ( int i = 0; i < _numHosts; ++i ) {
		if ( 1 == _goNext[i] ) {
			rc = _getNextDataOfHostFromMem( i ); // from _writebuf to _dbPairFileVec[i].kv
			if ( rc > 0 ) {
				_goNext[i] = 0;
				if ( minpos < 0 ) minpos = i; // first ith position which has next value
			} else {
				_goNext[i] = -1;
				++_endcnt;
			}
		} else if ( 0 == _goNext[i] ) {
			if ( minpos < 0 ) minpos = i;
		}
	}
	
	if ( _endcnt == _numHosts || minpos < 0 ) {
		return 0;
	}
	
	// find min data
	for ( int i = 0; i < _numHosts; ++i ) {
		if ( _goNext[i] != -1 ) {
			rc = memcmp( _dbPairFileVec[i].kv.c_str(), _dbPairFileVec[minpos].kv.c_str(), _keylen );
			if ( rc < 0 ) {
				minpos = i;
			}
		}
	}

	// also find if other data are same as min data
	for ( int i = 0; i < _numHosts; ++i ) {
		if ( _goNext[i] != -1 ) {
			rc = memcmp( _dbPairFileVec[i].kv.c_str(), _dbPairFileVec[minpos].kv.c_str(), _keylen );
			if ( 0 == rc ) {
				vec.append( _dbPairFileVec[i].kv ); // append another min data
				++cnt;
				_goNext[i] = 1;
			}
		}
	}
	
	/***
	int smallestnum = 0;
	int beginpos = 0;
	int rc;
	JagFixString kv;
	int  smallMoved = 0;

	for ( int i = 0; i < _numHosts; ++i ) {
		if ( 1 == _goNext[i] ) {
			// rc = _vec[i]->getNext( _buf+i*KEYVALLEN, pos );
			rc = _getNextDataOfHostFromMem( i  ); // from _writebuf to _dbPairFileVec[i].kv
			if ( 0 != rc ) {
				// got data
				_goNext[i] = 0;
				kv = _dbPairFileVec[i].kv;
				smallMoved = 1; // las-min key has moved down and got one new data
			} else {
				_goNext[i] = -1;
				++_endcnt;
			}
		}
	}

	if ( _endcnt == _numHosts ) {
		// clean();
		return 0;
	}
	
	// all _goNext[i] are either -1 or 0
	if ( ! smallMoved ) {
    	for ( int i = 0; i < _numHosts; ++i ) {
    		if ( _goNext[i] != -1 ) {
    			// memcpy(buf, _buf+i*KEYVALLEN, KEYVALLEN);
    			beginpos = i;
				kv = _dbPairFileVec[i].kv;
    			break;
    		}
    	}
	}

	int cnt = 0;
	for ( int i = beginpos; i < _numHosts; ++i ) {
		if ( _goNext[i] != -1 ) {
			// rc = memcmp(buf, _buf+i*KEYVALLEN, KEYLEN);
			if ( _dbPairFileVec[i].kv <= kv ) {
				// buf is >= i-th element
				if ( _dbPairFileVec[i].kv == kv ) {
					if ( _goNext[smallestnum] != -1 ) {
						_goNext[smallestnum] = 1;
					}
					vec.append( _dbPairFileVec[i].kv );
					++cnt;
				} else {
					vec.clean();
					cnt = 0;
				}
				// memcpy(buf, _buf+i*KEYVALLEN, KEYVALLEN);
				kv = _dbPairFileVec[i].kv;
				smallestnum = i;
			}
		}
	}

	_goNext[smallestnum] = 1;

	// debug
	prt(("c6601 mem(cnt=%d): ", cnt ));
	for ( int i=0; i < cnt; ++i ) {
		prt((" %[%s]", vec[i].c_str() ));
	}
	prt(("\n"));
	***/

	return cnt;	
}

// 1: OK
// 0: no more data
// Read from _writebuf to _dbPairFileVec[i].kv
// i is host num 0 -> N-1
int JagDataAggregate::_getNextDataOfHostFromMem( int hosti  )
{
	jagint memoff = _dbPairFileVec[hosti].memoff;
	if ( memoff >= _dbPairFileVec[hosti].mempos ) {
		return 0; // no more data
	}
	
	//_dbPairFileVec[i].kv  = JagFixString( _writebuf+memoff, _datalen );
	_dbPairFileVec[hosti].kv  = JagFixString( _writebuf[hosti]+memoff, _datalen, _datalen );
	_dbPairFileVec[hosti].memoff += _datalen;

	return 1;
}


// get next record from multiple dbpair files
// return n: n==vec.length()
//        0: no more, caller should call endJoinRead()
int JagDataAggregate::_joinReadFromDisk( JagVector<JagFixString> &vec )
{
	int cnt = _mergeReader->getNext( vec );

	// debug
	/***
	prt(("c6602 disk(cnt=%d): ", cnt ));
	for ( int i=0; i < cnt; ++i ) {
		prt((" %[%s]", vec[i].c_str() ));
	}
	prt(("\n"));
	***/

	return cnt;

}

void JagDataAggregate::shuffleSQLMemAndFlush()
{
	//prt(("s2209111 shuffleSQLMemAndFlush\n" ));
	srand(time(NULL));
	int radidx;
	// open new random file
	while ( true ) {
		radidx = rand() % JAG_RANDOM_FILE_LIMIT;
		_dbPairFileVec[0].fpath = _dirpath + _dbobj + "." + intToStr( radidx ) + ".sql";
		if ( _jfsMgr->exist( _dbPairFileVec[0].fpath ) ) {
			continue;
		} 
		_dbPairFileVec[0].fd = _jfsMgr->openfd( _dbPairFileVec[0].fpath, true );
		break;
	}

	for ( int i = 0; i < _dbPairFileVec[0].mempos; ++i ) {
		raysafewrite( _dbPairFileVec[0].fd, (char*)_sqlarr[i].c_str(), _sqlarr[i].size() );
		_sqlarr[i] = "";
	}
	
	_jfsMgr->closefd( _dbPairFileVec[0].fpath );
}

// send data to client, with sqlhdr
void JagDataAggregate::sendDataToClient( jagint cnt, const JagRequest &req )
{
	jagint len;
	char *ptr = NULL;
	char buf[SELECT_DATA_REQUEST_LEN+1];
	memset(buf, 0, SELECT_DATA_REQUEST_LEN+1);

	//prt(("s9283 sendDataToClient cnt=%d this->getdatalen=%d\n", cnt,  this->getdatalen() ));

	// first, send num of data to client, then waiting for request cmd
	sprintf( buf, "_datanum|%lld|%lld", cnt, this->getdatalen() );
	prt(("s42029 in sendDataToClient() sendMessage buf=[%s]\n", buf ));

	// send
	sendMessage( req, buf, "OK" );

	memset(buf, 0, SELECT_DATA_REQUEST_LEN+1);
 	char hdr[JAG_SOCK_TOTAL_HDR_LEN+1];
	char *newbuf = NULL;

	prt(("a21081 recvMessage ...\n"));
	jagint clen = recvMessage( req.session->sock, hdr, newbuf );
	prt(("s38733 recvMessage received hdr=[%s] newbuf=[%s]\n", hdr, newbuf ));

	//prt(("s9999 request cmd=[%s]\n", newbuf));
	if ( clen > 0 && 0 == strncmp( newbuf, "_send", 5 ) ) { // need send data to client
		prt(("s202911 newbuf has _send\n"));
		if ( 0 == strncmp( newbuf, "_senddata|", 10 ) ) { // split cmd to get start position and readcnt for range data
			JagStrSplit sp( newbuf, '|', true );
			this->setread( jagatoll( sp[1].c_str() ), jagatoll( sp[2].c_str() ) );
		} // otherwise, send all data
		
		// begin send range data to client 
		jagint cnt = 0;  // debug only
		while ( true ) {
			if ( ! ( ptr = this->readBlock( len ) ) || len < 0 ) {
				break;
			}

			prt(("s9319 sendMessageLength len=%lld ptr=[%.60s]\n", len, ptr));
			if ( sendMessageLength( req, ptr, len, "XX" ) < 0 ) {
				break;
			}
			++cnt;
		}
	} // otherwise, discard data  _discard

	prt(("s20018 done senddatatoclient\n"));
	if ( newbuf ) free( newbuf );
}

void JagDataAggregate::initWriteBuf()
{
	for ( int i=0; i < JAG_MAX_HOSTS; ++i ) {
		_writebuf[i] = NULL;
	}
	_writebufHasData = false;
}

void JagDataAggregate::cleanWriteBuf()
{
	if ( ! _writebufHasData ) return;
	for ( int i=0; i < JAG_MAX_HOSTS; ++i ) {
		if ( _writebuf[i] ) {
			free( _writebuf[i] );
			_writebuf[i] = NULL;
		}
	}

	_writebufHasData = false;
}

