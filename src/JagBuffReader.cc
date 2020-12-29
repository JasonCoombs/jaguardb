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

#include <fcntl.h>
#include <sys/stat.h>

#include <JDFS.h>
#include <JagCfg.h>
#include <JagUtil.h>
#include <JagDBPair.h>
#include <JagDiskArrayBase.h>
#include <JagBuffReader.h>

JagBuffReader::JagBuffReader( JagDiskArrayBase *darr, jagint readlen, jagint keylen, jagint vallen, 
							  jagint start, jagint headoffset, jagint bufferSize ) 
	: _darr( darr )
{
	KEYLEN = keylen; 
	VALLEN = vallen;
	KEYVALLEN = keylen + vallen;
	_dolock = 0;
	_readAll = 0;
	_setRestartPos = 0;
	_lastSuperBlock = -1;
	_relpos = 0;
	_stlastSuperBlock = -1;
	_strelpos = 0;
	_curBlockElements = 0;
	_headoffset = headoffset;
	_start = start;
	_readlen = readlen;

	_elements = getNumBlocks( KEYVALLEN, bufferSize )*JAG_BLOCK_SIZE;
	if ( KEYVALLEN > 100000 ) { _elements = 2; } // for tables with large kvlen
	
	AbaxString dolock = "NO";
	if ( darr->_servobj && darr->_servobj->_cfg ) {
		dolock = darr->_servobj->_cfg->getValue("BUFF_READER_LOCK", "NO");
	}
	if ( dolock != "NO" ) { _dolock = 1; }
	
	if ( _start < 0 ) { // start position is less than 0, regard as read all table
		_start = 0;
		_readAll = 1;
		if ( _readlen < 0 ) {
			struct stat sbuf;
			if ( stat(darr->_filePath.c_str(), &sbuf) != 0 || sbuf.st_size/KEYVALLEN == 0 ) {
				prt(("s222029 fatal error file=[%s] not found\n", darr->_filePath.c_str() ));
				abort();
			} // return;
			_readlen = sbuf.st_size/KEYVALLEN;
		}
	}
	
	_superbuf = NULL;
	_superbuf = (char*)jagmalloc( _elements*KEYVALLEN );
	while ( !_superbuf ) {
		_elements = _elements/2;
		_superbuf = (char*)jagmalloc( _elements*KEYVALLEN );
		if ( _superbuf ) {
			raydebug( stdout, JAG_LOG_LOW, "JagBuffReader malloc smaller memory %l _elements=%l _fpath=[%s]\n", 
					_elements*KEYVALLEN, _elements, _darr->getFilePath().c_str() );
		}
	}
	memset( _superbuf, 0, KEYVALLEN );
	
	/***
	prt(("s2945 JagBuffReader ctor done, fp=[%s] klen=%lld vlen=%lld dolock=%d readAll=%d lastSuperBlock=%d relpos=%d ",
		_darr->getFilePath().c_str(), KEYLEN, VALLEN, _dolock, _readAll, _lastSuperBlock, _relpos));
	prt(("headoffset=%lld start=%lld readlen=%lld numResize=%lld elements=%lld superbuf_size=%lld superbuf=%0x\n",
		_headoffset, _start, _readlen, _numResize, _elements, _elements*KEYVALLEN, _superbuf));
	***/
	_n = 0; // for debug
}

JagBuffReader::~JagBuffReader()
{
	if ( _superbuf ) {
		free( _superbuf );
		_superbuf = NULL;
		jagmalloc_trim(0);
		//raydebug( stdout, JAG_LOG_HIGH, "s4829 bufrdr _elements=%l freed\n", _elements );
	}
}

jagint JagBuffReader::getNumBlocks( jagint kvlen, jagint bufferSize )
{
	jagint num = 4096;
	jagint bytes = num*kvlen*JAG_BLOCK_SIZE;
	jagint maxmb = 32;

	if ( bufferSize > 0 ) {
		maxmb = bufferSize;
		bytes = maxmb * 1024 * 1024;
		num = bytes/kvlen/JAG_BLOCK_SIZE;
	}

	if ( bytes > maxmb * 1024*1024 ) {
		num = maxmb*1024*1024/kvlen/JAG_BLOCK_SIZE;
	}

	//prt(("s7384 JagBuffReader::getNumBlocks() num=%lld blocks\n", num));
	return num;
}

bool JagBuffReader::getNext( char *buf )
{
	jagint pos;
	return getNext( buf, KEYVALLEN, pos );
}

bool JagBuffReader::getNext( char *buf, jagint &i )
{
	return getNext( buf, KEYVALLEN, i );
}

bool JagBuffReader::getNext( char *buf, jagint len, jagint &i )
{
	//prt(("s11209 JagBuffReader::getNext len=%d\n", len ));
	if ( !_darr ) {
		//prt(("e8394 error read buffer darr empty\n"));
		return false;
	}

	if ( len < KEYVALLEN ) {
		//prt(("s80394 error JagBuffReader::getNext passedin len=%d is less than KEYVALLEN=%d\n", len, KEYVALLEN ));
		return false;
	}

	if ( _lastSuperBlock*_elements + _relpos >= _readlen ) {
		//prt(("s44190 reach to the end\n"));
		//prt(("s44190 _lastSuperBlock=%d _elements=%d _relpos=%d  _readlen=%d false\n", _lastSuperBlock, _elements, _relpos, _readlen));
		return false;
	}

	//prt(("s44190 _lastSuperBlock=%d _elements=%d _relpos=%d  _readlen=%d ok\n", _lastSuperBlock, _elements, _relpos, _readlen));
	
	jagint rc;
    if ( -1 == _lastSuperBlock ) { // first time read
		//if ( _dolock ) { _darr->_memLock->readLock( -1 ); }	
		//if ( _dolock ) { pthread_rwlock_rdlock( &(_darr->_memLock) ); } // 7-2-2020
		/***
		if ( _numResize != _darr->_numOfResizes ) {
			if ( !_readAll ) { // if file has been resized and buffreader request is not all data, stop reading
				if ( _dolock ) { _darr->_memLock->readUnlock( -1 ); }
				return false;
			}
			_readlen = _darr->_garrlen;
			_numResize = _darr->_numOfResizes;
		}
		**/
	
		if ( _readlen <= _elements ) {  // total is smaller than a single superblock
			_curBlockElements = _readlen;
		} else {
			_curBlockElements = _elements;			
		}
		rc = jdfpread( _darr->_jdfs, _superbuf, _curBlockElements*KEYVALLEN, _start*KEYVALLEN+_headoffset );
		// prt(("s3000 raypread rc=%lld\n", rc ));
		//if ( _dolock ) { _darr->_memLock->readUnlock( -1 ); }
		//if ( _dolock ) { pthread_rwlock_unlock( &_darr->_memLock ); }

		if ( rc <= 0 ) { 
			// no valid bytes read from file
			//prt(("s222083 jdfpread rc=%d <=0 no valid bytes from file false\n", rc ));
			return false;
		}
        _lastSuperBlock = 0;
		_relpos = 0;
    }

	rc = findNonblankElement( buf, i );
	if ( !rc ) { // no more data
		//prt(("s222183 no more data false _n=%d _readlen=%d\n", _n, _readlen ));
       	return false;
	}

	++ _n;
    return true;
}

bool JagBuffReader::findNonblankElement( char *buf, jagint &i )
{
	jagint rc;
	while ( 1 ) {
		if ( _relpos < _curBlockElements ) {
			// current block is not reach to the end
	 		if ( _superbuf[_relpos*KEYVALLEN] == '\0' ) {
				// is empty, find next kvlen
				++_relpos;
				continue;
			} else {
				// get data and return
				memcpy(buf, _superbuf+_relpos*KEYVALLEN, KEYVALLEN);
				i = _lastSuperBlock*_elements+_relpos;
				++_relpos;
				return true;
			}
		} else if ( _curBlockElements < _elements ) {
			// _relpos reaches end of current block, and no more blocks to be read from file ( since last block is not read full )
			return false;
		} else {
			// _relpos reaches end of current block, read next superblock
			_relpos = 0;
			++ _lastSuperBlock;

			//if ( _dolock ) { _darr->_memLock->readLock( -1 ); }
			//if ( _dolock ) { _darr->_memLock->readLock( -1 ); }
			//if ( _dolock ) { pthread_rwlock_rdlock( &_darr->_memLock ); }
			// check to see if more data needed to be read
			rc = _readlen-_lastSuperBlock*_elements;
			if ( rc <= 0 ) {
				// reach the end of read range
				//if ( _dolock ) { _darr->_memLock->readUnlock( -1 ); }
				//if ( _dolock ) {  pthread_rwlock_unlock( &_darr->_memLock ); }
				return false;
			} else if ( rc < _elements ) {
				_curBlockElements = rc;
			} else {
				_curBlockElements = _elements;
			}

			rc = jdfpread( _darr->_jdfs, _superbuf, _curBlockElements*KEYVALLEN, (_start+_lastSuperBlock*_elements)*KEYVALLEN+_headoffset );
			// prt(("s3002 raypread rc=%lld\n", rc ));
			//if ( _dolock ) { _darr->_memLock->readUnlock( -1 ); }
			//if ( _dolock ) {  pthread_rwlock_unlock( &_darr->_memLock ); }
						
			if ( rc <= 0 ) {
				// no valid bytes read from file
				return false;
			}
			
			// if resized and not readAll, ignore data which is less than current buf
			/***
			if ( _numResize != _darr->_numOfResizes ) {
				if ( !_readAll ) {
					return false;
				}
				_numResize = _darr->_numOfResizes;
				_readlen = _darr->_garrlen;

				while ( 1 ) {
					if ( _relpos < _curBlockElements ) {
	 					if ( _superbuf[_relpos*KEYVALLEN] == '\0' ) {
							// if empty, jump to next one
							++_relpos;
							continue;
						} else {
							if ( memcmp(_superbuf+_relpos*KEYVALLEN, buf, KEYLEN) <= 0 ) {
								// if this data is smaller than last read one ( buf ), ignore and read next
								++_relpos;
								continue;
							} else {
								// otherwise, break loop and use this one ( get data from next outer loop )
								break;
							}
						}
					} else {
						// otherwise, break loop and read next super block ( get data from next outer loop )
						break;
					}
				}
			}
			***/
		}
	}

    return false;
}

// method to call if want to store 
bool JagBuffReader::setRestartPos()
{
	if ( _setRestartPos ) return false;
	_stlastSuperBlock = _lastSuperBlock;
	_strelpos = _relpos-1; // since relpos+1 after get last data, need to store at _relpos-1 for actual buf start position
	_setRestartPos = 1;
	return true;
}

// method to move back block buffer to position where stored by setRestartPos
bool JagBuffReader::moveToRestartPos()
{
	if ( !_setRestartPos ) return false;
	// move back to stored position
	// if super block counts are the same, simple move _relpos is enough
	// otherwise, read the super block again
	jagint rc;
	if ( _stlastSuperBlock >= _lastSuperBlock ) {
		_relpos = _strelpos;
	} else {
		//if ( _dolock ) { _darr->_memLock->readLock( -1 ); }
		//if ( _dolock ) {  pthread_rwlock_rdlock( &_darr->_memLock ); }
		rc = _readlen - _stlastSuperBlock*_elements;
		if ( rc <= 0 ) {
			// reach the end of read range
			//if ( _dolock ) { _darr->_memLock->readUnlock( -1 ); }
			//if ( _dolock ) {  pthread_rwlock_unlock( &_darr->_memLock ); }
			_setRestartPos = 0;
			return false;
		} else if ( rc < _elements ) {
			_curBlockElements = rc;
		} else {
			_curBlockElements = _elements;
		}

		rc = jdfpread( _darr->_jdfs, _superbuf, _curBlockElements*KEYVALLEN, (_start+_stlastSuperBlock*_elements)*KEYVALLEN+_headoffset );
		// if ( _dolock ) { _darr->_memLock->readUnlock( -1 ); }
		//if ( _dolock ) {  pthread_rwlock_unlock( &_darr->_memLock ); }
		if ( rc <= 0 ) {
			// no valid bytes read from file
			_setRestartPos = 0;
			return false;
		}

		_relpos = _strelpos;
		_lastSuperBlock = _stlastSuperBlock;
		_setRestartPos = 0;
		return true;
	}
	return false;
}

void JagBuffReader::setClearRestartPosFlag() { 
	_setRestartPos = 0; 
}
