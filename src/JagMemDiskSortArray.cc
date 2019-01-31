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

#include<JagMemDiskSortArray.h>
#include<JagDataAggregate.h>
#include<JagDiskArrayClient.h>
#include<JagSchemaRecord.h>
#include<JagBuffBackReader.h>
#include<JagBuffReader.h>
#include<JagArray.h>
#include<JagMutex.h>

JagMemDiskSortArray::JagMemDiskSortArray()
{
	_memarr = NULL;
	_diskarr = NULL;
	_jda = NULL;
	_srecord = NULL;
	_ntr = NULL;
	_bntr = NULL;
	_diskhdr = "";
	_opstr = "";
	_mempos = 0;
	_memlimit = 0;
	_rwtype = 0;
	_usedisk = 0;
	_isback = 0;
	_hassort = 0;

	_keylen = 0;
	_vallen = 0;
	_kvlen = 0;

	_cnt = 0;
	_cntlimit = 0;
	_rend = 0;

	_cntstr = "";
	_selhdr = "";
	_aggstr = "";
	pthread_mutex_init( &_umutex, NULL );
}

JagMemDiskSortArray::~JagMemDiskSortArray()
{
	clean();
	pthread_mutex_destroy( &_umutex );
}

void JagMemDiskSortArray::init( int memlimit, const char *diskhdr, const char *opstr )
{
	// memlimit in MB
	clean();
	if ( memlimit > 0 ) {
		_memlimit = memlimit;
		_memlimit *= 1024*1024;
		// _memlimit = memlimit*1024*1024; // in bytes
		_diskhdr = diskhdr;
		_opstr = opstr;
	}
}

void JagMemDiskSortArray::clean()
{
	if ( _memarr ) {
		delete _memarr;
		_memarr = NULL;
	}
	
	if ( _diskarr ) {
		delete _diskarr;
		_diskarr = NULL;
	}
	
	if ( _srecord ) {
		delete _srecord;
		_srecord = NULL;
	}
	
	if ( _ntr ) {
		delete _ntr;
		_ntr = NULL;
	}

	if ( _bntr ) {
		delete _bntr;
		_bntr = NULL;
	}

	// please pay attention, _jda pointer is passed outside of class
	// do not delete the pointer inside this class
	if ( _jda ) {
		_jda = NULL;
	}
	
	_diskhdr = "";
	_opstr = "";

	_mempos = 0;
	_memlimit = 0;
	_rwtype = 0;
	_usedisk = 0;
	_isback = 0;
	_hassort = 0;

	_keylen = 0;
	_vallen = 0;
	_kvlen = 0;

	_cnt = 0;
	_cntlimit = 0;
	_rend = 0;

	_cntstr = "";
	_selhdr = "";
	_aggstr = "";
}

void JagMemDiskSortArray::setClientUseStr( const Jstr &selcnt, const Jstr &selhdr, const JagFixString &aggstr )
{
	_cntstr = selcnt;
	_selhdr = selhdr;
	_aggstr = aggstr;
	_rwtype = 3;
};

void JagMemDiskSortArray::setJDA( JagDataAggregate *jda, bool isback )
{
	_jda = jda;
	_isback = isback;
	_rwtype = 3;
	_keylen = 0;
	_vallen = _jda->getdatalen();
	_kvlen = _keylen + _vallen;
}

int JagMemDiskSortArray::beginWrite()
{
	if ( 0 != _rwtype ) return 0;
	_hassort = 1;
	_rwtype = 1;
	_memarr = new JagArray<JagDBPair>();
	_srecord = new JagSchemaRecord( true );
	_srecord->parseRecord( _diskhdr.c_str() );
	_keylen = _srecord->keyLength;
	_vallen = _srecord->valueLength;
	_kvlen = _keylen + _vallen;
	return 1;
}
int JagMemDiskSortArray::endWrite()
{
	if ( 1 != _rwtype ) return 0;
	if ( _usedisk ) {
		_diskarr->flushInsertBufferSync();
	}
	_rwtype = 2;
	return 1;	
}
int JagMemDiskSortArray::beginRead( bool isback )
{
	if ( 2 != _rwtype ) return 0;
	_isback = isback;

	if ( !_usedisk ) {
		if ( !_isback ) _mempos = 0;
		else _mempos = _memarr->_arrlen-1;
	} else {
		if ( _ntr ) {
			delete _ntr;
			_ntr = NULL;
		}		
		if ( _bntr ) {
			delete _bntr;
			_bntr = NULL;
		}		
		if ( !_isback ) _ntr = new JagBuffReader( _diskarr, _diskarr->_arrlen, _diskarr->KEYLEN, _diskarr->VALLEN, 0 );
		else _bntr = new JagBuffBackReader( _diskarr, _diskarr->_arrlen, _diskarr->KEYLEN, _diskarr->VALLEN, _diskarr->_arrlen );
	}
	_rwtype = 3;
	return 1;	
}
int JagMemDiskSortArray::endRead()
{
	if ( 3 != _rwtype ) return 0;
	_rwtype = 0;
	clean();
	return 1;	
}

int JagMemDiskSortArray::insert( JagDBPair &pair )
{
	if ( 1 != _rwtype ) return 0; // not correct sequence calling for write
	
	if ( !_usedisk ) {
		abaxint curbytes = _memarr->_arrlen*(_kvlen);
		if ( curbytes < _memlimit ) {
			_memarr->insert( pair );
		} else {
			// memory is used up, transfer data to disk
			if ( _diskarr ) {
				delete _diskarr;
				_diskarr = NULL;
			}
			Jstr fpath = jaguarHome() + "/tmp/" + longToStr(THREADID) + "_" + _opstr;
			_diskarr = new JagDiskArrayClient( fpath, _srecord, true );
			
			JagDBPair mpair;
			// read all buffer and flush to disk
			for ( abaxint i = 0; i < _memarr->_arrlen; ++i ) {
				if ( _memarr->isNull( i ) ) continue;
				mpair = _memarr->exactAt( i );
				_diskarr->insertSync( mpair );
			}
			_diskarr->insertSync( pair );
			_usedisk = true;
			if ( _memarr ) {
				delete _memarr;
				_memarr = NULL;
			}
		}
	} else {
		_diskarr->insertSync( pair );
	}
	return 1;
}

// return 0: no more data accepted
// memory of value can be changed for this method
int JagMemDiskSortArray::groupByUpdate( const JagDBPair &pair )
{
	if ( 1 != _rwtype ) return 0; // not correct sequence calling for write
	
	int rc;
	abaxint index;
	JagDBPair oldpair;
	if ( !_usedisk ) {
		abaxint curbytes = _memarr->_arrlen*(_kvlen);
		if ( curbytes < _memlimit ) {
			oldpair.key = pair.key;
			rc = _memarr->get( oldpair );
			if ( !rc ) {
				_memarr->insert( pair );
			} else {
				groupByValueCalculation( pair, oldpair );
				_memarr->set( oldpair );
			}
			return 1;
		} 
	} 
	return 0;
}

void JagMemDiskSortArray::groupByValueCalculation( const JagDBPair &pair, JagDBPair &oldpair )
{
	char save;
	Jstr type;
	int rc, keylen, offset, length, func;
	abaxint cnt, cnt1, cnt2;
	abaxdouble value1 = 0.0, value2 = 0.0, value3 = 0.0;
	JagDBPair toldpair = oldpair;
	char *value = (char*)oldpair.value.c_str();
	keylen = _srecord->keyLength;
	for ( int i = _srecord->numKeys; i < _srecord->columnVector->size(); ++i ) {
		type = (*(_srecord->columnVector))[i].type;
		func = (*(_srecord->columnVector))[i].func;
		offset = (*(_srecord->columnVector))[i].offset;
		length = (*(_srecord->columnVector))[i].length;
		offset -= keylen;
		if ( func == JAG_FUNC_COUNT ) {
			cnt1 = rayatol(value+offset, length);
			cnt2 = rayatol(pair.value.c_str()+offset, length);
			cnt = cnt1 + cnt2;
			value1 = (abaxdouble)cnt;
			save = *(value+offset+length);
			jagsprintfLongDouble( 1, 1, value+offset, value1, length );
			*(value+offset+length) = save;
		} else if ( func == JAG_FUNC_SUM ) {
			value1 = raystrtold(value+offset, length);
			value2 = raystrtold(pair.value.c_str()+offset, length);
			value1 += value2;
			save = *(value+offset+length);
			jagsprintfLongDouble( 1, 1, value+offset, value1, length );
			*(value+offset+length) = save;
		} else if ( func == JAG_FUNC_MIN ) {
			rc = checkColumnTypeMode( type );
			if ( rc == 2 || rc == 3 || rc == 4 ) { // bool, int, abaxint
				cnt1 = rayatol(value+offset, length);
				cnt2 = rayatol(pair.value.c_str()+offset, length);
				if ( cnt1 > cnt2 ) {
					memcpy(value+offset, pair.value.c_str()+offset, length);
				}
			} else if ( rc == 5 ) { // float, double
				value1 = raystrtold(value+offset, length);
				value2 = raystrtold(pair.value.c_str()+offset, length);
				if ( value1 > value2 ) {
					memcpy(value+offset, pair.value.c_str()+offset, length);
				}
			} else { // other types, string
				if ( memcmp(value+offset, pair.value.c_str()+offset, length) > 0 ) {
					memcpy(value+offset, pair.value.c_str()+offset, length);
				}
			}
		} else if ( func == JAG_FUNC_MAX ) {
			rc = checkColumnTypeMode( type );
			if ( rc == 2 || rc == 3 || rc == 4 ) { // bool, int, abaxint
				cnt1 = rayatol(value+offset, length);
				cnt2 = rayatol(pair.value.c_str()+offset, length);
				if ( cnt1 < cnt2 ) {
					memcpy(value+offset, pair.value.c_str()+offset, length);
				}
			} else if ( rc == 5 ) { // float, double
				value1 = raystrtold(value+offset, length);
				value2 = raystrtold(pair.value.c_str()+offset, length);
				if ( value1 < value2 ) {
					memcpy(value+offset, pair.value.c_str()+offset, length);
				}
			} else { // other types, string
				if ( memcmp(value+offset, pair.value.c_str()+offset, length) < 0 ) {
					memcpy(value+offset, pair.value.c_str()+offset, length);
				}
			}
		} else if ( func == JAG_FUNC_FIRST ) { // use original, no need to update
		} else if ( func == JAG_FUNC_LAST ) {
			memcpy(value+offset, pair.value.c_str()+offset, length);
		} else if ( func == JAG_FUNC_AVG ) {
			value1 = raystrtold(value+offset, length);
			value2 = raystrtold(pair.value.c_str()+offset, length);
			cnt1 = rayatol(toldpair.value.c_str()+(*(_srecord->columnVector))[_srecord->numKeys].offset,
				(*(_srecord->columnVector))[_srecord->numKeys].length );
			cnt2 = rayatol(pair.value.c_str()+(*(_srecord->columnVector))[_srecord->numKeys].offset,
				(*(_srecord->columnVector))[_srecord->numKeys].length );
			value3 = ( value1*cnt1 + value2*cnt2 ) / ( cnt1 + cnt2 );
			save = *(value+offset+length);
			jagsprintfLongDouble( 1, 1, value+offset, value3, length );
			*(value+offset+length) = save;
		} else {
			// nothing to do for other others including stddev
		}
	}
}

int JagMemDiskSortArray::setDataLimit( abaxint num )
{
	if ( 3 != _rwtype ) return 0; // not correct sequence calling for write
	if ( num <= 0 ) num = 0;
	
	_cnt = 0;
	_cntlimit = num;

	return 1;
}

int JagMemDiskSortArray::ignoreNumData( abaxint num )
{
	if ( 3 != _rwtype ) return 0; // not correct sequence calling for write
	if ( num <= 0 ) return 1;
	abaxint cnt = 0, rc;

	// first check if has aggstr
	if ( _aggstr.size() > 0 ) {
		_aggstr = "";
		return 0;
	}
	
	// if has jda
	if ( _jda ) {
		JagFixString rdata;
		while ( true ) {
			if ( !_isback ) rc = _jda->readit( rdata );
			else rc = _jda->backreadit( rdata );
			if ( !rc ) break;
			++cnt;
			if ( cnt >= num ) break;
		}
		return rc;
	}
	
	// if not has sort array, return
	if ( !_hassort ) return 0;
	
	// else if use memory arr
	if ( !_usedisk ) {
		if ( !_isback ) {
			while ( _mempos < _memarr->_arrlen ) {
				if ( _memarr->isNull(_mempos) ) {
					++_mempos;
					continue;
				}
				++cnt;
				++_mempos;
				if ( cnt >= num ) break;
			}
		} else {
			while ( _mempos >= 0 ) {
				if ( _memarr->isNull(_mempos) ) {
					--_mempos;
					continue;
				}
				++cnt;
				--_mempos;
				if ( cnt >= num ) break;
			}
		}
		if ( _mempos < 0 || _mempos >= _memarr->_arrlen ) return 0;
		return 1;
	}

	// else if use disk arr
	char *buf = (char*)jagmalloc(_kvlen+1);
	memset( buf, 0, _kvlen+1 );
	while ( true ) {
		if ( !_isback ) rc = _ntr->getNext( buf );
		else rc = _bntr->getNext( buf );
		if ( !rc ) break;
		++cnt;
		if ( cnt >= num ) break;
	}
	if ( buf ) free( buf );
	return rc;
}

/***
// return 0: failure or no more data ( end of read )
// return 1: continue get data
// return 2: get select hdr string
// return 3: get select count(*) string
// return 4: get aggregate data string
int JagMemDiskSortArray::get( JagFixString &str )
{
	if ( 3 != _rwtype || _rend ) return 0; // not correct sequence calling for write
	str = "";
	
	int rc;
	if ( _cntstr.size() > 0 ) {
		str = _cntstr;
		_cntstr = "";
		_rend = 1;
		return 3;
	}
	
	if ( _selhdr.size() > 0 ) {
		str = _selhdr;
		_selhdr = "";
		return 2;
	}	
	
	if ( _aggstr.size() > 0 ) {
		str = _aggstr;
		_aggstr = "";
		_rend = 1;
		return 4;
	}
	
	// if has jda
	if ( _jda ) {
		if ( !_isback ) rc = _jda->readit( str );
		else rc = _jda->backreadit( str );
		if ( rc ) {
			++_cnt;
			if ( _cnt > _cntlimit ) {
				rc = 0;
				_rend = 1;
			}
		} else {
			_rend = 1;
		}
		return rc;
	}
	
	// if not has sort array, return
	if ( !_hassort ) {
		_rend = 1;
		return 0;
	}
	
	char *buf = (char*)malloc(_kvlen+1);
	memset( buf, 0, _kvlen+1 );
	if ( !_usedisk ) {
		if ( !_isback ) {
			while ( _mempos < _memarr->_arrlen ) {
				if ( _memarr->isNull(_mempos) ) {
					++_mempos;
					continue;
				}
				break;
			}
		} else {
			while ( _mempos >= 0 ) {
				if ( _memarr->isNull(_mempos) ) {
					--_mempos;
					continue;
				}
				break;
			}
		}
		++_cnt;
		if ( _mempos < 0 || _mempos >= _memarr->_arrlen || _cnt > _cntlimit ) {
			if ( buf ) free ( buf );
			_rend = 1;
			return 0;
		}
		JagDBPair pair = _memarr->exactAt(_mempos);
		memcpy(buf, pair.key.c_str(), _keylen);
		memcpy(buf+_keylen, pair.value.c_str(), _vallen);
		str = JagFixString( buf, _kvlen );
		if ( !_isback ) ++_mempos;
		else --_mempos;
		if ( buf ) free ( buf );
		return 1;
	}
	
	if ( !_isback ) {
		rc = _ntr->getNext( buf );
	} else {
		rc = _bntr->getNext( buf );
	}
	
	if ( rc ) {
		++_cnt;
		if ( _cnt > _cntlimit ) {
			rc = 0;
			_rend = 1;
		} else {
			str = JagFixString( buf, _kvlen );
		}	
	} else {
		_rend = 1;
	}
	if ( buf ) free ( buf );
	return rc;
}
***/

int JagMemDiskSortArray::get( char *buf )
{
	if ( 3 != _rwtype ) return 0; // not correct sequence calling for write
	if ( !_usedisk ) {
		if ( !_isback ) {
			while ( _mempos < _memarr->_arrlen ) {
				if ( _memarr->isNull(_mempos) ) {
					++_mempos;
					continue;
				}
			break;
			}
		} else {
			while ( _mempos >= 0 ) {
				if ( _memarr->isNull(_mempos) ) {
					--_mempos;
					continue;
				}
			break;
			}
		}
		if ( _mempos < 0 || _mempos >= _memarr->_arrlen ) return 0;
		JagDBPair pair = _memarr->exactAt(_mempos);
		memcpy(buf, pair.key.c_str(), _keylen);
		memcpy(buf+_keylen, pair.value.c_str(), _vallen);
		if ( !_isback ) ++_mempos;
		else --_mempos;
		return 1;
	}

	if ( !_isback ) return _ntr->getNext( buf );
	else return _bntr->getNext( buf );
}
