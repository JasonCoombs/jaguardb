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
#include<JagSchemaRecord.h>
#include<JagBuffBackReader.h>
#include<JagBuffReader.h>
#include<JagArray.h>
#include<JagMutex.h>

JagMemDiskSortArray::JagMemDiskSortArray()
{
	_memarr = NULL;
	//_diskarr = NULL;
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
	
	/**
	if ( _diskarr ) {
		delete _diskarr;
		_diskarr = NULL;
	}
	**/
	
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
	prt(("s40018 JagMemDiskSortArray::beginWrite parseRecord\n"));
	prt(("_diskhdr=[%s]\n", _diskhdr.c_str() ));

	return 1;
}
int JagMemDiskSortArray::endWrite()
{
	if ( 1 != _rwtype ) return 0;
	if ( _usedisk ) {
		// _diskarr->flushInsertBufferSync();
		// _diskarr->flushInsertBuffer();
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
		/***
		if ( !_isback ) _ntr = new JagBuffReader( _diskarr, _diskarr->_arrlen, _diskarr->KEYLEN, _diskarr->VALLEN, 0 );
		else _bntr = new JagBuffBackReader( _diskarr, _diskarr->_arrlen, _diskarr->KEYLEN, _diskarr->VALLEN, _diskarr->_arrlen );
		***/
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
		jagint curbytes = _memarr->_arrlen*(_kvlen);
		if ( curbytes < _memlimit ) {
			_memarr->insert( pair );
		} else {
			// memory is used up, transfer data to disk
			/***
			if ( _diskarr ) {
				delete _diskarr;
				_diskarr = NULL;
			}
			Jstr fpath = jaguarHome() + "/tmp/" + longToStr(THREADID) + "_" + _opstr;
			_diskarr = new JagDiskArrayClient( fpath, _srecord, true );
			
			JagDBPair mpair;
			// read all buffer and flush to disk
			for ( jagint i = 0; i < _memarr->_arrlen; ++i ) {
				if ( _memarr->isNull( i ) ) continue;
				mpair = _memarr->at( i );
				//_diskarr->insertSync( mpair );
				_diskarr->insert( mpair );
			}
			//_diskarr->insertSync( pair );
			_diskarr->insert( pair );
			_usedisk = true;
			if ( _memarr ) {
				delete _memarr;
				_memarr = NULL;
			}
			***/
		}
	} else {
		//_diskarr->insertSync( pair );
		// _diskarr->insert( pair );
	}
	return 1;
}

// return 0: no more data accepted
// memory of value can be changed for this method
int JagMemDiskSortArray::groupByUpdate( const JagDBPair &pair )
{
	if ( 1 != _rwtype ) return 0; // not correct sequence calling for write

	prt(("s23301 pair:\n"));
	
	int rc;
	JagDBPair oldpair;
	prt(("s50031 groupByUpdate _usedisk=%d\n", _usedisk ));

	if ( !_usedisk ) {
		jagint curbytes = _memarr->_arrlen * _kvlen;
		if ( curbytes < _memlimit ) {
			oldpair.key = pair.key;
			rc = _memarr->get( oldpair ); // existing value
			prt(("s43103 get oldpair rc=%d\n", rc ));
			if ( !rc ) {
				prt(("s44022  _memarr->insert\n"));
				_memarr->insert( pair );
			} else {
				groupByValueCalculation( pair, oldpair );
				prt(("s44023  _memarr->set\n"));
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
	jagint cnt, cnt1, cnt2;
	abaxdouble value1 = 0.0, value2 = 0.0, value3 = 0.0;
	JagDBPair toldpair = oldpair;
	char *value = (char*)oldpair.value.c_str();
	keylen = _srecord->keyLength;

	prt(("s34013 groupByValueCalculation keylen=%d  _srecord->numKeys=%d\n", keylen,  _srecord->numKeys ));
	prt(("s32031 pair oldpair:\n"));

	for ( int i = _srecord->numKeys; i < _srecord->columnVector->size(); ++i ) {
		type = (*(_srecord->columnVector))[i].type;
		func = (*(_srecord->columnVector))[i].func;
		offset = (*(_srecord->columnVector))[i].offset - keylen;
		length = (*(_srecord->columnVector))[i].length;

		prt(("s53102 i=%d func=%d offset=%d length=%d func=%d\n", i, func, offset, length, func ));

		if ( func == JAG_FUNC_COUNT ) {
			prt(("s20031 JAG_FUNC_COUNT\n"));
			cnt1 = rayatol(value+offset, length);
			cnt2 = rayatol(pair.value.c_str()+offset, length);
			cnt = cnt1 + cnt2;
			value1 = (abaxdouble)cnt;
			save = *(value+offset+length);
			jagsprintfLongDouble( 1, 1, value+offset, value1, length );
			*(value+offset+length) = save;
			prt(("s32103 count final value=[%s] value+offset=[%s]\n", value, value+offset ));
		} else if ( func == JAG_FUNC_SUM ) {
			prt(("s20032 JAG_FUNC_SUM\n"));
			value1 = raystrtold(value+offset, length);
			value2 = raystrtold(pair.value.c_str()+offset, length);
			prt(("s32012 value1=%.3f value2=%.3f\n", value1, value2 ));
			value1 += value2;
			save = *(value+offset+length);
			jagsprintfLongDouble( 1, 1, value+offset, value1, length );
			*(value+offset+length) = save;
			prt(("s32203 sum final value=[%s] value+offset=[%s]\n", value, value+offset ));
		} else if ( func == JAG_FUNC_MIN ) {
			rc = checkColumnTypeMode( type );
			if ( rc == 2 || rc == 3 || rc == 4 ) { // bool, int, jagint
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
			if ( rc == 2 || rc == 3 || rc == 4 ) { // bool, int, jagint
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

			int soffset = (*(_srecord->columnVector))[_srecord->numKeys].offset;
			int slen = (*(_srecord->columnVector))[_srecord->numKeys].length;

			cnt1 = rayatol(toldpair.value.c_str()+soffset, slen ); 
			cnt2 = rayatol(pair.value.c_str()+soffset, slen );

			value3 = ( value1*cnt1 + value2*cnt2 ) / ( cnt1 + cnt2 );
			save = *(value+offset+length);
			jagsprintfLongDouble( 1, 1, value+offset, value3, length );
			*(value+offset+length) = save;
			prt(("s31203 avg final value=[%s] value+offset=[%s]\n", value, value+offset ));
		} else {
			// nothing to do for other others including stddev
			prt(("s50031 no func=%d match\n", func ));
		}
	}

	prt(("s32035 pair oldpair:\n"));
}

int JagMemDiskSortArray::setDataLimit( jagint num )
{
	if ( 3 != _rwtype ) return 0; // not correct sequence calling for write
	if ( num <= 0 ) num = 0;
	
	_cnt = 0;
	_cntlimit = num;

	return 1;
}

int JagMemDiskSortArray::ignoreNumData( jagint num )
{
	if ( 3 != _rwtype ) return 0; // not correct sequence calling for write
	if ( num <= 0 ) return 1;
	jagint cnt = 0, rc;

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
		JagDBPair pair = _memarr->at(_mempos);
		memcpy(buf, pair.key.c_str(), _keylen);
		memcpy(buf+_keylen, pair.value.c_str(), _vallen);
		if ( !_isback ) ++_mempos;
		else --_mempos;
		return 1;
	}

	if ( !_isback ) return _ntr->getNext( buf );
	else return _bntr->getNext( buf );
}
