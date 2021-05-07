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

#include <malloc.h>
#include <JagDiskArrayBase.h>
#include <JagHashLock.h>
#include <JagBuffReader.h>
#include <JagBuffBackReader.h>
#include <JagSingleBuffWriter.h>
#include <JagDiskArrayFamily.h>
#include <JagCompFile.h>

// ctor and dtors
// ctor
// User by server only
JagDiskArrayBase::JagDiskArrayBase( const JagDBServer *servobj,  JagDiskArrayFamily *fam, const Jstr &filePathName, 
									const JagSchemaRecord *record, int index ) 
								: _schemaRecord(record)
{
	_servobj = servobj;
	_family = fam;
	_pathname = filePathName;
	_fulls = 0;
	_index = index; // i-th file in family
	_partials = 0;
	_reads = _writes = _dupwrites = _upserts = 0;
	_insdircnt = _insmrgcnt = 0;
	_lastSyncTime = 0;
	_lastSyncOneTime = 0;
	_isClient = 0;
	_isFlushing = 0;
	_nthserv = 0;
	_numservs = 1;
	_jdfs = nullptr;
}

// client use only
JagDiskArrayBase::JagDiskArrayBase( const Jstr &filePathName, const JagSchemaRecord *record ) 
    :  _schemaRecord(record)
{	
	_servobj = NULL;
	_family = NULL;
	_pathname = filePathName;
	_fulls = 0;
	_partials = 0;
	_reads = _writes = _dupwrites = _upserts = 0;
	_insdircnt = _insmrgcnt = 0;
	_lastSyncTime = 0;
	_lastSyncOneTime = 0;
	_isClient = 1;
	_isFlushing = 0;
	_nthserv = 0;
	_numservs = 1;
}

// dtor
JagDiskArrayBase::~JagDiskArrayBase()
{
	destroy();
}

void JagDiskArrayBase::destroy()
{
	if ( _jdfs ) {
		delete _jdfs;
	}
	_jdfs = NULL;
	
}

// simple get attributes methods
// get DBPair for KV both or key only
void JagDiskArrayBase::_getPair( char buffer[], int keylength, int vallength, JagDBPair &pair, bool keyonly ) const 
{
	if (buffer[0] != '\0') {
		if ( keyonly ) {
			pair = JagDBPair( buffer, keylength );
		} else {
			pair = JagDBPair( buffer, keylength, buffer+keylength, vallength );
		}
	} else {
		JagDBPair temp_pair;
		pair = temp_pair;
	}
}

jagint JagDiskArrayBase::size() const 
{ 
	return _jdfs->getCompf()->size(); // bytes
}

bool JagDiskArrayBase::getFirstLast( const JagDBPair &pair, jagint &first, jagint &last )
{
	//prt(("s331115 JagDiskArrayBase::getFirstLast pair=[%s]\n", pair.key.c_str() ));
	if ( *pair.key.c_str() == NBT ) {
		first = 0;
		last = first + JagCfg::_BLOCK - 1;
		return 1;
	}

	if ( 0==memcmp(pair.key.c_str(), _maxKey, _KLEN ) ) {
		first = this->size()/_KVLEN;
		last = first + JagCfg::_BLOCK - 1;
		return 1;
	}

	JagCompFile *compf = getCompf();

	bool rc;
	rc = compf->findFirstLast( pair, first, last );
	if ( ! rc ) {
		return false;
	}

	last = first + JagCfg::_BLOCK - 1;
	return true;
}


// equal element or predecessor of pair
bool JagDiskArrayBase::findPred( const JagDBPair &pair, jagint *index, jagint first, jagint last, 
								 JagDBPair &retpair, char *diskbuf )
{
	//prt(("s412112 findPred() pair=[%s]  _path=[%s] kvlen=%d first=%d\n", pair.key.c_str(),  _pathname.c_str(), _KVLEN, first ));
	bool found = 0;
	*index = -1;

    JagDBPair arr[JagCfg::_BLOCK];
    JagFixString key, val;

   	char *ldiskbuf = (char*)jagmalloc(JagCfg::_BLOCK*(_KLEN+1)+1);
   	memset( ldiskbuf, 0, JagCfg::_BLOCK*(_KLEN+1) + 1 );
   	for (int i = 0; i < JagCfg::_BLOCK; ++i ) {
   		raypread(_jdfs, ldiskbuf+i*_KLEN, _KLEN, (first+i)*_KVLEN);
       	arr[i].point( ldiskbuf+i*_KLEN, _KLEN );
   	}

   	found = binSearchPred( pair, index, arr, JagCfg::_BLOCK, 0, JagCfg::_BLOCK-1 );

	char *kvbuf = (char*)jagmalloc(_KVLEN+1);
   	memset( kvbuf, 0, _KVLEN+1 );
   	if ( *index != -1 ) {
		raypread(_jdfs, kvbuf, _KVLEN, (first+*index)*_KVLEN );
		//prt(("s222339 index not -1\n"));
	} else {
		raypread(_jdfs, kvbuf, _KVLEN, first*_KVLEN );
		//prt(("s222339 index == -1\n"));
	}

	retpair = JagDBPair( kvbuf, _KLEN, kvbuf+_KLEN, _VLEN );
   	*index += first;
	free( kvbuf );
	free( ldiskbuf );

	//prt(("s233338 pred pair=[%s] index=%d found=%d\n", retpair.key.c_str(), *index, found ));
   	return found;
}

jagint JagDiskArrayBase::getRegionElements( jagint first, jagint length )
{
	JagCompFile *compf = getCompf();
	jagint rangeElements = 0;
	first /= JAG_BLOCK_SIZE;
	for ( jagint i = 0; i < length; ++i ) {
		rangeElements += compf->getPartElements( first+i );
	}
	return rangeElements;
}

Jstr JagDiskArrayBase::jdbPath( const Jstr &jdbhome, const Jstr &db, const Jstr &tab )
{
	Jstr fpath = jdbhome + "/" + db + "/" + tab;
	return fpath;
}

Jstr JagDiskArrayBase::jdbPathName( const Jstr &jdbhome, const Jstr &db, const Jstr &tab )
{
	Jstr fpath = jdbhome + "/" + db + "/" + tab + "/" + tab + ".jdb";
	return fpath;
}

void JagDiskArrayBase::logInfo( jagint t1, jagint t2, jagint cnt, const JagDiskArrayBase *jda )
{
	if ( t1 > 1000 ) { // in sec
		t1 /= 1000;
		raydebug(stdout, JAG_LOG_HIGH, "s6028 flib %s %d wt=%l s flsh=%l ms %l/%l/%l/%l/%l/%l\n",
			jda->_dbobj.c_str(), cnt, t1, t2, jda->_writes, jda->_upserts, jda->_dupwrites, 
			jda->_reads, jda->_insmrgcnt, jda->_insdircnt );
	} else if ( t1 > 60000 ) { // in min
		t1 /= 60000;
		raydebug(stdout, JAG_LOG_HIGH, "s6028 flib %s %d wt=%l m flsh=%l ms %l/%l/%l/%l/%l/%l\n",
			jda->_dbobj.c_str(), cnt, t1, t2, jda->_writes, jda->_upserts, jda->_dupwrites, 
			jda->_reads, jda->_insmrgcnt, jda->_insdircnt );
	} else if ( t1 > 3600000 ) { // in hour
		t1 /= 3600000;
		raydebug(stdout, JAG_LOG_HIGH, "s6028 flib %s %d wt=%l h flsh=%l ms %l/%l/%l/%l/%l/%l\n",
			jda->_dbobj.c_str(), cnt, t1, t2, jda->_writes, jda->_upserts, jda->_dupwrites, 
			jda->_reads, jda->_insmrgcnt, jda->_insdircnt );
	} else if ( t1 > 86400000 ) { // in day
		t1 /= 86400000;
		raydebug(stdout, JAG_LOG_HIGH, "s6028 flib %s %d wt=%l d flsh=%l ms %l/%l/%l/%l/%l/%l\n",
			jda->_dbobj.c_str(), cnt, t1, t2, jda->_writes, jda->_upserts, jda->_dupwrites, 
			jda->_reads, jda->_insmrgcnt, jda->_insdircnt );
	} else if ( t1 > 2592000000 ) { // in month
		t1 /= 2592000000;
		raydebug(stdout, JAG_LOG_HIGH, "s6028 flib %s %d wt=%l M flsh=%l ms %l/%l/%l/%l/%l/%l\n",
			jda->_dbobj.c_str(), cnt, t1, t2, jda->_writes, jda->_upserts, jda->_dupwrites, 
			jda->_reads, jda->_insmrgcnt, jda->_insdircnt );
	} else if ( t1 > 31104000000 ) { // in year
		t1 /= 31104000000;
		raydebug(stdout, JAG_LOG_HIGH, "s6028 flib %s %d wt=%l Y flsh=%l ms %l/%l/%l/%l/%l/%l\n",
			jda->_dbobj.c_str(), cnt, t1, t2, jda->_writes, jda->_upserts, jda->_dupwrites, 
			jda->_reads, jda->_insmrgcnt, jda->_insdircnt );
	} else { // in millisec
		raydebug(stdout, JAG_LOG_HIGH, "s6028 flib %s %d wt=%l ms flsh=%l ms %l/%l/%l/%l/%l/%l\n",
			jda->_dbobj.c_str(), cnt, t1, t2, jda->_writes, jda->_upserts, jda->_dupwrites, 
			jda->_reads, jda->_insmrgcnt, jda->_insdircnt );
	}
}

// returns # of elements in new file
jagint JagDiskArrayBase::mergeBufferToFile( const JagDBMap *pairmap, const JagVector<JagMergeSeg> &vec )
{
	jagint bytes = _compf->mergeBufferToFile( pairmap, vec );
	return bytes/_KVLEN;;
}


// returns # of elements flushed
jagint JagDiskArrayBase::flushBufferToNewFile( const JagDBMap *pairmap )
{
	jagint cnt = _compf->flushBufferToNewSimpFile( pairmap );
	prt(("s1007374 added cnt=%d\n", cnt ));
	return cnt;
}


// check conditon
bool JagDiskArrayBase::checkSetPairCondition( const JagDBServer *servobj, const JagRequest &req, const JagDBPair &pair, char *buffers[], 
												bool uniqueAndHasValueCol, 
												ExprElementNode *root, const JagParseParam *parseParam, int numKeys, 
												const JagSchemaAttribute *schAttr, 
												jagint KLEN, jagint VLEN,
												jagint setposlist[], JagDBPair &retpair )
{
	bool 			rc, needInit = true;
	ExprElementNode *updroot;
	Jstr 			errmsg;
	JagFixString 	strres;
	int 			typeMode = 0, treelength = 0;
	Jstr 			treetype = " ";
	const JagSchemaAttribute* attrs[1];

	jagint KVLEN = KLEN + VLEN;
	char 		*tbuf = (char*)jagmalloc(KVLEN+1);
	memset( tbuf, 0, KVLEN+1 );
	attrs[0] = schAttr;
	
	memcpy(buffers[0], pair.key.c_str(), KLEN);
	memcpy(buffers[0]+KLEN, pair.value.c_str(), VLEN);
	memcpy(tbuf, pair.key.c_str(), KLEN);
	memcpy(tbuf+KLEN, pair.value.c_str(), VLEN);
	dbNaturalFormatExchange( buffers[0], numKeys, schAttr ); // db format -> natural format
	dbNaturalFormatExchange( tbuf, numKeys, schAttr ); // db format -> natural format

	if ( !uniqueAndHasValueCol || 
		root->checkFuncValid( NULL, NULL, attrs, (const char **)buffers, strres, typeMode, treetype, treelength, needInit, 0, 0 ) == 1 ) {
		for ( int i = 0; i < parseParam->updSetVec.size(); ++i ) {
			updroot = parseParam->updSetVec[i].tree->getRoot();
			needInit = true;
			if ( updroot->checkFuncValid( NULL, NULL, attrs, (const char **)buffers, strres, typeMode, 
										  treetype, treelength, needInit, 0, 0 ) == 1 ) {
				memset(tbuf+schAttr[setposlist[i]].offset, 0, schAttr[setposlist[i]].length);	
				rc = formatOneCol( req.session->timediff, servobj->servtimediff, tbuf, strres.c_str(), errmsg, 
								   parseParam->updSetVec[i].colName, schAttr[setposlist[i]].offset, 
								   schAttr[setposlist[i]].length, schAttr[setposlist[i]].sig, schAttr[setposlist[i]].type );
				if ( !rc ) {
					free( tbuf );
					return false;
				}
			} else {
				free( tbuf );
				return false;
			}
		}
		dbNaturalFormatExchange( buffers[0], numKeys, schAttr ); // natural format -> db format
		dbNaturalFormatExchange( tbuf, numKeys, schAttr ); // natural format -> db format
		retpair = JagDBPair( tbuf, KLEN, tbuf+KLEN, VLEN );
	} else {
		free( tbuf );
		return false;
	}

	free( tbuf );
	return true;
}
