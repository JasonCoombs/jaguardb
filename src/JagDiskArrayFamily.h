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
#ifndef _jag_disk_arjag_family_h_
#define _jag_disk_arjag_family_h_

#include <stdio.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <string.h>
#include <limits.h>
#include <time.h>
#include <atomic>

#include <abax.h>
#include <JagUtil.h>
#include <JagDef.h>
#include <JagDBPair.h>
#include <JagVector.h>
#include <JagRequest.h>
#include <JagParseExpr.h>
#include <JagParseParam.h>
#include <JagSchemaRecord.h>
#include <JagMergeReader.h>
#include <JagMergeBackReader.h>
#include <JagFamilyKeyChecker.h>

class JagDiskArrayFamily;

class SepThrdDACompressPass
{
  public:
	JagDiskArrayFamily *darrFamily;
};

class JagDiskArrayFamily
{
  public:
	JagDiskArrayFamily( const JagDBServer *servobj, const AbaxDataString &fpathname, const JagSchemaRecord *record, 
		bool buildInitIndex=true, abaxint length=32, bool noMonitor=false );
	~JagDiskArrayFamily();

	// set newest darrs
	int insert( JagDBPair &pair, int &insertCode, bool doFirstRedist, bool direct, JagDBPair &retpair, 
				 bool noDupPrint=false );
	abaxint getElements( abaxint &keyCheckerCnt );
	bool isFlushing();
	bool hasdarrnew();
	bool remove( const JagDBPair &pair ); // set all darrs
	bool exist( JagDBPair &pair ); // check from oldest darr to newest darr
	bool get( JagDBPair &pair ); // get from oldest darr to newest darr
	bool set( JagDBPair &pair ); // set all darrs
	bool getWithRange( JagDBPair &pair, abaxint &index ); // set all darrs
	bool setWithRange( const JagRequest &req, JagDBPair &pair, const char *buffers[], bool uniqueAndHasValueCol, 
		ExprElementNode *root, JagParseParam *parseParam, int numKeys, const JagSchemaAttribute *schAttr, 
		abaxint setposlist[], JagDBPair &retpair ); // set all darrs
	void drop();
	void flushBlockIndexToDisk();
	void copyAndInsertBufferAndClean();
	void forceSleepTime( int time );

	// may need to be imporved later for join
	abaxint setFamilyRead( JagMergeReader *&nts, const char *minbuf=NULL, const char *maxbuf=NULL ); 
	abaxint setFamilyReadPartial( JagMergeReader *&nts, const char *minbuf=NULL, const char *maxbuf=NULL, 
		abaxint spos=0, abaxint epos=0, abaxint mmax=128 ); 
	abaxint setFamilyReadBackPartial( JagMergeBackReader *&nts, const char *minbuf=NULL, const char *maxbuf=NULL, 
		abaxint spos=0, abaxint epos=0, abaxint mmax=128 ); 
	void debugJDBFile( int flag, abaxint limit, abaxint hold, abaxint instart, abaxint inend );

	bool checkFileOrder( const JagRequest &req );
	abaxint orderRepair( const JagRequest &req );
	
	static void *separateDACompressStatic( void *ptr );
	int buildInitKeyCheckerFromSigFile();
	void flushKeyCheckerString();
	abaxint addKeyCheckerFromJDB( const JagDiskArrayServer *ldarr, int activepos );
	
	// data members may need to be called outside of diskarray
	int 		_filecnt;
	int 		_darrlistlen;
	int			_activepos;
	int         _KLEN;
	int         _VLEN;
	bool		_needCompressThrd;
	abaxint		_compthrdhold;
	AbaxDataString 		_tablepath;
	AbaxDataString 		_pathname;
	AbaxDataString 		_sfilepath;
	AbaxDataString 		_objname;
	const JagSchemaRecord 	*_schemaRecord;
	JagVector<JagDiskArrayServer*> _darrlist;
	JagFamilyKeyChecker		*_keyChecker;
	JagReadWriteLock    *_lock;
	JagDBServer			*_servobj;
	std::atomic<bool>	_isDestroyed;
	std::atomic<bool>	_hasCompressThrd;
	std::atomic<abaxint> _insdelcnt;

};

#endif
