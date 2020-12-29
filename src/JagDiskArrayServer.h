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
#ifndef _jag_disk_arrjagserv_h_
#define _jag_disk_arrjagserv_h_

#include <stdio.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <string.h>
#include <limits.h>
#include <atomic>
#include <vector>

#include <abax.h>
#include <JagDiskArrayBase.h>
#include <JagDBMap.h>

class JagDataAggregate;

////////////////////////////////////////// disk array class ///////////////////////////////////

class JagDiskArrayServer : public JagDiskArrayBase
{
	public:
		// JagDiskArrayServer public methods
		JagDiskArrayServer( const JagDBServer *servobj, JagDiskArrayFamily *fam, int index, const Jstr &fpathname, 
							const JagSchemaRecord *record, jagint length, bool buildInitIndex );
		virtual ~JagDiskArrayServer();
			
		// bool remove( const JagDBPair &pair ) { jagint idx; return removeData( pair, &idx ); }
		bool remove( const JagDBPair &pair );
		// jagint removeMatchKey( const char *str, int strlen );
		bool get( JagDBPair &pair ) { jagint idx; return get( pair, idx ); }		
		bool set( const JagDBPair &pair ) { jagint idx; return set( pair, idx ); }
		bool exist( JagDBPair &pair ) { jagint idx; return exist( pair, &idx, pair ); }		
		
		bool exist( const JagDBPair &pair, jagint *index, JagDBPair &retpair );		
		bool exist( const JagDBPair &pair, JagDBPair &retpair );		
		bool get( JagDBPair &pair, jagint &index );
		bool getWithRange( JagDBPair &pair, jagint &index );
		bool set( const JagDBPair &pair, jagint &index );
		/***
		bool setWithRange(  const JagRequest &req, JagDBPair &pair, const char *buffers[], bool uniqueAndHasValueCol, 
							ExprElementNode *root, 
							const JagParseParam *parseParam, int numKeys, const JagSchemaAttribute *schAttr, 
							jagint setposlist[], JagDBPair &retpair );	
							***/
		// bool getLimitStartPos( jagint &startlen, jagint limitstart, jagint &soffset );

		void flushBlockIndexToDisk();
		void removeBlockIndexIndDisk();
		//void separateResizeForce();
		//void separateMergeForce();
		void print( jagint start=-1, jagint end=-1, jagint limit=-1 );
		int	orderCheckOK(); // check if the order in diskarray is correct	
		int buildInitIndexFromIdxFile();
		// Jstr getListKeys();

		jagint waitCopyAndInsertBufferAndClean();
		JagDiskArrayServer* flushInsertBufferToFile( );
		jagint flushInsertBufferOneByOne();
		//void  appendNewSimpFile();

		//virtual void dropFile();
		virtual void buildInitIndex( bool force=false );
		virtual void init( jagint length, bool buildBlockIndex );
		//virtual int _insertData( JagDBPair &pair, int &insertCode, bool doFirstRedist, JagDBPair &retpair );
		//virtual int reSize( bool force=false, jagint newarrlen=-1 );
		//virtual void reSizeLocal();

		//bool checkInitResizeCondition();
		// void recountElements();
		//bool reSizeCompress();
		
		bool checkFileOrder( const JagRequest &req );
		jagint orderRepair( const JagRequest &req );
		void updateCorrectDataBlockIndex( char *buf, jagint pos, JagDBPair &tpair, jagint &lastBlock );
		//static void *monitorCommandsMem( void *ptr );
		//virtual int pushToBuffer( JagDBPair &pair );
		float computeMergeCost( const JagDBMap *pairmap, jagint sequentialReadSpeed, 
								jagint sequentialWriteSpeed, JagVector<JagMergeSeg> &vec );

		/***
		static bool checkSetPairCondition( const JagDBServer *servobj, const JagRequest &req, const JagDBPair &pair, char *buffers[], 
									bool uniqueAndHasValueCol, ExprElementNode *root, 
									const JagParseParam *parseParam, int numKeys, const JagSchemaAttribute *schAttr, 
									jagint  KLEN, jagint VLEN,
									jagint setposlist[], JagDBPair &retpair );
									***/
		
	protected:
		// bool removeData( const JagDBPair &pair, jagint *retindex );
		int removeFromRange( const JagDBPair &pair, jagint *retindex );
		int removeFromAll( const JagDBPair &pair, jagint *retindex );		
		//int needResize();
		//static void *separateResizeStatic( void *ptr );
};

#endif
