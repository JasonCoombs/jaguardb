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
#ifndef _jag_disk_arjagserv_h_
#define _jag_disk_arjagserv_h_

#include <stdio.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <string.h>
#include <limits.h>
#include <atomic>

#include <abax.h>
#include <JagDiskArrayBase.h>

class JagDataAggregate;

////////////////////////////////////////// disk array class ///////////////////////////////////

class JagDiskArrayServer : public JagDiskArrayBase
{
	public:
		// JagDiskArrayServer public methods
		JagDiskArrayServer( const JagDBServer *servobj, const Jstr &fpathname, 
							const JagSchemaRecord *record, 
							bool buildInitIndex=true, abaxint length=32, bool noMonitor=false, bool isLastOne=true );
		JagDiskArrayServer( const JagDBServer *servobj, const Jstr &fpathname, 
							const JagSchemaRecord *record, 
							const Jstr &pdbobj, JagDBPair &minpair, JagDBPair &maxpair );
		virtual ~JagDiskArrayServer();
			
		inline bool remove( const JagDBPair &pair ) { abaxint idx; return removeData( pair, &idx ); }
		abaxint removeMatchKey( const char *str, int strlen );
		inline bool checkFirstResize() { return _firstResize; }
		inline bool get( JagDBPair &pair ) { abaxint idx; return get( pair, idx ); }		
		inline bool set( JagDBPair &pair ) { abaxint idx; return set( pair, idx ); }
		inline bool exist( JagDBPair &pair ) { abaxint idx; return exist( pair, &idx, pair ); }		
		
		bool exist( const JagDBPair &pair, abaxint *index, JagDBPair &retpair );		
		bool get( JagDBPair &pair, abaxint &index );
		bool getWithRange( JagDBPair &pair, abaxint &index );
		bool set( JagDBPair &pair, abaxint &index );
		bool setWithRange(  const JagRequest &req, JagDBPair &pair, const char *buffers[], bool uniqueAndHasValueCol, 
							ExprElementNode *root, 
							JagParseParam *parseParam, int numKeys, const JagSchemaAttribute *schAttr, 
							abaxint setposlist[], JagDBPair &retpair );	
		bool getLimitStartPos( abaxint &startlen, abaxint limitstart, abaxint &soffset );

		void flushBlockIndexToDisk();
		void removeBlockIndexIndDisk();
		void separateResizeForce();
		void separateMergeForce();
		void print( abaxint start=-1, abaxint end=-1, abaxint limit=-1 );
		int	orderCheckOK(); // check if the order in diskarray is correct	
		int buildInitIndexFromIdxFile();
		Jstr getListKeys();

		virtual void drop();
		virtual void buildInitIndex( bool force=false );
		virtual void init( abaxint length, bool buildBlockIndex, bool isLastOne );
		virtual int _insertData( JagDBPair &pair, abaxint *retindex, int &insertCode, bool doFirstRedist, JagDBPair &retpair );
		virtual int reSize( bool force=false, abaxint newarrlen=-1 );
		virtual int mergeResize( int mergeMode, abaxint mergeElements, char *mbuf, const char *sbuf, JagBuffReader *mbr );
		virtual void reSizeLocal();

		bool checkInitResizeCondition( bool isLastOne );
		void recountElements();

		bool reSizeCompress();
		
		bool checkFileOrder( const JagRequest &req );
		abaxint orderRepair( const JagRequest &req );
		void updateCorrectDataBlockIndex( char *buf, abaxint pos, JagDBPair &tpair, abaxint &lastBlock );
		JagDiskArrayServer *_darrnew;
		
	protected:
		// JagDiskArrayServer protected methods
		bool checkSetPairCondition( const JagRequest &req, JagDBPair &pair, char *buffers[], 
									bool uniqueAndHasValueCol, ExprElementNode *root, 
									JagParseParam *parseParam, int numKeys, const JagSchemaAttribute *schAttr, 
									abaxint setposlist[], JagDBPair &retpair );
		bool removeData( const JagDBPair &pair, abaxint *retindex );
		int removeFromRange( const JagDBPair &pair, abaxint *retindex );
		int removeFromAll( const JagDBPair &pair, abaxint *retindex );		
		void mergeDarrnew( abaxint &cnt, abaxint &rsz );
		int needResize();
		static void *separateResizeStatic( void *ptr );
};

#endif
