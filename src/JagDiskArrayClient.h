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
#ifndef _jag_disk_arr_client_h_
#define _jag_disk_arr_client_h_

#include <JagDiskArrayBase.h>

class JagDiskArrayClient : public JagDiskArrayBase
{
	public:
	
		JagDiskArrayClient( const Jstr &fpathname, 
							const JagSchemaRecord *record, bool dropClean=false, abaxint length=32 );
		virtual ~JagDiskArrayClient();

		virtual void drop();
		virtual void buildInitIndex( bool force=false );	
		virtual void init( abaxint length, bool buildBlockIndex );
		virtual int _insertData( JagDBPair &pair, abaxint *retindex, int &insertCode, bool doFirstRedist, JagDBPair &retpair );
		virtual int reSize( bool force=false, abaxint newarrlen=-1 );
		// virtual int reSize( bool force=false, abaxint newarrlen=-1, bool doFirstDist=true, bool sepThrdSelfCall=false );
		virtual void reSizeLocal( );

		// client own methods
		int insertSync(  JagDBPair &pair );
		int needResize();
		abaxint flushInsertBufferSync();

		int _dropClean;

};

#endif
