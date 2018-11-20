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
#ifndef _jag_buff_writer_h_
#define _jag_buff_writer_h_

#include <abax.h>
#include <JagCfg.h>
#include <JagDef.h>

class JDFS;

class JagBuffWriter
{
	public:

		JagBuffWriter( JDFS *jdfs, int keyvallen, abaxint headoffset = JAG_ARJAG_FILE_HEAD, abaxint bufferSize=-1 ); 
		~JagBuffWriter();
		
		// void resetKVLEN( int newkvlen );
		void writeit( abaxint pos, const char *keyvalbuf, abaxint kvlen );
		void flushBuffer();

	protected:

		// static const abaxint SUPERBLOCK = 1024*JagCfg::_BLOCK; 
		abaxint SUPERBLOCK;

		char	*_superbuf; 

		abaxint _lastBlock;
		abaxint _relpos;
		abaxint  KVLEN;
		abaxint SUPERBLOCKLEN;
		int  _fd;
		abaxint _headoffset;
		JDFS  *_jdfs;

};


#endif
