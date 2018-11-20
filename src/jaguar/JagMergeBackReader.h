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
#ifndef _jag_merge_back_navig_h_
#define _jag_merge_back_navig_h_

#include <abax.h>
#include <JagMergeReaderBase.h>

class JagMergeBackReader : public JagMergeReaderBase
{
  public:
	JagMergeBackReader( JagVector<OnefileRange> &fRange, int veclen, int keylen, int vallen );
  	virtual ~JagMergeBackReader(); 	
  	virtual bool getNext( char *buf, abaxint &pos );
  	virtual bool getNext( char *buf );
	virtual bool setRestartPos();
	virtual bool getRestartPos();
	virtual void setClearRestartPosFlag();

  protected:
  	/***
	int _veclen;
	int _endcnt;
	abaxint KEYLEN;
	abaxint VALLEN;
	abaxint KEYVALLEN;
	int *_goNext;
	char *_buf;
	bool _setRestartPos;
	***/

	JagBuffBackReaderPtr *_vec;
};


#endif

