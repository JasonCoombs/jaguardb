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
#ifndef _jag_index_string_h_
#define _jag_index_string_h_

#include <abax.h>

class JagIndexString
{
  public:
	JagIndexString();
	~JagIndexString();

	abaxint length() const { return _length; }
	abaxint size() const { return _length; }
	abaxint capacity() const { return _capacity; }
	abaxint tokens() const { return _tokens; }
	const char  *c_str() const { return _buf; }
	void destroy();
	void init();
	void reset();

	void add( const AbaxFixString &key, abaxint i, int isnew, int force );
  	
  protected:
	abaxint  _length;
	abaxint  _capacity;
	abaxint  _tokens;
	char  *_buf;
	abaxint  _lasti;

	// temp vars
	char  _tmp[32];
	char  _tmp2[32];
	abaxint   _tmplen;
	abaxint  _tokenLen;

};

#endif

