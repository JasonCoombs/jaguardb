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
#ifndef _jag_strsplit_h_
#define _jag_strsplit_h_

#include <abax.h>

class JagStrSplit
{
	public:   

		JagStrSplit();
		JagStrSplit(const AbaxDataString& str, char sep = ' ', bool ignoreregion=false );
		JagStrSplit(const char *str, char sep = ' ', bool ignoreregion=false );
		
		void init(const char *str, char sep=' ', bool ignoreregion=false );

		void destroy();
		~JagStrSplit();

	    const AbaxDataString& operator[](int i ) const;
	    AbaxDataString& operator[](int i );
		AbaxDataString& last() const;
		abaxint length() const;
		abaxint slength() const;
		bool  exists(const AbaxDataString &token) const;
		bool  contains(const AbaxDataString &token, AbaxDataString &rec ) const;
		void	print() const;
		void	printStr() const;
		void  shift();
		void  back();
		const char *c_str() const;
		void  pointTo( const char* str );

	private:
		AbaxDataString *list_;
		abaxint length_;
		char sep_;
		int  start_;
		AbaxDataString _NULL;
		const char* pdata_;
};

#endif

