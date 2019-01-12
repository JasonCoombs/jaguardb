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
#ifndef _jag_hash_str_str_h_
#define _jag_hash_str_str_h_

#include <abax.h>
#include <JagVector.h>
#include <jaghashtable.h>
class JagHashStrStr
{
    public:
		JagHashStrStr();
		~JagHashStrStr();
		JagHashStrStr( const JagHashStrStr &o );
		JagHashStrStr& operator= ( const JagHashStrStr &o );

		bool addKeyValue( const AbaxDataString &key, const AbaxDataString &value );
		void removeKey( const AbaxDataString &key );
		bool keyExist( const AbaxDataString &key ) const;
		AbaxDataString getValue( const AbaxDataString &key, bool &rc ) const;
		char *getValue( const AbaxDataString &key ) const;
		void reset();
		bool getValue( const AbaxDataString &key, AbaxDataString &val ) const;
		AbaxDataString getKVStrings( const char *sep = "|" );
		AbaxDataString getKeyStrings( const char *sep = "|" );
		int  size(); // how many items
		void removeAllKey();


	protected:
		jag_hash_t  _hash;
		int     _len;
};


#endif
