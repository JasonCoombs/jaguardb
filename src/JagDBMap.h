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
#ifndef _jag_dbmap_class_h_
#define _jag_dbmap_class_h_
#include <map>
#include <JagTime.h>
#include <JagDBPair.h>

typedef	std::pair<JagFixString,char>  FixValuePair;
typedef std::pair<JagFixString,FixValuePair>  FixPair;
typedef	std::map<JagFixString,FixValuePair>  FixMap;

class JagDBMap
{
	public:

		JagDBMap( );
		~JagDBMap();

		bool insert( const JagDBPair &newpair );
		// bool upsert( const JagDBPair &newpair );

		bool exist( const JagDBPair &pair );
		bool remove( const JagDBPair &pair );
		bool get( JagDBPair &pair ); 
		bool set( const JagDBPair &pair ); 

		void destroy();
		void clean() { _map->clear(); }
		abaxint size() const { return _map->size(); }
		abaxint elements() { return _map->size(); }
		void   print();
		std::map<JagFixString, FixValuePair> *_map; 

	protected:
		void init();

};
#endif
