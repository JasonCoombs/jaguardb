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
#include <JagGlobalDef.h>
#include <JagDBMap.h>

// ctor
JagDBMap::JagDBMap( )
{
	init();
}

// dtor
JagDBMap::~JagDBMap( )
{
	destroy();
}

void JagDBMap::destroy( )
{
	if ( _map ) {
		_map->clear();
		delete _map;
		_map = NULL;
	}
}

void JagDBMap::init()
{
	_map = new FixMap();
}

// returns false if item actually exists in array
// otherwise returns true
bool JagDBMap::insert( const JagDBPair &newpair )
{
	std::pair<std::map<AbaxFixString,FixValuePair>::iterator,bool> ret;
	FixValuePair vpair ( newpair.value, newpair.upsertFlag );
	FixPair pair( newpair.key, vpair );
    ret = _map->insert ( pair );
    if (ret.second == false) {
		return false;
	}

	return true;
}

bool JagDBMap::remove( const JagDBPair &pair )
{
	 _map->erase( pair.key );
	return true;
}

bool JagDBMap::exist( const JagDBPair &search )
{
	if ( _map->find( search.key ) == _map->end() ) {
		return false;
	}
	
    return true;
}

bool JagDBMap::get( JagDBPair &pair )
{
	FixMap::iterator it = _map->find( pair.key );
	if ( it == _map->end() ) {
		return false;
	}

	pair.value = it->second.first;
	return true;
}

bool JagDBMap::set( const JagDBPair &pair )
{
	// std::map<AbaxFixString, AbaxFixString>::iterator it = _map->find( pair.key );
	FixMap::iterator it = _map->find( pair.key );
	if ( it == _map->end() ) {
		return false;
	}

	it->second.first = pair.value;
	return true;
}


void JagDBMap::print( )
{
    for ( FixMap::iterator it=_map->begin(); it!=_map->end(); ++it) {
   		printf("key=[%s]  --> value=[%s]\n", it->first.c_str(), it->second.first.c_str() );
    }

}
