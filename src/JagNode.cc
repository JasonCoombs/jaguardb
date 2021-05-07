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

#include <JagFixKV.h>
#include <JagNode.h>

// ctor
JagNode::JagNode() : JagFixKV( "system", "NodeStat", 0 )
{
}

// dtor
JagNode::~JagNode()
{
	this->destroy();
}

#if 0
// PASS: password; PERM: role:  READ/WRITE  READ: read-only    WRITE: read and write
// PERM:  ADMIN or USER
bool JagNode::addNode( const AbaxString &nodeid, const AbaxString& usedGB, const AbaxString& freeGB )
{
	JagReadWriteMutex mutex( _lock, JagReadWriteMutex::WRITE_LOCK );
	char buf[32];
	sprintf( buf, "%lld", time(NULL) );

	//JagRecord  record;
	record.addNameValue( JAG_USED, usedGB.c_str() );
	record.addNameValue( JAG_FREE, freeGB.c_str() );
	record.addNameValue( JAG_REG,  buf );
	record.addNameValue( JAG_STAT, "NEW" );

    // char kv[ KVLEN + 1];
    char *kv = (char*) jagmalloc ( KVLEN+1 );
    memset(kv, 0, KVLEN + 1 );
    JagDBPair pair, retpair;
    pair.point( kv, KLEN, kv+KLEN, VLEN );
    strcpy( kv, nodeid.c_str() );
    strcpy( kv+KLEN, record.getSource() );
	int insertCode;
    int rc = _darr->insert( pair, insertCode, false, true, retpair );
    // printf("s4822 addNode _darr->insert rc=%d\n", rc );
	if ( kv ) free ( kv );

	return 1;
}

bool JagNode::dropNode( const AbaxString &nodeid )
{
	return this->dropKey( nodeid ); 
}
#endif

Jstr JagNode::getListNodes()
{
	return this->getListKeys();
}

