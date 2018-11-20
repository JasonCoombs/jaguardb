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
#ifndef _jag_userrole_h_
#define _jag_userrole_h_

#include <JagRecord.h>
#include <JagMutex.h>
#include <JagFixKV.h>

class JagDBServer;

//////// userid|db|tab|col|perm --> [filter where]
class JagUserRole : public JagFixKV
{
  public:

    JagUserRole( JagDBServer *servobj=NULL, int replicateType=0 );
	virtual ~JagUserRole();
	// virtual void init();
	AbaxDataString  getListRoles();
	bool 	addRole( const AbaxString &userid, const AbaxString& db, const AbaxString& tab, const AbaxString& col, 
				const AbaxString& role, const AbaxString &rowfilter );
	bool 	dropRole( const AbaxString &userid, const AbaxString& db, const AbaxString& tab, const AbaxString& col, const AbaxString& op ); 
	bool 	checkUserCommandPermission( const JagDBServer *servobj, const JagSchemaRecord *srec, const JagRequest &req, 
					const JagParseParam &parseParam, int i, AbaxDataString &rowFilter, AbaxDataString& errmsg );
	bool    isAuthed( const AbaxDataString &op, const AbaxDataString &userid, const AbaxDataString &db, const AbaxDataString &tab,
                 const AbaxDataString &col, AbaxDataString &rowFilter );

	AbaxDataString showRole( const AbaxString &userid );
	static AbaxDataString convertToStr( const AbaxDataString  &pm );
	static AbaxDataString convertManyToStr( const AbaxDataString &pm );

	protected:
	  void getDbTabCol( const JagParseParam &parseParam, int i, const JagStrSplit &sp2,
	  			AbaxDataString &db, AbaxDataString &tab, AbaxDataString &col );
	  AbaxDataString getError( const AbaxDataString &code, const AbaxDataString &action, 
	  		const AbaxDataString &db, const AbaxDataString &tab, const AbaxDataString &col, const AbaxDataString &uid );



};

#endif
