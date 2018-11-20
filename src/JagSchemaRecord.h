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
#ifndef _jag_schema_record_h_
#define _jag_schema_record_h_

#include <abax.h>
#include <JagDef.h>
#include <JagColumn.h>
#include <JagVector.h>
#include <JagHashStrInt.h>

class JagSchemaRecord
{
	public:
		JagSchemaRecord( bool newVec = false );
		// JagSchemaRecord( const AbaxDataString &dbobj );
		~JagSchemaRecord();
		void destroy(  AbaxDestroyAction act=ABAX_NOOP);
		JagSchemaRecord( const JagSchemaRecord& other );
		JagSchemaRecord& operator=( const JagSchemaRecord& other );
		bool print();
		bool renameKey( const AbaxString &oldKeyName, const AbaxString & newKeyName );
		bool addValueColumnFromSpare( const AbaxString &colName, const AbaxDataString &type, 
									  abaxint length, abaxint sig );
		AbaxDataString getString() const;
		int parseRecord( const char *str );
		// int formatHeadRecord( char *buf );
		AbaxDataString formatHeadRecord() const;
		//int formatColumnRecord( char *buf, const char *name, const char *type, int offset, int length, int sig, bool isKey=false );
		//int formatTailRecord( char *buf );
		AbaxDataString formatColumnRecord( const char *name, const char *type, int offset, int length, 
										   int sig, bool isKey=false ) const;
		AbaxDataString formatTailRecord( ) const;

		int getKeyMode() const;
		int getPosition( const AbaxString& colName ) const;
		bool hasPoly(int &dim ) const;
		void setLastKeyColumn();



		// AbaxDataString dbobj;

		char        type[2];
		int 		keyLength;
		int 		valueLength;
		int 		ovalueLength;
		AbaxDataString tableProperty;
		int		    numKeys;
		int		    numValues;
		bool        hasMute;
		JagVector<JagColumn> *columnVector;
		int	        lastKeyColumn;
		//int         polyDim;

	protected:
		// int getFuncType ( const AbaxString &colname, char &dataType );
		JagHashStrInt _nameMap;
		void copyData( const JagSchemaRecord& other ); 
		void init( bool newVec );
};


#endif
