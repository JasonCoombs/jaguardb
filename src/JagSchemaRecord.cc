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

#include <abax.h>
#include <JagUtil.h>
#include <JagStrSplit.h>

#include <JagSchemaRecord.h>

JagSchemaRecord::JagSchemaRecord( bool newVec )
{
	init( newVec );
}

void JagSchemaRecord::init( bool newVec )
{
	//prt(("s5050 JagSchemaRecord::init() this=%0x\n", this ));
	type[0] = 'N';
	type[1] = 'A';
	keyLength = 16;
	valueLength = 16;
	ovalueLength = 0;
	// columnVector = new JagVector<AbaxPair<AbaxString, JagColumn> >();
	// columnVector = new JagVector<JagColumn>();
	if ( newVec ) {
		columnVector = new JagVector<JagColumn>();
	} else {
		columnVector = NULL;
	}
	numKeys = 0;
	numValues = 0;
	hasMute = false;
	// dbobj = ".";
	lastKeyColumn = -1;
	tableProperty = "0";
	//polyDim = 0;
}

JagSchemaRecord::~JagSchemaRecord()
{
	destroy();
}

void JagSchemaRecord::destroy(  AbaxDestroyAction act)
{
	if ( columnVector ) {
		delete columnVector;
		columnVector = NULL;
	}
}

// copy ctor
JagSchemaRecord::JagSchemaRecord( const JagSchemaRecord& other )
{
	copyData( other );
	columnVector = NULL;

	if ( other.columnVector ) {
    	abaxint  size =  other.columnVector->size();
    	columnVector = new JagVector<JagColumn>( size+8 );
    	for ( int i = 0; i < size; ++i ) {
    		columnVector->append( (*other.columnVector)[i] );
    		_nameMap.addKeyValue( (*other.columnVector)[i].name.c_str(), i );
    	}
	}
}

JagSchemaRecord& JagSchemaRecord::operator=( const JagSchemaRecord& other )
{
	if ( columnVector == other.columnVector ) {
		return *this;
	}

	destroy();
	copyData( other );
	_nameMap.reset();

	abaxint  size = 0;
	if ( other.columnVector ) {
		size =  other.columnVector->size();
		columnVector = new JagVector<JagColumn>( size ); 
	}

	if ( size > 0 ) {
		for ( int i = 0; i < size; ++i ) {
			columnVector->append( (*other.columnVector)[i] );
			_nameMap.addKeyValue( (*other.columnVector)[i].name.c_str(), i );
		}
	}
	return *this;
}

void JagSchemaRecord::copyData( const JagSchemaRecord& other )
{
	// 7 basic data elements
	type[0] = other.type[0];
	type[1] = other.type[1];
	keyLength = other.keyLength;
	numKeys = other.numKeys;
	numValues = other.numValues;
	valueLength = other.valueLength;
	ovalueLength = other.ovalueLength;
	// dbobj = other.dbobj;
	lastKeyColumn = other.lastKeyColumn;
	tableProperty = other.tableProperty;
	//polyDim = other.polyDim;
	//prt(("s8753 lastKeyColumn=%d\n", lastKeyColumn ));
}


bool JagSchemaRecord::print()
{
	if ( ! columnVector ) return false;
	printf("%c%c|%d|%d|%s|{", type[0], type[1], keyLength, valueLength, tableProperty.c_str() );
	// printf("%s:%c%c|%d|%d|%d|{", dbobj.c_str(), type[0], type[1], keyLength, valueLength, ovalueLength );
	/** 
	 AbaxString name;
	 char           type;
	 unsigned int   offset;
	 unsigned int   length;
	 unsigned int   sig;
	 char           spare[4];
	**/
	int len = columnVector->size();
	for ( int i = 0; i < len; ++i ) {
		printf("!%s!%s!%d!%d!%d!%c%c%c%c%c%c%c%c!",
				(*columnVector)[i].name.c_str(),
				(*columnVector)[i].type.c_str(),
				(*columnVector)[i].offset,
				(*columnVector)[i].length,
				(*columnVector)[i].sig,
				(*columnVector)[i].spare[0],
				(*columnVector)[i].spare[1],
				(*columnVector)[i].spare[2],
				(*columnVector)[i].spare[3],
				(*columnVector)[i].spare[4],
				(*columnVector)[i].spare[5],
				(*columnVector)[i].spare[6],
				(*columnVector)[i].spare[7]
				);

			printf("|");
	}

	printf("}\n");
	return 1;
}

bool JagSchemaRecord::renameColumn( const AbaxString &oldName, const AbaxString & newName )
{
	int len = columnVector->size();
	AbaxString name;
	bool found = false;
	for ( int i = 0; i < len; ++i ) {
		name = (*columnVector)[i].name;
		if ( name == oldName ) {
			(*columnVector)[i].name = newName;
			_nameMap.removeKey( oldName.c_str() );
			_nameMap.addKeyValue( newName.c_str(), i );
			found = true;
			break;
		}
	}

	return found;
}

bool JagSchemaRecord::setColumn( const AbaxString &colName, const AbaxString &attr, const AbaxString & value )
{
	int len = columnVector->size();
	AbaxString name;
	bool found = false;
	for ( int i = 0; i < len; ++i ) {
		name = (*columnVector)[i].name;
		if ( name == colName ) {
			//(*columnVector)[i].name = newName;
			//_nameMap.removeKey( oldName.c_str() );
			//_nameMap.addKeyValue( newName.c_str(), i );
			if ( attr == "srid" ) {
				(*columnVector)[i].srid = jagatoi(value.s());
			}
			found = true;
			break;
		}
	}

	return found;
}


// add new column using spare space
bool JagSchemaRecord::addValueColumnFromSpare( const AbaxString &colName, const Jstr &type, 
											   abaxint length, abaxint sig )
{
	if ( ! columnVector ) {
		columnVector = new JagVector<JagColumn>();
	}

	int len = columnVector->size();
	AbaxString aname; unsigned int aoffset, alength, asig; 
	Jstr atype;
	char aspare[JAG_SCHEMA_SPARE_LEN]; 
	char aiskey; int afunc;
	aname = (*columnVector)[len-1].name;
	atype = (*columnVector)[len-1].type;
	aoffset = (*columnVector)[len-1].offset;
	alength = (*columnVector)[len-1].length;
	asig = (*columnVector)[len-1].sig;

	for ( int i = 0; i < JAG_SCHEMA_SPARE_LEN; ++i ) {
		aspare[i] = (*columnVector)[len-1].spare[i];
	}

	aiskey = (*columnVector)[len-1].iskey;
	afunc = (*columnVector)[len-1].func;
	
	// change last column from spare_ to colName, and add another spare_ with remain
	(*columnVector)[len-1].name = colName;
	(*columnVector)[len-1].type = type;
	(*columnVector)[len-1].length = length;
	(*columnVector)[len-1].sig = sig;
	// (*columnVector)[len-1].record = this;
	
	AbaxString dum;
	JagColumn onecolrec;
	onecolrec.name = aname;
	onecolrec.type = atype;
	onecolrec.offset = aoffset+length;
	onecolrec.length = alength-length;
	onecolrec.sig = asig;
	// onecolrec.record = this;

	for ( int i = 0; i < JAG_SCHEMA_SPARE_LEN; ++i ) {
		*(onecolrec.spare+i) = aspare[i];
	}

	onecolrec.iskey = aiskey;
	onecolrec.func = afunc;
	// columnVector->append( AbaxPair<AbaxString,JagColumn>(dum, onecolrec));
	columnVector->append( onecolrec );
	_nameMap.addKeyValue( colName.c_str(), len-1 );
}

Jstr JagSchemaRecord::getString() const
{
	if ( ! columnVector ) return "";

	Jstr res;
	char mem[4096];
	char buf[32];
	char buf2[2];
	buf2[1] = '\0';

	memset( mem, 0, 4096 );
	// sprintf(mem, "%c%c|%d|%d|%d|{", type[0], type[1], keyLength, valueLength, ovalueLength );
	sprintf(mem, "%c%c|%d|%d|%s|{", type[0], type[1], keyLength, valueLength, tableProperty.c_str() );
	res += Jstr( mem );

	int len = columnVector->size();
	for ( int i = 0; i < len; ++i ) {
		memset( mem, 0, 4096 );
		sprintf(mem, "!%s!%s!%d!%d!%d!",
				(*columnVector)[i].name.c_str(),
				(*columnVector)[i].type.c_str(),
				(*columnVector)[i].offset,
				(*columnVector)[i].length,
				(*columnVector)[i].sig );

		for ( int k = 0; k < JAG_SCHEMA_SPARE_LEN; ++ k ) {
			buf2[0] = (*columnVector)[i].spare[k];
			strcat( mem, buf2 );
		}
		strcat( mem, "!" );

		sprintf( buf, "%d!", (*columnVector)[i].srid ); strcat( mem, buf );
		sprintf( buf, "%d!", (*columnVector)[i].begincol ); strcat( mem, buf );
		sprintf( buf, "%d!", (*columnVector)[i].endcol ); strcat( mem, buf );
		sprintf( buf, "%d!", (*columnVector)[i].dummy1 ); strcat( mem, buf );
		sprintf( buf, "%d!", (*columnVector)[i].dummy2 ); strcat( mem, buf );
		sprintf( buf, "%d!", (*columnVector)[i].dummy3 ); strcat( mem, buf );
		sprintf( buf, "%d!", (*columnVector)[i].dummy4 ); strcat( mem, buf );
		sprintf( buf, "%d!", (*columnVector)[i].dummy5 ); strcat( mem, buf );
		sprintf( buf, "%d!", (*columnVector)[i].dummy6 ); strcat( mem, buf );
		sprintf( buf, "%d!", (*columnVector)[i].dummy7 ); strcat( mem, buf );
		sprintf( buf, "%d!", (*columnVector)[i].dummy8 ); strcat( mem, buf );
		sprintf( buf, "%d!", (*columnVector)[i].dummy9 ); strcat( mem, buf );
		sprintf( buf, "%d!", (*columnVector)[i].dummy10 ); strcat( mem, buf );

		res += mem;
		res += "|";
	}
	res += "}";

	return res;
}

Jstr JagSchemaRecord::formatHeadRecord() const
{ 
	// return "NN|0|0|0|{"; 
	return Jstr("NN|0|0|") + tableProperty + "|{"; 
}

Jstr JagSchemaRecord::formatColumnRecord( const char *name, const char *type, int offset, int length, 
													int sig, bool isKey ) const
{
	char buf[256];
	sprintf(buf, "!#%s#!%s!%d!%d!%d!", name, type, offset, length, sig);
	Jstr res = buf;
	if ( isKey ) {
		res += "k a ";
	} else {
		res += "v   ";
	}
	// used 4 bytes in spare

	// -4 means deduct 4 bytes above
	for ( int i = 0;  i < JAG_SCHEMA_SPARE_LEN-4; ++i ) {
		res += " ";
	}
	res += "!|";
	return res;
}


Jstr JagSchemaRecord::formatTailRecord() const
{
	return "}";
}

// parse schema record
// format is "NN|keylen|vallen|tableProperty|{!name!type!offset!length!sig!spare(16bytes)!srid!4!8!dummy1!dummy2!...!dummy10!|...}"
//                                                                                       4:start-col!8:end-col
//  tableProperty: "polyDim(0/2/3)!...!...!"
//  tableProperty: "1!2!...!...!"
int JagSchemaRecord::parseRecord( const char *str ) 
{
	if ( ! columnVector ) {
		columnVector = new JagVector<JagColumn>();
	}

	//prt(("s3980 parseRecord str=[%s]\n", str ));
	abaxint actkeylen = 0, actvallen = 0, rc = 0;
	JagColumn onecolrec;
	const char *p, *q;
	q = p = str;
	numKeys = 0;
	numValues = 0;	
	
	while ( *q != ':' && *q != '|' ) {
		++q;
	}
	if ( *q == '\0' ) return -10;

	if ( *q == ':' ) {
		// a.b(q)NN|
		// dbobj = Jstr(p, q-p);
		++q;
		type[0] = *q; type[1] = *(q+1);
		while ( *q != '|' ) ++q;
		if ( *q == '\0' ) return -20;
		// q points to first |
	}  else {
		type[0] = *(q-2); type[1] = *(q-1);
		// q points to first |
	}
	
	// get keylen
	++q; p = q;
	while ( isdigit(*q) ) ++q;
	if ( *q != '|' ) return -20;
	keyLength = rayatoi(p, q-p);
	
	// get vallen
	++q; p = q;
	while ( isdigit(*q) ) ++q;
	if ( *q != '|' ) return -30;
	valueLength = rayatoi(p, q-p);
	
	/***
	// get ovallen
	++q; p = q;
	while ( isdigit(*q) ) ++q;
	if ( *q != '|' || *(q+1) != '{' ) return -40;
	ovalueLength = rayatoi(p, q-p);
	q += 2; p = q;
	***/
	++q; p = q;
	while ( *q != '|' && *q != '{' ) ++q;
	if ( *q == '\0' ) return -42;
	tableProperty = Jstr(p, q-p);
	//prt(("s5034 tableProperty=[%s] this=%0x\n", tableProperty.c_str(), this ));
	if ( *q == '|' && *(q+1) == '{' ) {
		q += 2;
	} else if ( *q == '{' ) {
		++q;
	} else {
		return -43;
	}
	
	// begin parse each column
	while ( 1 ) {
		// begin with '!' before name part
		if ( *q != '!' ) {
			//prt(("s2039 q=[%s] return 0\n", q ));
			return -50;
		}
		p = ++q;
		if ( *q == '#' ) {
			p = ++q;
			while ( 1 ) {
				while ( *q != '#' && *q != '\'' && *q != '"' && *q != '\0' ) ++q;
				if ( *q == '\0' ) return -60;
				else if ( *q == '\'' || *q == '"' ) {
					q = (const char*)jumptoEndQuote(q);
					if ( *q == '\0' ) return -70;
					++q;
				} else break;
			}
			onecolrec.name = Jstr(p, q-p);
			++q;
		} else {
			while ( 1 ) {
				while ( *q != '!' && *q != '\'' && *q != '"' && *q != '\0' ) ++q;
				if ( *q == '\0' ) return -80;
				else if ( *q == '\'' || *q == '"' ) {
					q = (const char*)jumptoEndQuote(q);
					if ( *q == '\0' ) return -90;
					++q;
				} else break;
			}
			onecolrec.name = Jstr(p, q-p);
		}
		// get func(col) and assign onecolrec.func and change  onecolrec.type
		// onecolrec.func = getFuncType( onecolrec.name, newDataType );
		//prt(("s5130 onecolrec.name=[%s]\n", onecolrec.name.c_str() ));
		
		// get type
		// prt(("s1282 q=[%s]\n", q ));
		++q; p = q; 
		while ( *q != '!' && *q != '\0' ) ++q;
		if ( *q != '!' ) {
			// prt(("s1283 q=[%s]\n", q ));
			return -100;
		}
		onecolrec.type = Jstr(p, q-p);
		// prt(("s2031 onecolrec.type=[%s]\n", onecolrec.type.c_str() ));
		// get offset
		++q; p = q;
		while ( isdigit(*q) ) ++q;
		if ( *q != '!' ) return -110;
		onecolrec.offset = rayatoi(p, q-p);

		// get length
		++q; p = q;
		while ( isdigit(*q) ) ++q;
		if ( *q != '!' ) return -120;
		onecolrec.length = rayatoi(p, q-p);

		// get sig
		++q; p = q;
		while ( isdigit(*q) ) ++q;
		if ( *q != '!' ) return -130;
		onecolrec.sig = rayatoi(p, q-p);	

		// get spare
		++q; p = q; q += JAG_SCHEMA_SPARE_LEN;
		if ( *q != '!' ) return -140;
		memcpy( onecolrec.spare, p, JAG_SCHEMA_SPARE_LEN );
		if ( onecolrec.spare[5] == JAG_KEY_MUTE ) {
			hasMute = true;
			// prt(("s2230 col=[%s] hasMute=true\n", onecolrec.name.c_str() ));
		}
		
		if ( *(onecolrec.spare) == JAG_C_COL_KEY ) {
			actkeylen += onecolrec.length;
			onecolrec.iskey = true;
			++ numKeys;
		} else {
			actvallen += onecolrec.length;
			++ numValues;
		}

		// format is "NN|keylen|vallen|tableProperty|{!name!type!offset!length!sig!spare(16bytes)!srid!4!8!|...}"
		// q is at !
		// prt(("s4128 q=[%s]\n", q ));
		if ( *(q+1) != '|' ) {
			// there are more fields, composite-fields
			// get sig
			++q; p = q;
			while ( isdigit(*q) ) ++q;
			if ( *q != '!' ) return -150;
			onecolrec.srid = rayatoi(p, q-p);	
			//prt(("s3410 onecolrec.srid=%d\n", onecolrec.srid ));

			// get composite-col begincol
			++q; p = q;
			while ( isdigit(*q) ) ++q;
			if ( *q != '!' ) return -160;
			onecolrec.begincol = rayatoi(p, q-p);	

			// get composite-col endcol
			++q; p = q;
			while ( isdigit(*q) ) ++q;
			if ( *q != '!' ) return -170;
			onecolrec.endcol = rayatoi(p, q-p);	

			// get composite-col dummy1
			++q; p = q;
			while ( isdigit(*q) ) ++q;
			if ( *q != '!' ) return -180;
			onecolrec.dummy1 = rayatoi(p, q-p);	

			// get composite-col dummy2
			++q; p = q;
			while ( isdigit(*q) ) ++q;
			if ( *q != '!' ) return -190;
			onecolrec.dummy2 = rayatoi(p, q-p);	

			// get composite-col dummy3
			++q; p = q;
			while ( isdigit(*q) ) ++q;
			if ( *q != '!' ) return -200;
			onecolrec.dummy3 = rayatoi(p, q-p);	

			// get composite-col dummy4
			++q; p = q;
			while ( isdigit(*q) ) ++q;
			if ( *q != '!' ) return -210;
			onecolrec.dummy4 = rayatoi(p, q-p);	

			// get composite-col dummy5
			++q; p = q;
			while ( isdigit(*q) ) ++q;
			if ( *q != '!' ) return -220;
			onecolrec.dummy5 = rayatoi(p, q-p);	

			// get composite-col dummy6
			++q; p = q;
			while ( isdigit(*q) ) ++q;
			if ( *q != '!' ) return -230;
			onecolrec.dummy6 = rayatoi(p, q-p);	

			// get composite-col dummy7
			++q; p = q;
			while ( isdigit(*q) ) ++q;
			if ( *q != '!' ) return -240;
			onecolrec.dummy7 = rayatoi(p, q-p);	

			// get composite-col dummy8
			++q; p = q;
			while ( isdigit(*q) ) ++q;
			if ( *q != '!' ) return -250;
			onecolrec.dummy8 = rayatoi(p, q-p);	

			// get composite-col dummy9
			++q; p = q;
			while ( isdigit(*q) ) ++q;
			if ( *q != '!' ) return -260;
			onecolrec.dummy9 = rayatoi(p, q-p);	


			// get composite-col dummy10
			++q; p = q;
			while ( isdigit(*q) ) ++q;
			if ( *q != '!' ) {
				//prt(("s2284 q=[%s] not ! return 0\n", q ));
				return -280;
			}
			onecolrec.dummy10 = rayatoi(p, q-p);	

		}

		// columnVector->append( AbaxPair<AbaxString,JagColumn>(dum, onecolrec));
		// onecolrec.record = this;
		columnVector->append( onecolrec );
		// prt(("s2203 columnVector->append size=%d\n", columnVector->size() ));
		rc = 1;
		++q;
		if ( *q != '|' ) {
			// prt(("s2029 q=[%s] not '|' return 0\n", q ));
			return -290;
		}
		++q;
		if ( *q == '}' ) break;
	}

	++q;
	while ( *q == ' ' ) ++q;
	if ( *q != '\0' ) {
		// prt(("s2013 q=[%s] not null return 0\n", q ));
		return -300;
	}
	
	if ( 0 == keyLength && 0 == valueLength ) {
		keyLength = actkeylen;
		valueLength = actvallen;
	}

	// prt(("s0931 parseRecord hasMute=%d\n", hasMute ));
	setLastKeyColumn();

	return 1;
}


void JagSchemaRecord::setLastKeyColumn()
{
	lastKeyColumn = -1;
	for ( int i=0; i < (*columnVector).length(); ++ i ) {
		//prt(("s5140 *columnVector i=%d name=[%s] iskey=%d\n", 
			  // i, (*columnVector)[i].name.c_str(), (*columnVector)[i].iskey ));
		if ( (*columnVector)[i].iskey ) {
			if ( ! strstr( (*columnVector)[i].name.c_str(), "geo:" ) ) {
				lastKeyColumn = i;
				//prt(("s4502 lastKeyColumn=%d\n", lastKeyColumn ));
			} else {
				break;
			}
		} else {
			break;
		}
	}
	//prt(("s2036 lastKeyColumn=%d\n", lastKeyColumn ));
}


// Get key mode from first key column: byte 3 in spare field
int JagSchemaRecord::getKeyMode() const
{
	if ( ! columnVector ) return 0;

	int mode = JAG_RAND;
	for ( int i = 0; i < columnVector->size(); ++i ) {
		// keymode of first key column
		if ( (*columnVector)[i].iskey ) {
			mode = (*columnVector)[i].spare[2];
			break;
		}
	}
	return mode;
}

// return index-position of colname in _nameMap in columnVec
int JagSchemaRecord::getPosition( const AbaxString& colName ) const
{
	int  n = -1;
	bool rc;
	n = _nameMap.getValue( colName.c_str(), rc );
	if ( ! rc ) return -1;
	return n;
}

// tableProperty:  polyDim!...!...
bool JagSchemaRecord::hasPoly(int &dim ) const
{
	//prt(("s4045 this=%0x hasPoly keyLength=%d valueLength=%d polyDim=%d\n", this, keyLength, valueLength, polyDim ));
	//prt(("s3027 tableProperty=[%s]\n", tableProperty.c_str() ));
	dim = 0;
	JagStrSplit sp(tableProperty, '!', true );
	// prt(("s3027 tableProperty=[%s] sp.length=%d\n", tableProperty.c_str(), sp.length() ));
	if ( sp.length() < 1 ) return false;
	dim = jagatoi( sp[0].c_str() );
	if ( dim < 1 ) return false;
	return true;
}
