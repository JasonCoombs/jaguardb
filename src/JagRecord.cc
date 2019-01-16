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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <malloc.h>
#include <sstream>
#include "JagRecord.h"
#include "JagUtil.h"


// record:
// CZnnnnnnnn000021#000234,name1:mm+nn,name2:mm+nn,name3:mmm+nnn^value1#value2#value3
// C: C if compress, ' ' if not compressed
// Z: 'Z' if ZLIB compressed
// nnnnnnnn: length of ensuing bytes (compressed with NULL in between or regular uncompressed string
// 21: header size (excluding 000021#000234, excluding ^)  size of name1:....name3:mmm+nnn
// 234: size of value record behind ^, not including the ending NULL

// mm, nn are digits, resprenting start+length of value<N> in the value part of the record

// ctor
JagRecord::JagRecord()
{
	_record = NULL;
	_readOnly = 0;
}

// ctor: accept formated source record
JagRecord::JagRecord( const char *srcrec )
{
	_record = NULL;
	_readOnly = 0;
	setSource( srcrec );
}

// dtor
void JagRecord::destroy ( ) 
{
	if ( _readOnly ) {
		return;
	}

	if ( _record ) {
		free( _record );
	}
	_record = NULL;
}

// dtor
JagRecord::~JagRecord ( ) 
{
	destroy();
}

// private method
int JagRecord::makeNewRecLength( const char *name, int n1,  const char *value, int n2 )
{
	char *pnew;
	int len;
	int hdrsize;
	//char buf256[256];
	AbaxDataString buf256str;
	char buf[32];

	//if ( n1 > FREC_MAX_NAME_LEN ) return -2089;
	if ( _record ) {
		free ( _record );
		_record = NULL;
	}

	len = 10 + n1 + 32 + n2; 
	pnew = (char*)jagmalloc( len );
	memset(pnew, 0, len );

	// name:start+length^value
	// name:0+strlen(value)^value
	/**
	memset(buf256, 0, 256);
	sprintf(buf256, "%s:0+%06d", name, n2 ); 
	hdrsize = strlen(buf256);  // hdrsize
	***/
	sprintf(buf, "%06d", n2 );
	buf256str = AbaxDataString(name) + ":0+" + buf;
	hdrsize = buf256str.size();

	// fixhead
	// sprintf(pnew, "%06d#%06d,%s^%s", n, n2, buf256, value );
	int len2 = 6+1+6+1 + hdrsize + 1 + n2; 
	//sprintf(pnew, "  %08d%06d#%06d,%s^%s", len2, hdrsize, n2, buf256, value );
	// sprintf(pnew, "  %08d%06d#%06d%c%s^%s", len2, hdrsize, n2, FREC_COMMA, buf256, value );
	sprintf(pnew, "  %08d%06d%c%06d%c%s%c%s", len2, hdrsize, FREC_VAL_SEP, n2, FREC_COMMA, buf256str.c_str(), FREC_HDR_END, value );
	_record = pnew;
	return 0;
}

// private
// out: colstart, collen
// < 0 error
// 0: OK
// int getNameStartLen(const char *record, const char *name, int namelen, int *colstart, int *collen )
int JagRecord::getNameStartLen( const char *name, int namelen, int *colstart, int *collen )
{
	int index;
	char *p;
	char *start;
	//char startpos[FREC_STR_MAX_LEN];
	//char poslen[FREC_STR_MAX_LEN];  // FREC_STR_MAX_LEN=128
	AbaxDataString startposstr;
	AbaxDataString poslenstr;;
	//int  i;
	//char buf256[256];
	if ( name == NULL || *name == '\0' ) {
		*colstart = *collen = 0;
		return 0;
	}
	AbaxDataString buf256;

	//if (namelen > FREC_MAX_NAME_LEN ) return -100;
	/**
	memset( buf256, 0, 256); 
	//strncat(buf256, ",", 1);
	strncat(buf256, FREC_COMMA_STR, 1);
	strncat(buf256, name, namelen);
	strncat(buf256, ":", 1);
	**/
	buf256 = FREC_COMMA_STR;
	buf256 += AbaxDataString(name, namelen );
	buf256 += ":";

	// index = str_str_ch( _record, FREC_HDR_END, buf256 );
	index = str_str_ch( _record+10, FREC_HDR_END, buf256.c_str() );
	if ( index < 0 ) {
		return -2;
	}

	// start = (char*)( _record + index + 1 + namelen + 1); // 1st:1 is to pass; 2nd 1 is to point to once byte after :
	start = (char*)( _record + 10 + index + 1 + namelen + 1); // 1st:1 is to pass; 2nd 1 is to point to once byte after :

	//memset(startpos, 0, FREC_STR_MAX_LEN);
	//memset(poslen, 0, FREC_STR_MAX_LEN);

	//if ( debug ) { printf("start=[%s]\n", start ); }

	// name1:mm+nn,name2:mm+nn,name3:mmm+nnn^value1#value2#value3
	// name1:mm+nn,name2:mm+nn,name3:mmm+nnn,^value1#value2#value3
	// start point to mm+nn[,|^]
	//i = 0;
	for( p= start; *p != '+' && *p!='\0'; p++ ) {
		//startpos[i++] = *p;
		startposstr += *p;
		/***
		if ( i > FREC_STR_MAX_LEN ) {
			//if ( debug ) { printf("startpos too large name=[%s] namelen=%d \n", name, namelen ); }
			return -3; // too large
		}
		***/
	}

	//prt(("s2129  startposstr=[%s]\n", startposstr.c_str() ));

	if ( *p != '+' ) {
		// if ( debug ) { printf("no + sign \n"); }
		return -4;  // strucure error, not found
	}
	// *colstart = atoi( startpos );
	*colstart = jagatoi( startposstr.c_str() );
	/**
	if ( debug ) {
		printf("startpos=[%s] colstart=[%d]\n", startpos, *colstart );
	}
	**/

	p++;  // poins to nn[,|^] now

	// if ( debug ) { printf("now p=[%s]\n", p ); }

	//i = 0;
	//while ( (*p != ',') && ( *p != FREC_HDR_END ) && ( *p != '\0' ) )
	while ( (*p != FREC_COMMA ) && ( *p != FREC_HDR_END ) && ( *p != '\0' ) ) {
		poslenstr += *p;
		/***
		poslen[i++] = *p;
		if ( i > FREC_STR_MAX_LEN ) {
			// if ( debug ) { printf("poslen too large i=%d poslen=[%s] name=[%s] namelen=%d \n", i, poslen, name, namelen ); }
			return -5; // too large
		}
		***/
		p++;
	}

	//*collen = atoi( poslen );
	//prt(("s2123  poslenstr=[%s]\n", poslenstr.c_str() ));
	*collen = jagatoi( poslenstr.c_str() );

	//prt(("s61029 name=[%s] colstart=%d  collen=%d\n", name, *colstart, *collen ));

	return 0;
}


// out: hder size and val size
// <0: error
// 0: OK
// int getSize(const char *record, int *hdrsize, int *valsize )
int JagRecord::getSize( int *hdrsize, int *valsize )
{
	char *p;
	//char lenbuf[FREC_STR_MAX_LEN];
	AbaxDataString lenbufstr;
	//int i;
	// uncompress();
	//memset(lenbuf, 0, FREC_STR_MAX_LEN );
	// p = _record;
	p = _record+10;
	//i = 0;
	while ( (*p !=  FREC_VAL_SEP ) && *p != '\0' ) {
		/**
		if ( i > FREC_STR_MAX_LEN ) { return -1; }
		lenbuf[i++] = *p;
		**/
		lenbufstr += *p;
		p++;
	}

	if ( *p != FREC_VAL_SEP ) {
		*hdrsize = 0;
		*valsize = 0;
		return -2;
	}

	// printf("s23232 keys lenbuf=[%s] _record=[%s]\n", lenbuf, _record );
	//*hdrsize = atoi(lenbuf);
	*hdrsize = jagatoi(lenbufstr.c_str());

	// look for total size
	//memset(lenbuf, 0, FREC_STR_MAX_LEN );
	lenbufstr = "";
	p++;
	//i = 0;
	while ( (*p != FREC_COMMA ) && *p != '\0' ) {
		//if ( i > FREC_STR_MAX_LEN ) { return -4; }
		//lenbuf[i++] = *p;
		lenbufstr += *p;
		p++;
	}

	if ( *p != FREC_COMMA ) {
		*hdrsize = 0;
		*valsize = 0;
		return -5;
	}

	// printf("s23234 val lenbuf=[%s]\n", lenbuf );
	//*valsize = atoi(lenbuf);
	*valsize = jagatoi(lenbufstr.c_str() );
	//prt(("s1723 hdrsize=%d valsize=%d\n", *hdrsize, *valsize ));
	return 0;
}

size_t JagRecord::getLength()
{

	char *p = _record;
	if ( p == NULL || *p == '\0' ) {
		return 0;
	}

	size_t  len = 0;
	// CZnnnnnnnn666666#666666,
	/*********
	int hdrsize, valsize;
	hdrsize = valsize = 0;
	int rc = getSize( &hdrsize, &valsize );
	if ( rc < 0 ) {
		return 0;
	}

	len = 10+6+1+6+1 +hdrsize + 1 + valsize; 
	printf("getleng size=%d  hdrsize=%d  valsize=%d\n", len, hdrsize, valsize );
	return len;
	********/

	char buf[9];
	for ( int i = 2; i < 10; ++i ) {
		buf[i-2] = _record[i];
	}
	buf[8] = '\0';
	// printf("getlength size=%d\n", 10 + atoi( buf ) );
	return ( 10 + atoi( buf ) );
}


// 21,234,name1:mm+nn,name2:mm+nn,name3:mmm+nnn^value1#value2#value3
// 21#234#name1:mm+nn,name2:mm+nn,name3:mmm+nnn,^value1#value2#value3#
// <0 for eror
// 0 for OK
// int getAllNameValues(const char *record, char *names[],  char *values[], int *len )
int JagRecord::getAllNameValues( char *names[],  char *values[], int *len )
{
	//int i;
	//char *pname; 
	//char *pvalue; 
	//char buf[FREC_MAX_NAME_LEN];
	char *p;
	int  startposi[FREC_MAX_NAMES];
	int  posleni[FREC_MAX_NAMES];
	//char valbuf[FREC_STR_MAX_LEN];
	int  donenames = 0;
	int  total = 0;
	int  hdrsize, valsize;

	//#define FREC_MAX_NAMES 64
	//#define FREC_MAX_NAME_LEN 64
	// i = uncompress();
	// printf("after uncompress rc=%d\n", i );

	total = 0;
	*len = 0;

	// names is array of maxsize 
	AbaxDataString valbufstr;
	//memset(valbuf, 0, FREC_STR_MAX_LEN);
	//i = 0;
	// for ( p = _record;  (*p != FREC_VAL_SEP ) && *p != '\0'; p++ )
	for ( p = _record+10;  (*p != FREC_VAL_SEP ) && *p != '\0'; p++ ) {
		/**
		if ( i>=FREC_STR_MAX_LEN ) {
			//debug
			// printf("c3949 i=%d > %d return -1\n", i, FREC_STR_MAX_LEN );
			return -1;
		}
		***/
		//valbuf[i++] = *p;
		valbufstr += *p;
	}

	if ( *p != FREC_VAL_SEP ) {
		// printf("c3963 -1 \n");
		return -1;
	}
	//hdrsize = atoi( valbuf );
	hdrsize = jagatoi( valbufstr.c_str() );

	p++;
	//memset(valbuf, 0, FREC_STR_MAX_LEN);
	valbufstr = "";
	//i = 0;
	while ( (*p != FREC_COMMA ) && *p != '\0' ) {
		//if ( i>=FREC_STR_MAX_LEN ) return -1;
		//valbuf[i++] = *p;
		valbufstr += *p;
		p++;
	}

	if ( *p != FREC_COMMA ) {
		//printf("c3961 -1 \n");
		return -2;
	}

	// if ( debug ) printf("valsize valbuf=[%s]\n", valbuf );
	//valsize = atoi( valbuf );
	valsize = jagatoi( valbufstr.c_str() );

	p++; // past second  #

    // get all names and positions
	AbaxDataString bufstr;
	valbufstr = "";
	while ( ( *p != FREC_HDR_END ) && (*p != '\0')  ) {
		donenames = 0;

		// work with one name/value
		//memset(buf, 0, FREC_MAX_NAME_LEN  );
		bufstr = "";
		//i = 0;
		while ( *p != ':' && *p != '\0' ) {
			/**
			if ( i>=FREC_MAX_NAME_LEN )
			{
				//printf("c3961 error -1 \n");
				return -3;
			}
			**/

			//buf[i++] = *p;
			bufstr += *p;
			p++;
		}

		if ( *p != ':' ) {
			return -4;
		}

		// if ( *len  >= FREC_MAX_NAMES  )
		if ( total  >= FREC_MAX_NAMES  ) {
			return -5;
		}

		//names[total] = strdup(buf);
		names[total] = strdup(bufstr.c_str());
		// 21#234#name1:mm+nn,name2:mm+nn,name3:mmm+nnn^value1#value2#value3

		p++;  // past :

		//memset(valbuf, 0, FREC_STR_MAX_LEN);
		valbufstr = "";
		//i = 0;
		while ( *p != '+' && *p != '\0' ) {
			/**
			if ( i>=FREC_STR_MAX_LEN )
			{
				return -6;
			}
			**/

			// valbuf[i++] = *p;
			valbufstr += *p;
			p++;
		}

		if ( *p != '+' ) {
			return -7;
		}

		//startposi[total] = atoi(valbuf);  // got start position
		startposi[total] = atoi(valbufstr.c_str() );  // got start position

		p++; // pas '+'

		// memset(valbuf, 0, FREC_STR_MAX_LEN);
		valbufstr = "";
		//i = 0;
		//while ( *p != ',' && *p != FREC_HDR_END && *p != '\0' )
		while ( *p != FREC_COMMA && *p != FREC_HDR_END && *p != '\0' ) {
			//valbuf[i++] = *p;
			valbufstr += *p;
			p++;
			/**
			if ( i>=FREC_STR_MAX_LEN )
			{
				return -8;
			}
			**/
		}

		if ( *p == FREC_HDR_END ) {
			donenames = 1;
		}

		if ( *p == '\0' ) {
			return -8;
		}

		//posleni[total] = atoi(valbuf);  // got length
		posleni[total] = atoi(valbufstr.c_str());  // got length

		total ++;
		p++;

		if ( donenames ) {
			break;
		}


		//if ( total >= JAG_GETALL_VALUE_MAX ) 
		if ( total >= FREC_MAX_NAMES-1 ) {
			break;
		}
	}

	// find value strings
	for (int i = 0; i < total; i++) {
		values[i] = getValueFromPosition( startposi[i], posleni[i], hdrsize, valsize );
		if ( ! values[i] ) {
			return -12;
		}
	}

	*len = total;
	return 0;
}

// start is relative to hdrsize
// does malloc
// 0: if error
// pointer to malloced memory  true
// char *getValueFromPosition(const char *record, int start, int len, int hdrsize, int valsize )
char * JagRecord::getValueFromPosition( int start, int len, int hdrsize, int valsize )
{
	char 	*pd; 
	char 	*p; 
	int 	i;
	char    *pstart;

	/**
	if(debug) {
		printf("getValueFromPosition start=%d len=%d hdrsize=%d valsize=%d\n", start, len, hdrsize, valsize );
	}
	**/

	if ( ( start + len ) > valsize ) {
		return NULL; 
	}

	// pstart = _record;
	pstart = _record+10;
	while ( *pstart != FREC_VAL_SEP && *pstart != '\0' ) pstart++;
	if ( '\0' == *pstart ) return 0;
	pstart ++;  // past first #
	while ( *pstart != FREC_COMMA  && *pstart != '\0' ) pstart++;
	if ( '\0' == *pstart ) return 0;
	pstart ++;  // past second ,

	pd = (char*)jagmalloc(len+1);
	memset(pd, 0, len+1);
	p = pd;

	pstart += hdrsize + 1;

	// if ( debug ) { //printf("new pstart=[%s] start=%d len=%d\n", pstart, start, len ); }

	for ( i = start; i< start + len; i++) {
		*p = pstart[i]; 
		p++;
	}

	// if ( debug ) { printf("pd=[%s]\n\n", pd ); }
	return pd;
}


// 0: if error
// char *remove (const char *record, const char *name )
bool JagRecord::remove ( const char *name )
{
	char 	*names[ FREC_MAX_NAMES ];
	char 	*vals[ FREC_MAX_NAMES ];
	int 	rc;
	int 	len; 
	char  	*pnew;
	int 	oldlen;
	int		i;
	char 	*phdr;
	char 	*pval;
	int   	relpos = 0;
	int  	vallen;
	int  	namelen;
	int  	hdrsize, valsize;
	char 	sizebuf[32];

	// uncompress();

	// rc = getAllNameValues(record, names,  vals, &len ); 
	len = 0;
	rc = getAllNameValues( names,  vals, &len ); 

	if ( rc < 0 ) {
		printf("c9394 rc=%d < 0 \n", rc );
		return false;
	}
	// printf("s3293 remove getAllNameValues len=%d\n", len );

	// oldlen = strlen(_record);
	char buf[9];
	for ( int i = 2; i < 10; ++i ) {
		buf[i-2] = _record[i];
	}
	buf[8] = '\0';
	oldlen = 10 + atoi( buf );

	pnew = (char*)jagmalloc(oldlen + 1 );
	memset(pnew, 0, oldlen + 1 );

	phdr = (char*)jagmalloc(oldlen + 1 );
	memset( phdr, 0, oldlen + 1 );

	pval = (char*)jagmalloc(oldlen + 1 );
	memset( pval, 0, oldlen + 1 );

	relpos = 0;
	for (i=0; i<len; i++ ) {
		if ( 0 == strcmp( name, names[i] ) ) {
			continue;
		}

		memset( sizebuf, 0, 32 );
		namelen = strlen( names[i] );
		vallen = strlen( vals[i] );

		strcat( phdr, names[i] );
		//sprintf( sizebuf, ":%06d+%06d,", relpos, vallen ); 
		sprintf( sizebuf, ":%06d+%06d%c", relpos, vallen, FREC_COMMA ); 
		strcat( phdr, sizebuf); 

		// if ( debug ) { printf("namelen=%d vallen=%d\n", namelen, vallen ); printf("phdr=%s\n", phdr ); }

		strcat( pval, vals[i] );
		//strcat( pval, "#");
		strcat( pval, FREC_VAL_SEP_STR );

		// if ( debug ) printf("pval=%s\n", pval );
		relpos += vallen + 1;
	}

	hdrsize = strlen(phdr);
	valsize = strlen(pval);
	// printf("s7484 hdrsize=%d  valsize=%d\n", hdrsize, valsize );

	memset(pnew, 0, oldlen + 1 );
	// fixhead
	// sprintf( pnew, "%06d#%06d,%s^%s", hdrsize, valsize, phdr, pval );
	// sprintf( pnew, "  %06d#%06d,%s^%s", hdrsize, valsize, phdr, pval );
	int len2 = 6+1+6+1 +hdrsize + 1 + valsize; 
	//sprintf( pnew, "  %08d%06d#%06d,%s^%s", len2, hdrsize, valsize, phdr, pval );
	//sprintf( pnew, "  %08d%06d#%06d%c%s^%s", len2, hdrsize, valsize, FREC_COMMA, phdr, pval );
	sprintf( pnew, "  %08d%06d%c%06d%c%s%c%s", len2, hdrsize, FREC_VAL_SEP, valsize, FREC_COMMA, phdr, FREC_HDR_END, pval );
	// if ( debug ) printf( "%s\n", pnew );

	if ( phdr ) free(phdr);
	if ( pval ) free(pval);
	phdr = pval = NULL;
	for (i=0; i<len; i++ ) {
		if ( names[i] ) free( names[i] );
		if ( vals[i] ) free( vals[i] );
		names[i] = vals[i] = NULL;
	}

	if ( _record ) free ( _record );
	_record = pnew;

	return true;
}



// 21#234,name1:mm+nn,name2:0000mm+0000nn,name3:000mmm+000nnn^value1#value2#value3
// 21#234,name1:mm+nn,name2:0000mm+0000nn,name3:000mmm+000nnn,^value1#value2#value3#
// if record is empty, make a new record
// char *addNameValue (const char *record, const char *name, const char *value )
int JagRecord::addNameValue ( const char *name, const char *value )
{
	int namelen;
	int vallen;
	if ( _readOnly ) { return -1; }
	// uncompress();
	namelen = strlen(name);
	vallen = strlen(value);
	return addNameValueLength ( name, namelen, value, vallen );
}


// 0: if error
// 21#234,name1:mm+nn,name2:mm+nn,name3:mmm+nnn^value1#value2#value3
// 21#234,name1:mm+nn,name2:mm+nn,name3:mmm+nnn,^value1#value2#value3#
// if record is empty, make a new record
// char *addNameValueLength (const char *record, int oldlen, const char *name, int n1, const char *value, int n2 )
int JagRecord::addNameValueLength ( const char *name, int n1, const char *value, int n2 )
{
	int newlen;
	char *pnew;
	char *psep;
	char *phdr;
	int   rc;
	int   hdrsize, valsize;
	int   new_hdrsize, new_valsize;
	char  *p;
	int   comma;
	int   pound;
	//char  buf256[256];
	char buf[32];
	int oldlen;

	if ( NULL == _record ) {
		oldlen = 0;
	} else {
		oldlen = getLength();
	}

	if ( oldlen <1 || NULL == _record ) {
		rc = makeNewRecLength( name, n1, value, n2 );
		//prt(("s1383 makeNewRecLength rc=%d\n", rc ));
		return rc;
	}

	// parse record, looking for hdrsize#valsize, in record begining
	// rc = getSize(record, &hdrsize, &valsize ); 
	rc = getSize( &hdrsize, &valsize ); 
	if ( rc < 0 ) {
		return -2903;
	}

	// if ( debug ) { printf("oldlen=%d hdrsize=%d  valsize=%d\n", oldlen, hdrsize, valsize ); }
	//if (n1 > FREC_MAX_NAME_LEN ) return -2813;

	newlen = oldlen + n1 + 32 + n2; 

	// find start of header
	// phdr = _record;
	phdr = _record + 10;
	while ( *phdr != FREC_COMMA && *phdr != '\0' ) phdr++;
	if ( *phdr == '\0' ) { return -2829; }
	phdr++;


	// find the separation spot
	psep = phdr;
	while ( *psep != FREC_HDR_END && *psep != '\0' ) psep++;
	if ( *psep == '\0' ) { return -2818; }
	// psep now points to '^'

	//memset(buf256, 0, 256 );
	AbaxDataString buf256str;
	pound = 0;
	if ( _record[oldlen-1] == FREC_VAL_SEP ) {
		pound = 1;
		//sprintf( buf256, "%s:%06d+%06d", name, valsize, n2 );
		buf256str = name;
		sprintf( buf, ":%06d+%06d", valsize, n2 );
		buf256str += buf;
		new_valsize = valsize + n2;
	} else {
		//sprintf( buf256, "%s:%06d+%06d", name, valsize+1, n2 );
		buf256str = name;
		sprintf( buf, ":%06d+%06d", valsize+1, n2 );
		buf256str += buf;
		new_valsize = valsize + 1 + n2;  // 1 is for #
	}

	// if ( debug ) { printf("buf256=[%s]\n", buf256 ); }
	comma = 0;
	//if ( *(psep-1) == ',' )
	if ( *(psep-1) == FREC_COMMA ) {
		//new_hdrsize = hdrsize + strlen(buf256);
		new_hdrsize = hdrsize + buf256str.size();
		comma = 1;
	} else {
		//new_hdrsize = hdrsize + 1 + strlen(buf256);  // 1 for the ,
		new_hdrsize = hdrsize + 1 + buf256str.size();  // 1 for the ,
	}

	pnew = (char*)jagmalloc( newlen );
	memset( pnew, 0, newlen );

	// fixhead
	// sprintf( pnew, "%06d#%06d,", new_hdrsize, new_valsize );
	int len2 = 6+1+6+1 + new_hdrsize + 1 + new_valsize; 
	//sprintf( pnew, "  %08d%06d#%06d,", len2, new_hdrsize, new_valsize );
	sprintf( pnew, "  %08d%06d%c%06d%c", len2, new_hdrsize, FREC_VAL_SEP, new_valsize, FREC_COMMA );
	strncat( pnew, phdr, psep - phdr );
	if ( comma ) {
		//strcat(pnew, buf256);
		strcat(pnew, buf256str.c_str());
	} else {
		//strcat(pnew, ",");
		strcat(pnew, FREC_COMMA_STR);
		//strcat(pnew, buf256);
		strcat(pnew, buf256str.c_str() );
	}

	//strcat(pnew, "^");
	strcat(pnew, FREC_HDR_END_STR);
	strcat(pnew, psep+1);

	if ( pound ) {
		strcat(pnew, value);
	} else {
		strcat(pnew, FREC_VAL_SEP_STR);
		strcat(pnew, value);
	}

	if ( _record ) free ( _record );
	_record = pnew;

	return 0;
}


// 0: if error
// ptr to malloc memoty hacing value of name
// char *getValue( const char *record, const char *name )
char *JagRecord::getValue( const char *name )
{
	int namelen;
	namelen = strlen(name);

	// make sure they are not compressed
	// uncompress();

	return getValueLength( name, namelen );
}

// faster
// 0: if error
// ptr to malloc memoty hacing value of name
// char *getValueLength( const char *record, const char *name, int namelen )
char * JagRecord::getValueLength( const char *name, int namelen )
{
	// get positions
	int rc;
	int colstart, collen;
	int   hdrsize, valsize;
	char *res;

	// rc = getSize(record, &hdrsize, &valsize );
	rc = getSize( &hdrsize, &valsize );
	if ( rc < 0 ) {
		/**
		char *p = (char*)malloc(1);
		*p = '\0';
		return p;
		**/
		return NULL;
	}

	rc = getNameStartLen( name, namelen, &colstart, &collen );
	if ( rc < 0 ) {
		/**
		char *p = (char*)malloc(1);
		*p = '\0';
		return p;
		**/
		return NULL;
	}

	res = getValueFromPosition( colstart, collen, hdrsize, valsize );
	// printf("c74747 _record=[%s]  getValueFromPosition=[%s]  name=[%s]\n", _record, res, name );
	return res;
}

// int nameExists(const char *record, const char *name )
int JagRecord::nameExists( const char *name )
{
	// uncompress();
	int namelen = strlen(name);
	return nameLengthExists( name, namelen );
}

// 0: if name does not exist in record
// 1: if exists
// int nameLengthExists(const char *record, const char *name, int namelen )
int JagRecord::nameLengthExists( const char *name, int namelen )
{
	int    index;
	//char  buf256[256];
	AbaxDataString buf256;

	//if ( namelen > FREC_MAX_NAME_LEN ) return 0;
	/***
    memset(buf256, 0, 256 );
    //strncat(buf256, ",", 1);
    strncat(buf256, FREC_COMMA_STR, 1);
    strncat(buf256, name, namelen);
    strncat(buf256, ":", 1);
	**/
	buf256 = FREC_COMMA_STR;
	buf256 += AbaxDataString(name, namelen );
	buf256 += ":";

    //index = str_str_ch( _record+10, FREC_HDR_END, buf256 );
    index = str_str_ch( _record+10, FREC_HDR_END, buf256.c_str() );
    if ( index < 0 ) {
        return 0; 
    }

	return 1;
}


// char *modifyValue (const char *record, const char *name, const char *value )
bool JagRecord::setValue ( const char *name, const char *value )
{
	int rc;
	bool pdel;
	int namelen;

	if ( _readOnly ) { return false; }

	// uncompress();
	namelen = strlen(name);
	rc = nameLengthExists( name, namelen );
	if ( rc ) {
		// delete name from value record
		// pdel = remove (record, name );
		pdel = remove ( name );
		if ( 0 == pdel ) {
			return 0;
		}
		addNameValue ( name, value );
	} else {
		addNameValue ( name, value );
	}

	return true;
}

// Accept new source string as record
void JagRecord::setSource( const char *srcrec )
{
	char buf[11];
	int i;

	_readOnly = 0;

	if ( _record ) {
		free ( _record );
	}
	_record = NULL;

	if ( NULL == srcrec ) {
		_record = (char*)malloc( 1 );
		*_record = '\0';
		return;
	}

	// check if too short
	for ( i = 0; i <10; ++i ) {
		buf[i ] = srcrec[i];
		if ( srcrec[i] == '\0' ) {
			_record = (char*)malloc( 1 );
			*_record = '\0';
			return;
		}
	}
	buf[10] = '\0';

	// int len = strlen( srcrec );
	int len = 10 + atoi( buf+2 ); // CSnnnnnnnn

	_record = (char*)jagmalloc( len +1 );
	memcpy( _record, srcrec, len );
	_record[len] = '\0';
}


// Direct read data from srcrec. No malloc is used.
void JagRecord::readSource( const char *srcrec )
{
	// printf("c6474 JagRecord::readSource(%s)\n", srcrec );
	_record = (char*)srcrec;
	_readOnly = 1;
}

// Direct read data from srcrec. No malloc is used.
void JagRecord::referSource( const char *srcrec )
{
	// printf("c6474 JagRecord::readSource(%s)\n", srcrec );
	_record = (char*)srcrec;
}

void JagRecord::print()
{
	printf("%s\n", _record );
	fflush( stdout );
}

bool JagRecord::addNameValueArray ( const char *name[], const char *value[], int len )
{
	if ( _readOnly ) { return false; }
	// uncompress();
	int  i;
	for ( i = 0; i < len; i++ ) {
		addNameValue (  name[i], value[i] );
	}

	return true;
}
