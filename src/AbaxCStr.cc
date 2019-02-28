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
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <assert.h>
#include <unistd.h>
#include <AbaxCStr.h>

AbaxCStr::AbaxCStr()
{
	initmem(1);
}

AbaxCStr::AbaxCStr( size_t size )
{
	initmem(size);
}

AbaxCStr::AbaxCStr(const char* str)
{
	if ( 0 == str ) {
		initmem(0);
	    return;
	}

	int len = strlen(str);
	initmem(len);
	length_ = len;
	memcpy(buf_, str, len );
	buf_[length_] = '\0';
}

AbaxCStr::AbaxCStr(const char* str, size_t len )
{
	if ( 0 == str ) {
		initmem(0);
	    return;
	}

	initmem(len);
	length_ = len;
	memcpy(buf_, str, len );
	buf_[length_] = '\0';
}


// copy constructor
AbaxCStr::AbaxCStr(const AbaxCStr& str)
{
	initmem( str.length_ );

	length_ = str.length_;
	memcpy(buf_, str.buf_, length_ );
	buf_[length_] = '\0';
}



// destructor
AbaxCStr::~AbaxCStr()
{
	if ( buf_ ) free(buf_);
}

// assignment operator
// const AbaxCStr& AbaxCStr::operator=( const AbaxCStr& s) 
AbaxCStr& AbaxCStr::operator=( const AbaxCStr& s) 
{
	if ( this == &s ) {
		return *this;
	}

	if ( length_ < s.length_ ) {
		length_ = 0;
		allocMoreMemory( s.length_ );
	}

	if ( ! buf_ ) {
		initmem( s.length_ );
	}

	length_ = s.length_;
	memcpy(buf_, s.buf_, length_);
	buf_[length_]= '\0';

	return *this;
}


AbaxCStr& AbaxCStr::operator+= ( const AbaxCStr &str )
{
	if ( str.length() == 0 ) {
		return *this;
	}

	allocMoreMemory(str.length_ );
	memcpy(buf_+length_, str.buf_, str.length_);
	length_ += str.length_;
	buf_[length_] = '\0';
	return *this;
}

// plus operator
AbaxCStr& AbaxCStr::operator+=( const char *s)
{
	int len2 = strlen(s);

	if ( s == NULL || 0 == len2 ) {
		return *this;
	}

	allocMoreMemory( len2);
	memcpy(buf_+length_, s, len2 );
	length_ += len2;
	buf_[length_] = '\0';
	return *this;
}

// plus operator
// inline AbaxCStr& AbaxCStr::operator+=( int ch )
// inline void AbaxCStr::operator+=( int ch )
AbaxCStr&  AbaxCStr::operator+=( int ch )
{
	allocMoreMemory(1);  // one extra byte will cause buffer grow?
	buf_[length_] = ch;
	length_ += 1;
	buf_[length_] = '\0';
	return *this;
}

AbaxCStr& AbaxCStr::append( const char *s, unsigned long len2)
{
	if ( s == NULL || 0 == len2 ) {
		return *this;
	}

	allocMoreMemory( len2);
	memcpy(buf_+length_, s, len2 );
	length_ += len2;
	buf_[length_] = '\0';
	return *this;
}


// equality operator
bool AbaxCStr::operator==( const char *s) const
{
	if ( 0 == strcmp(buf_, s) ) {
		return 1;
	} else {
		return 0;
	}
}

// equality operator
bool AbaxCStr::operator==( const AbaxCStr &s) const
{
	if ( 0 == strcmp(buf_, s.data() ) ) {
		return 1;
	} else {
		return 0;
	}
}

int AbaxCStr::compare( const AbaxCStr &s ) const
{
	return strcmp(buf_, s.data() );
}


// equality operator
bool AbaxCStr::operator<( const AbaxCStr &s ) const
{
	int rc = strcmp(buf_, s.data() );
	if ( rc < 0 ) return 1;
	else return 0;
}
bool AbaxCStr::operator<=( const AbaxCStr &s ) const
{
	int rc = strcmp(buf_, s.data() );
	if ( rc <= 0 ) return 1;
	else return 0;
}
bool AbaxCStr::operator>( const AbaxCStr &s ) const
{
	int rc = strcmp(buf_, s.data() );
	if ( rc > 0  ) { return 1; } else { return 0; }
}
bool AbaxCStr::operator>=( const AbaxCStr &s ) const
{
	int rc = strcmp(buf_, s.data() );
	if ( rc >= 0  ) { return 1; } else { return 0; }
}

// inequality operator
bool AbaxCStr::operator!=( const char *s) const
{
	if ( 0 == strcmp(buf_, s) ) {
		return 0;
	} else {
		return 1;
	}
}

// inequality operator
bool AbaxCStr::operator!=( const AbaxCStr &s) const
{
	if ( 0 == strcmp(buf_, s.data() ) ) {
		return 0;
	} else {
		return 1;
	}
}

// conversion operator to int
int AbaxCStr::toInt() const
{
	return atoi(buf_);
}
long  AbaxCStr::toLong() const
{
	return strtoul(buf_, (char**)NULL, 10 );

}

// end 2: begin and end
// end 1: end blanks only
AbaxCStr& AbaxCStr::trimSpaces( int end )
{
	if ( length_ < 1 ) return *this;

	// remove trailing blanks first
	char c;
	int i;
	for( i=length_-1; i>=0; i--) {
		c = buf_[i];
		if ( c == ' ' || c == '\t' || c == '\r' || c == '\n' ) {
			buf_[i]='\0';
		} else {
			break;
		}
	}

	length_ = i+1;
	if ( end == 1 ) {
		return *this;
	}

	// leave beginning blanks then
	char *pstart=0;
	int  nbeg=0;
	for( char *p=buf_; *p != '\0'; p++) {
		if ( *p == ' ' || *p == '\t' || *p == '\r' || *p == '\n' ) {
			nbeg ++;
		} else {
			pstart = p;
			break;
		}
	}

	length_ = length_ - nbeg;
	AbaxCStr *ps = new AbaxCStr(length_ );
	memcpy(ps->buf_, pstart, length_);

	if ( buf_ ) free(buf_);
	buf_ = ps->buf_;
	buf_[length_] = '\0';
	nseg_ = ps->nseg_;
	return *this;
}

AbaxCStr& AbaxCStr::trimnull()
{
	// trim end nulls
	for ( int i = length_-1; i>=0; --i ) {
		if ( buf_[i] == '\0' ) { --length_; }
	}
}

AbaxCStr& AbaxCStr::trimChar( char C )
{
	if ( length_ < 1 ) return *this;

	// remove trailing blanks first
	int i = length_-1;
	if ( C == buf_[i] ) {
		buf_[i]='\0';
		--length_;
	}

	if ( buf_[0] != C ) {
		return *this;
	}

	--length_; 
	AbaxCStr *ps = new AbaxCStr(length_+1 );
	memcpy(ps->buf_, buf_+1, length_);

	if ( buf_ ) free(buf_);
	buf_ = ps->buf_;
	buf_[length_] = '\0';
	nseg_ = ps->nseg_;
	return *this;
}


AbaxCStr & AbaxCStr::trimEndChar( char chr )
{
	if ( length_ < 1 ) return *this;
	// remove trailing blanks 
	char c;
	int i;
	for( i=length_-1; i>=0; i--) {
		c = buf_[i];
		if ( c == chr || c== ' ' || c == '\t' || c == '\r' || c == '\n' ) {
			buf_[i]='\0';
		} else {
			break;
		}
	}
	length_ = i+1;
	return *this;
}

void AbaxCStr::remove( char c )
{
	if ( !strchr(buf_, c) ) return;
	AbaxCStr *newStr = new AbaxCStr();
	int nlen = 0;
	for (char *p=buf_; *p != '\0'; p++) {
		if ( *p == c ) { }
		else {
			*newStr += *p;
			nlen ++;
		}
	}

	if ( buf_ ) free(buf_);
	buf_ = newStr->buf_;
	nseg_ = newStr->nseg_;
	length_ = nlen;
}

void AbaxCStr::remove( const char *chset )
{
	AbaxCStr *newStr = new AbaxCStr();
	int nlen = 0;
	for (char *p=buf_; *p != '\0'; p++) {
		if ( strchr( chset, *p ) ) {
			// left out
		} else {
			*newStr += *p;
			nlen ++;
		}
	}

	if ( buf_ ) free(buf_);
	buf_ = newStr->buf_;
	nseg_ = newStr->nseg_;
	length_ = nlen;
}


void AbaxCStr::replace( char old, char newc )
{
	for ( int i=0; i < length_; ++i ) {
		if ( buf_[i] == old ) {
			buf_[i] = newc;
		}
	}
}

void AbaxCStr::replace( const char *chset, char newc )
{
	for (char *old=(char*) chset; *old != '\0'; old++) {
		for (char *p=buf_; *p != '\0'; p++) {
			if ( *p == *old ) {
				*p = newc;
			}
		}
	}
}

void AbaxCStr::removeString( const char *oldstr ) 
{
	replace( oldstr, "" ); 
}

// replace whole oldstr in AbaxCStr by newstr
void AbaxCStr::replace( const char *oldstr, const char *newstr)
{
	AbaxCStr *pnew = new AbaxCStr();
	int  n;
	int  olen = strlen(oldstr);

	char *p = buf_;
	while ( *p != '\0' ) {
		if ( 0 == strncmp(p, oldstr, olen) ) {
			*pnew += newstr;
			p += olen;
		} else {
			*pnew += *p++;
		}
	}

	length_ = pnew->length();
	if ( buf_ ) free(buf_);
	buf_ = pnew->buf_;
	nseg_ = pnew->nseg_;
}

void AbaxCStr::initmem( int size )
{
	nseg_ = int(size/ASTRSIZ) + 1;
	buf_= new char[nseg_ * ASTRSIZ];
	memset( (void*)buf_, 0, nseg_ * ASTRSIZ );
	length_=0;
}

// private
void AbaxCStr::allocMoreMemory( int len2 )
{
	int newLen = length_ + len2;

	if (  newLen > nseg_ * ASTRSIZ )
	{
		int  newSegs = int( newLen/ASTRSIZ ) +1;
		buf_ = (char*)realloc( buf_, newSegs * ASTRSIZ );
		memset(buf_+length_, 0, len2 );
		nseg_ = newSegs;
	}
}

// private
void AbaxCStr::replaceself( char *databuf, int datalen ) 
{
	int i=0;
	if (  datalen  > nseg_ * ASTRSIZ )
	{
		int  newSegs = int( datalen/ASTRSIZ ) +1;
		char *pnew = new char[ newSegs * ASTRSIZ ];
		memcpy(pnew, databuf, datalen );
		if ( buf_ ) free(buf_);
		buf_ = pnew;
		nseg_ = newSegs;
		length_ = datalen;
		buf_[length_] = '\0';
	}
	else
	{
		memcpy(buf_, databuf, datalen );
		length_ = datalen;
		buf_[length_] = '\0';
	}
}

// how many times c occur in the string
int AbaxCStr::countChars( char c) const
{
	int n=0;
	for ( int i=0; i < length_; ++i ) {
		if ( buf_[i] == c ) ++n;
	}
	return n;
}

AbaxCStr AbaxCStr::substr(int start, int len ) const
{
	return AbaxCStr(buf_+start, len );
}


// 0 1 2 3 4 5
AbaxCStr AbaxCStr::substr(int start ) const
{
	return AbaxCStr(buf_+start, length_ - start );
}

void AbaxCStr::destroy()
{
	if ( buf_ ) free( buf_ );
	buf_ = NULL; 
	length_=0;
}

AbaxCStr & AbaxCStr::pad0()
{
	if ( length_ == 1 )
	{
		// make copy of buf_
		int len = length_;
		char *pc = (char*)malloc(len + 1);
		memcpy(pc, buf_, len);

		allocMoreMemory(1);

		buf_[0] = '0';
		memcpy( buf_+1, pc, len); 
		free(pc);
		buf_[length_] = '\0';
	}

	return *this;
}

void AbaxCStr::toUpper() 
{
	for(int i=0; i< length_; i++) {
		buf_[i] = toupper( buf_[i] );
	}
}

void AbaxCStr::toLower()
{
	for(int i=0; i< length_; i++) {
		buf_[i] = tolower( buf_[i] );
	}
}

int AbaxCStr::operator[] (int i) const
{
	if ( i<length_ ) {
		return buf_[i];
	} else {
		return 0;
	}
}

// non-member
AbaxCStr operator+(const AbaxCStr &s1, const AbaxCStr &s2 ) 
{
	AbaxCStr res = s1;
	res += s2;
	return res;
}

AbaxCStr operator+(const char *s1, const AbaxCStr &s2 )
{
	AbaxCStr res = s1;
	res += s2;
	return res;
}


// public
int AbaxCStr::caseEqual(const char *str) const
{
	int ilen = strlen(str);
	if ( ilen != length_ ) return 0;

	int  t=0;
	for ( int i=0; i<length_; i++) {
		if ( toupper(buf_[i]) != toupper( str[i] ) ) {
			return 0;
		}
	}
	return 1;
}

int AbaxCStr::numPunct() const
{
	int c = 0;
	for ( int i=0; i<length_; i++) {
		if ( ::ispunct(buf_[i]) )  ++c;
	}
	return c;
}

size_t AbaxCStr::find( int c) const
{
	char *p = strchr( buf_ , c );
	if ( ! p ) return -1;
	return ( p - buf_ );
}

void AbaxCStr::print() const
{
	printf("AbaxCStr::print() length_=%d:\n[", length_ );
	for ( int i=0; i < length_; ++i ) {
		if ( buf_[i] == '\0' ) {
			putchar( '@' );
		} else {
			putchar( buf_[i] );
		}
	}
	putchar(']');
	putchar('\n');
	putchar('\n');
}

AbaxCStr&  AbaxCStr::trimEndZeros()
{
	//printf("s2083  AbaxCStr::trimEndZeros ... %s\n", buf_ );

	if ( length_ < 1 ) return *this;
	if ( ! strchr( buf_, '.') ) return *this;
	if ( ! buf_ ) return *this;

	char *buf= new char[nseg_ * ASTRSIZ];
	memset( (void*)buf, 0, nseg_ * ASTRSIZ );

    int start=0;
    bool leadzero = false;
    int len = 0;
    if ( buf_[0] == '+' || buf_[0] == '-' ) {
		buf[len++] = buf_[0];
        start = 1;
    }

	if ( buf_[start] == '0' && buf_[start+1] != '\0' ) leadzero = true;
	for ( int i = start; i < length_; ++i ) {
		if ( buf_[i] != '0' || ( buf_[i] == '0' && buf_[i+1] == '.' ) ) {
			leadzero = false;
		}
		if ( ! leadzero ) {
			buf[len++] = buf_[i];
		}
	}

	//printf("a8080 len=%d buf=[%s]\n", len, buf );
    if ( len < 1 ) {
		buf[0] = '0'; buf[1] = '\0'; len=1;
		free( buf_ );
		buf_ = buf;
		length_ = len;
		return *this;
	}

	char *p = buf+len-1;
	while ( p >= buf+1 ) {
		if ( *(p-1) != '.' && *p == '0' ) {
			*p = '\0';
			--len;
		} else {
			break;
		}
		--p;
	}

	if ( buf[0] == '.' && buf[1] == '\0' ) { buf[0] = '0'; len=1; }
	if ( buf[0] == '\0' || len==0 ) {  buf[0] = '0'; buf[1] = '\0'; len=1; }

	free( buf_ );
	buf_ = buf;
	length_ = len;
	return *this;
}

AbaxCStr AbaxCStr::firstToken( char sep )
{
	if ( length_ < 1 ) return "";
	char *p = buf_;
	while ( *p != sep && *p != '\0' ) ++p;
	return AbaxCStr(buf_, p-buf_);
}

const char * AbaxCStr::secondTokenStart( char sep )
{
	if ( length_ < 1 ) return NULL;
	char *p = buf_;
	while ( *p != sep && *p != '\0' ) ++p;
	if ( *p == '\0' ) return NULL;
	while ( *p == sep ) ++p;
	return p;
}

// buf_ "anan(kddddd)llsk"
// startc='('  endc=')'
AbaxCStr AbaxCStr::substr( char startc, char endc )
{
	const char *p = buf_;
	while ( *p != startc && *p != '\0' ) ++p;
	if ( *p == '\0' ) return "";
	++p;
	if ( *p == '\0' ) return "";

	const char *q = p;
	while ( *q != endc && *q != '\0' ) ++q;
	if ( *q == '\0' ) return p;  // no endc found
	return AbaxCStr(p, q-p);
}

// if first byte is zero. i.e. C-string size is 0
bool  AbaxCStr::isNull() const
{
	if ( buf_[0] == '\0' ) return true;
	return false;
}

// if first byte is not zero. i.e. C-string size is >0
bool  AbaxCStr::isNotNull() const
{
	if ( buf_[0] != '\0' ) return true;
	return false;
}

bool AbaxCStr::containsChar( char c )
{
	if ( buf_[0] == '\0' ) return false;
	if ( strchr( buf_, c ) ) return true;
	return false;
}

double AbaxCStr::tof() const
{
	if ( ! buf_ || buf_[0] == '\0' ) return 0.0;
	return atof( buf_ );
}

bool AbaxCStr::isNumeric() const
{
	for ( int i=0; i < length_; ++i ) {
		if ( ! isdigit(buf_[i]) && buf_[i] != '.' ) {
			return false;
		}
	}
	return true;
}
