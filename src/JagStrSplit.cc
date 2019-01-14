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
#include <string.h>
#include <JagStrSplit.h>

JagStrSplit::JagStrSplit()
{
	list_ = NULL;
	length_ = 0;
	start_ = 0;
	_NULL = "";
	pdata_ = NULL;
}

JagStrSplit::JagStrSplit(const AbaxDataString& str, char sep, bool ignoreregion )
{
	list_ = NULL;
	length_ = 0;
	start_ = 0;
	_NULL = "";
	init( str.c_str(), sep, ignoreregion );
}

JagStrSplit::JagStrSplit(const char *str, char sep, bool ignoreregion )
{
	list_ = NULL;
	length_ = 0;
	start_ = 0;
	_NULL = "";
	init( str, sep, ignoreregion );
}

void JagStrSplit::init(const char *str, char sep, bool ignoreregion )
{
	destroy();
	char *p;

	// pdata_ = NULL;
	pdata_ = str;

	list_ = NULL;
	length_ = 0;

	sep_ = sep;
	if ( str == NULL  || *str == '\0' ) return;

	char *start, *end, *ps;
	int len;
	int tokens=1;

	// find number of tokens separated by sep
	// aaa=ccc&b=xxx
	// "aaa=" tokens=2
	// "=" tokens=2
	// "aaa=bb=" tokens=3
	// p = (char*) str.c_str();
	p = (char*) str;
	if ( ignoreregion ) { while ( *p == sep_ ) { ++p; } }

	while ( *p != '\0' ) {
		if ( *p == sep_ ) {
			// several consecutive seps are counted as one
			if ( ignoreregion ) {
				while( *p == sep_ ) ++p;
				if ( *p == '\0' ) break;
			} 
			tokens ++;
		}
		++p;
	}

	// printf("c8383 tokens=%d\n", tokens );

	list_ = new AbaxDataString[tokens];
	length_ = tokens;

	// start = ps = (char*) str.c_str();
	start = ps = (char*) str;
	if ( ignoreregion ) {
		while ( *ps == sep_ ) { ++start; ++ps; }
	}

	end = start;
	int i = 0;
	while(  i <= tokens -1 )
	{
		// move start to begining of non-blank char
		for( end=start; *end != sep_ && *end != '\0'; end++ ) { ; }
		
		// end either points to sep_ or NULL 
		len= end-start;
		if ( len == 0 ) {
			list_[i] = "";
		} else {
			list_[i] = AbaxDataString(start, len);
		}

		i++;
		if ( *end == '\0' ) {
			break;
		}

		end++;
		if ( ignoreregion ) {
			while ( *end != '\0' && *end == sep_ ) ++end;
		}
		start = end;
	}

}

JagStrSplit::~JagStrSplit()
{
	destroy();
}

void JagStrSplit::destroy()
{
	if ( list_ ) {
		delete [] list_;
	}
	list_ = NULL;
	length_=0;
}

const AbaxDataString& JagStrSplit::operator[](int i ) const
{
	if ( i+start_ < 0 ) return _NULL;

	if ( i < length_ - start_ )
	{
		return list_[start_+i];
	}
	else
	{
		// return list_[ length_ -1];
		return _NULL; 
	}
}

AbaxDataString& JagStrSplit::operator[](int i ) 
{
	if ( i+start_ < 0 ) return _NULL;

	if ( i < length_ - start_ )
	{
		return list_[start_+i];
	}
	else
	{
		// return list_[ length_ -1];
		return _NULL; 
	}
}

abaxint JagStrSplit::length() const
{
	return length_ - start_;
}
abaxint JagStrSplit::size() const
{
	return length_ - start_;
}

abaxint JagStrSplit::slength() const
{
	return length_ - start_;
}

bool JagStrSplit::exists(const AbaxDataString &token) const
{
	for (int i=0; i < length_; i++) {
		if ( 0==strcmp( token.c_str(), list_[i].c_str() ) ) {
		    return true;
		}
	}

	return false;
}

bool JagStrSplit::contains(const AbaxDataString &token, AbaxDataString &rec) const
{
	const char *tok;
	for (int i=0; i < length_; i++) {
		tok = list_[i].c_str(); 
		if ( strstr( tok, token.c_str() ) ) {
		  	rec = tok;
		  	return 1;
		 }
	}

	rec = "";
	return 0;
}

void JagStrSplit::print() const
{
	printf("s3008 JagStrSplit::print():\n" );
	for (int i=0; i < length_; i++)
	{
		printf("i=%d [%s]\n", i, list_[i].c_str() );
	}
	printf("\n"); 
	fflush(stdout);
}

void JagStrSplit::printStr() const
{
	printf("s3008 JagStrSplit::printStr(): [%s]\n", pdata_ );
}

AbaxDataString& JagStrSplit::last() const
{
	return list_[ length_ -1];
}

void JagStrSplit::shift()
{
	if ( start_ <= length_-2 ) {
		++ start_;
	}
}

void JagStrSplit::back()
{
	if ( start_ <= 1 ) {
		-- start_;
	}
}

const char* JagStrSplit::c_str() const
{
	return pdata_;
}

void JagStrSplit::pointTo( const char *str )
{
	pdata_ = str;
}

