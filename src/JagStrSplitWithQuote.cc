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
#include <string.h>
#include <JagStrSplitWithQuote.h>
#include <JaguarCPPClient.h>
#include <JagUtil.h>

JagStrSplitWithQuote::JagStrSplitWithQuote()
{
	list_ = NULL;
	length_ = 0;
}

JagStrSplitWithQuote::JagStrSplitWithQuote(const char* str, char sep)
{
	list_ = NULL;
	length_ = 0;
	init( str, sep );
}

// skip ( ) and ' "
void JagStrSplitWithQuote::init(const char* str, char sep)
{
	destroy();

	list_ = NULL;
	length_ = 0;

	sep_ = sep;
	if ( str == NULL || *str == '\0' ) return;

	int len = strchrnum(str, sep);
	++len;
	int tokens = 0;
	int parencnt = 0;

	JagSplitPosition *pos[len];
	for ( int i = 0; i < len; ++i ) {
		pos[i] = new JagSplitPosition();
	}

	// char *parsestart[len], *savestart[len], *saveend[len], *trackpos, *token, *pp;
	char *trackpos, *token, *pp;
	token = (char*)str;
	pos[0]->parsestart = trackpos = token;
	// find number of tokens separated by sep, using r_strtok_r
	// aaa=ccc&b=xxx
	// "aaa=" tokens=2
	// "=" tokens=2
	// "aaa=bb=" tokens=3
	
	while ( true ) {
		if ( *trackpos == '\0' ) { 
			pp = trackpos - 1;
			//while ( *pp == ' ' ) --pp;
			while ( isspace(*pp) ) --pp;
			++pp;
			// saveend[tokens] = pp;
			pos[tokens]->saveend = pp;
			++tokens; 
			break;
		} else if ( *trackpos == '\'' && (trackpos = jumptoEndQuote(trackpos)) && *trackpos != '\0' ) ++trackpos;
		else if ( *trackpos == '"' && (trackpos = jumptoEndQuote(trackpos)) && *trackpos != '\0' ) ++trackpos;
		else if ( *trackpos == '(' ) {
			++parencnt;
			++trackpos;
			while ( true ) {
				if ( *trackpos == '\0' ) break;
				if ( *trackpos == ')' ) {
					--parencnt;
					if ( !parencnt ) break;
					else ++trackpos;
				} else if ( *trackpos == '\'' && (trackpos = jumptoEndQuote(trackpos)) && *trackpos != '\0' ) ++trackpos;
				else if ( *trackpos == '"' && (trackpos = jumptoEndQuote(trackpos)) && *trackpos != '\0' ) ++trackpos;
				else if ( *trackpos == '(' ) {
					++parencnt;
					++trackpos;
				//} else if ( *trackpos != ')' && *trackpos != '\0' ) ++trackpos;
				} else if ( *trackpos != '\0' ) ++trackpos;
			}
			if ( *trackpos == ')' ) ++trackpos;
		} else if ( *trackpos == sep ) {
			pp = trackpos - 1;
			//while ( *pp == ' ' ) --pp;
			while ( isspace(*pp) ) --pp;
			++pp;
			// saveend[tokens] = pp;
			pos[tokens]->saveend = pp;
			// *pp = '\0';
			// *trackpos = '\0';
			++trackpos;
			++tokens;
			// parsestart[tokens] = trackpos;
			pos[tokens]->parsestart = trackpos;
		} else ++trackpos;
	}

	len = tokens;
	tokens = 0;
	for ( int i = 0; i < len; ++i ) {
		// trackpos = parsestart[i];
		trackpos = pos[i]->parsestart;
		while( isspace(*trackpos) ) ++trackpos;
		if ( *trackpos != '\0' ) {
			// savestart[tokens] = trackpos;
			// saveend[tokens] = saveend[i];
			pos[tokens]->savestart = trackpos;
			pos[tokens]->saveend = pos[i]->saveend;
			++tokens;
		}
	}

	list_ = new Jstr[tokens];
	length_ = tokens;

	for ( int i = 0; i < length_; ++i ) {
		if ( pos[i]->saveend - pos[i]->savestart <= 0 ) {
			// list_[i] == ""; // bug
			list_[i] = "";
		} else {
		 	// list_[i] = Jstr(savestart[i], saveend[i]-savestart[i]);
		 	list_[i] = Jstr(pos[i]->savestart, pos[i]->saveend - pos[i]->savestart );
		}
	}

	// free ( token );
	for ( int i = 0; i < len; ++i ) {
		delete pos[i];
	}
}

JagStrSplitWithQuote::~JagStrSplitWithQuote()
{
	destroy();
}

void JagStrSplitWithQuote::destroy()
{
	if ( list_ ) {
		delete [] list_;
	}
	list_ = NULL;
	length_=0;
}


const Jstr& JagStrSplitWithQuote::operator[](int i ) const
{
	if ( i < length_ )
	{
		return list_[i];
	}
	else
	{
		return list_[ length_ -1];
	}
}

abaxint JagStrSplitWithQuote::length() const
{
	return length_;
}

bool JagStrSplitWithQuote::exists(const Jstr &token) const
{
	for (int i=0; i < length_; i++) {
		if ( 0==strcmp( token.c_str(), list_[i].c_str() ) ) {
		    return true;
		}
	}

	return false;
}

void JagStrSplitWithQuote::print()
{
	for (int i=0; i < length_; i++)
	{
		printf("%d=[%s]\n", i, list_[i].c_str() );
	}
}
