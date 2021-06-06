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
#include <JagMinMax.h>
#include <JagUtil.h>

JagMinMax::JagMinMax() 
{
	minbuf = NULL;
	maxbuf = NULL;
	type = " ";
	buflen = offset = length = sig = 0;
	pointTo = true;
}
	
JagMinMax::~JagMinMax() 
{
	if ( pointTo )  return;

	if ( minbuf ) {
		free ( minbuf );
		minbuf = NULL;
	}

	if ( maxbuf ) {
		free ( maxbuf );
		maxbuf = NULL;
	}
}

JagMinMax& JagMinMax::operator=( const JagMinMax& o )
{
	abort();
	return *this;
}

JagMinMax::JagMinMax( const JagMinMax& o )
{
	abort();
}
	
int JagMinMax::setbuflen ( const int klen ) 
{
	if ( ! pointTo ) {
		if ( minbuf ) { free ( minbuf ); }
	} else {
	}

	minbuf = (char*)malloc(klen+1);
	memset(minbuf, 0, klen+1);

	if ( ! pointTo ) {
		if ( maxbuf ) { free( maxbuf ); }
	}

	maxbuf = (char*)malloc(klen+1);
	memset(maxbuf, 255, klen);

	maxbuf[klen] = '\0';
	buflen = klen;

	pointTo = false;
	return 1;
}

void JagMinMax::printc()
{
	prints(("s202228 JagMinMax::printc() this=%0x buflen=%d\n", this, buflen ));
	prints(("minbuf: "));
	for (int i = 0; i < buflen; ++i ) {
		prints(("%c ", minbuf[i] ));
	}
	prints(("\n"));
	prints(("maxbuf: "));
	for (int i = 0; i < buflen; ++i ) {
		prints(("%c ", maxbuf[i] ));
	}
	prints(("\n"));
}

void JagMinMax::printd()
{
	prints(("s202228 JagMinMax::printd() this=%0x buflen=%d\n", this, buflen ));
	prints(("minbuf: "));
	for (int i = 0; i < buflen; ++i ) {
		prints(("%d ", minbuf[i] ));
	}
	prints(("\n"));
	prints(("maxbuf: "));
	for (int i = 0; i < buflen; ++i ) {
		prints(("%d ", maxbuf[i] ));
	}
	prints(("\n"));
}
