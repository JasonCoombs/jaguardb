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
	//prt(("s7523 JagMinMax ctor this=%0x\n", this ));
}
	
JagMinMax::~JagMinMax() 
{
	//prt(("s7524 JagMinMax dtor this=%0x pointTo=%d minbuf=%0x maxbuf=%0x\n", this, pointTo, minbuf, maxbuf ));
	if ( pointTo )  return;

	if ( minbuf ) {
		//prt(("s2367 JagMinMax dtor free minbuf=%0x this=%0x\n", minbuf, this ));
		free ( minbuf );
		minbuf = NULL;
		//prt(("s2367 JagMinMax dtor done free minbuf=%0x this=%0x\n", minbuf, this ));
	}

	if ( maxbuf ) {
		//prt(("s2368 JagMinMax dtor free maxbuf=%0x this=%0x\n", maxbuf, this ));
		free ( maxbuf );
		maxbuf = NULL;
		//prt(("s2368 JagMinMax dtor done free maxbuf=%0x this=%0x\n", maxbuf, this ));
	}
}

JagMinMax& JagMinMax::operator=( const JagMinMax& o )
{
	//prt(("s7180 exception\n" ));
	abort();
	return *this;
}

JagMinMax::JagMinMax( const JagMinMax& o )
{
	//prt(("s7181 exception\n" ));
	abort();
}
	
int JagMinMax::setbuflen ( const int klen ) 
{
	if ( ! pointTo ) {
		//prt(("s2130 JagMinMax setbuflen free minbuf=%0x this=%0x\n", minbuf, this ));
		if ( minbuf ) { free ( minbuf ); }
	} else {
		//prt(("s2134 JagMinMax setbuflen pointTo is true. now setbuf  this=%0x\n", this ));
	}

	//prt(("s2357 JagMinMax::setbuflen  minbuf=%0x this=%0x\n", minbuf, this ));
	minbuf = (char*)malloc(klen+1);
	memset(minbuf, 0, klen+1);

	if ( ! pointTo ) {
		//prt(("s2330 JagMinMax setbuflen free maxbuf=%0x this=%0x\n", maxbuf, this ));
		if ( maxbuf ) { free( maxbuf ); }
	}

	maxbuf = (char*)malloc(klen+1);
	memset(maxbuf, 255, klen);

	maxbuf[klen] = '\0';
	buflen = klen;
	//prt(("s8930 JagMinMax setbuflen new minbuf=%0x maxbuf=%0x this=%0x\n", minbuf, maxbuf, this ));

	pointTo = false;
	return 1;
}
