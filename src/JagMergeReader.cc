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

#include <JagMergeReader.h>
#include <JagBuffReader.h>
#include <JagUtil.h>


JagMergeReader::JagMergeReader( const JagDBMap *dbmap, const JagVector<OnefileRange> &fRange, int veclen, int keylen, int vallen, 
							    const char *minbuf, const char *maxbuf )
	:JagMergeReaderBase(dbmap, veclen, keylen, vallen, minbuf, maxbuf )
{
	_isMarkSet = false;
	findBeginPos( minbuf, maxbuf );

	if ( veclen > 0 ) {
		_buffReaderPtr = new JagBuffReaderPtr[veclen];
	} else {
		_buffReaderPtr = NULL;
	}

	for ( int i = 0; i < veclen; ++i ) {
		_buffReaderPtr[i] = new JagBuffReader( (JagDiskArrayBase*)fRange[i].darr, fRange[i].readlen, 
									 			KEYLEN, VALLEN, fRange[i].startpos, 0, fRange[i].memmax );
	}

	initHeap();
}

JagMergeReader::~JagMergeReader()
{
	if ( _buffReaderPtr ) {
		for ( int i = 0; i < _readerPtrlen; ++i ) {
			if ( _buffReaderPtr[i] ) {
				delete _buffReaderPtr[i];
				_buffReaderPtr[i] = NULL;
			}
		}
		delete [] _buffReaderPtr;
		_buffReaderPtr = NULL;
	}

}

void JagMergeReader::initHeap()
{
	if ( _pqueue ) { delete _pqueue; }
    _pqueue = new JagPriorityQueue<int,JagDBPair>(256, JAG_MINQUEUE );

	if ( this->_dbmap && this->_dbmap->size() > 0 ) {

		if ( beginPair <= endPair ) {
    		_pqueue->push( -1, beginPair );
    		currentPos = beginPos;
    		if ( currentPos == endPos ) {
    			memReadDone = true;
    		}
		} else {
    		memReadDone = true;
		}
	}

	int rc;
	jagint pos;
	for ( int i = 0; i < _readerPtrlen; ++i ) {
		_goNext[i] = 1;
	}

	for ( int i = 0; i < _readerPtrlen; ++i ) {
		rc = _buffReaderPtr[i]->getNext( _buf, pos );
		if ( rc ) {
			JagDBPair pair(_buf, KEYLEN, VALLEN );
			if ( pair <= endPair ) {
				_pqueue->push( i, pair );
				_goNext[i] = 0;
			} else {
				_goNext[i] = -1;
				++_endcnt;
			}

		} else {
			_goNext[i] = -1;
			++_endcnt;
		}
	}
}

bool JagMergeReader::getNext( char *buf )
{
	JagDBPair pair;
	int  filenum;
	jagint pos;
	int rc;

	if ( ! _pqueue->pop( filenum, pair ) ) {
		return false;
	}

	memcpy(buf, pair.key.c_str(), KEYLEN);
	memcpy(buf+KEYLEN, pair.value.c_str(), VALLEN);

	if ( filenum == -1 ) {
		if ( ! memReadDone  ) {
			prevPos = currentPos;
			//print("in getNext() prevPos: ", prevPos );
    		++currentPos;

    		JagDBPair nextpair;
    		nextpair.key = currentPos->first;
    		nextpair.value = currentPos->second;
    		_pqueue->push( -1, nextpair );
    		if ( currentPos == endPos ) {
    			memReadDone = true;
			}
		}
	} else {
		_goNext[filenum] = 1; 

		rc = _buffReaderPtr[filenum]->getNext( _buf, pos );

		if ( rc ) {
			JagDBPair nextpair(_buf, KEYLEN, VALLEN );
			if ( nextpair <= endPair ) {
				_pqueue->push( filenum, nextpair );
			} else {
				_goNext[filenum] = -1; 
			}
		} else {
			_goNext[filenum] = -1;  // end of file for this filenum
		}
	}
	return true;
}

void JagMergeReader::setRestartPos()
{
	setMemRestartPos();
	for ( int i = 0; i < _readerPtrlen; ++i ) {
		_buffReaderPtr[i]->setRestartPos();
	}
	_isMarkSet = true;
}

void JagMergeReader::moveToRestartPos()
{
	moveMemToRestartPos();
	currentPos = _restartMemPos;
	if ( ! isAtEnd( currentPos ) ) {
		JagDBPair restartPair( currentPos->first.c_str(), KEYLEN, currentPos->second.c_str(), VALLEN );
   		_pqueue->clear();
   		_pqueue->push( -1, restartPair );
	} else {
	}

	for ( int i = 0; i < _readerPtrlen; ++i ) {
		_buffReaderPtr[i]->moveToRestartPos();
	}
}

void JagMergeReader::findBeginPos( const char *minbuf, const char *maxbuf )
{
	beginPair = JagDBPair( minbuf, KEYLEN ); 
	endPair = JagDBPair( maxbuf, KEYLEN );  

	beginPos = _dbmap->getSuccOrEqual( beginPair );
	if ( isAtEnd( beginPos) ) {
		memReadDone = true;
		return;
	}

	_dbmap->iterToPair( beginPos, beginPair );

	endPos = _dbmap->getPredOrEqual( endPair ); 

	if ( isAtEnd( endPos ) ) {
		memReadDone = true;
	}

}

void JagMergeReader::setMemRestartPos()
{
	_restartMemPos = prevPos; 
	if ( isAtEnd(prevPos) ) {
		memReadDone = true;
	}
}

void JagMergeReader::moveMemToRestartPos()
{
	beginPos = _restartMemPos;
	currentPos = _restartMemPos;
	memReadDone = false;
	if ( isAtEnd(currentPos) ) {
		memReadDone = true;
	}
}

bool JagMergeReader::isAtEnd( JagFixMapIterator iter )
{
	if ( NULL == _dbmap ) return true;
	if ( _dbmap->isAtEnd(iter) ) return true;
	return false;
}

void JagMergeReader::print( const char *hdr, JagFixMapIterator iter )
{
	prt(("%0x s8827399 print %s iter=[%s][%s]\n", this, hdr, iter->first.c_str(), iter->second.c_str() ));
}

