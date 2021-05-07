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
	prt(("s093838 JagMergeReader ctor veclen=%d\n", veclen ));
	//prt(("s300281 _dbmap.print():\n"));
	//_dbmap->print();
	//prt(("s300281 _dbmap.print() done\n"));
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
		//_goNext[i] = 1;
	}

	initHeap();
}

JagMergeReader::~JagMergeReader()
{
	prt(("s300992 JagMergeReader dtor _readerPtrlen=%d...\n", _readerPtrlen ));
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

	//prt(("s300283 _dbmap.print():\n"));
	//_dbmap->print();
}

/*** old method, not using insertBuffer
bool JagMergeReader::getNext( char *buf )
{
	jagint pos;
	return getNext( buf, pos );
}

bool JagMergeReader::getNext( char *buf, jagint &pos )
{
	if ( _cacheBuf ) {
		pos = 0;
		memcpy(buf, _cacheBuf, KEYVALLEN);
		free( _cacheBuf ); _cacheBuf = NULL;
		return true;
	}
	
	int beginpos = 0;
	int rc;
	for ( int i = 0; i < _readerPtrlen; ++i ) {
		if ( _goNext[i] == 1 ) {
			rc = _buffReaderPtr[i]->getNext( _buf+i*KEYVALLEN, pos );
			if ( !rc ) {
				_goNext[i] = -1;
				++_endcnt;
			} else {
				_goNext[i] = 0;
			}
		}
	}
	
	if ( _endcnt == _readerPtrlen ) {
		return false;
	}
	
	// get initial maxbuf from first avail buf
	for ( int i = beginpos; i < _readerPtrlen; ++i ) {
		if ( _goNext[i] != -1 ) {
			memcpy(buf, _buf+i*KEYVALLEN, KEYVALLEN);
			beginpos = i;
			break;
		}
	}
	
	// find the min buf ( while oldest file first )
	for ( int i = beginpos; i < _readerPtrlen; ++i ) {
		if ( _goNext[i] != -1 ) {
			rc = memcmp(buf, _buf+i*KEYVALLEN, KEYLEN);
			if ( rc >= 0 ) {
				memcpy(buf, _buf+i*KEYVALLEN, KEYVALLEN);
			}
		}
	}
	
	// check and set all other equal buf to getNext
	for ( int i = beginpos; i < _readerPtrlen; ++i ) {
		if ( _goNext[i] != -1 ) {
			rc = memcmp(buf, _buf+i*KEYVALLEN, KEYLEN);
			if ( rc >= 0 ) {			
				if ( _goNext[i] != -1 ) {
					_goNext[i] = 1;
				}
			}
		}
	}
	return true;
}
***/

// read first data from inertbuffer, from each of N files
void JagMergeReader::initHeap()
{
	if ( _pqueue ) { delete _pqueue; }
    _pqueue = new JagPriorityQueue<int,JagDBPair>(256, JAG_MINQUEUE );

	// memory buffer
	prt(("s300848 initHeap this=%0x _dbmap=%0x\n", this, this->_dbmap ));
	if ( this->_dbmap && this->_dbmap->size() > 0 ) {

		if ( beginPair <= endPair ) {
    		prt(("s300848 initHeap _dbmap=%0x _dbmap->size()=%d\n", this->_dbmap, this->_dbmap->size() ));
    		_pqueue->push( -1, beginPair );
    		// -1 mean memory
    		//prt(("%0x s830188 initHeap push -1 [%s][%s]\n", this, beginPair.key.c_str(),  beginPair.value.c_str() ));
    		currentPos = beginPos;
    		if ( currentPos == endPos ) {
    			memReadDone = true;
    		}
			//prevPos = currentPos;  
			//print("prevPos: ", prevPos);
    		//++ currentPos;   
		} else {
			prt(("s33302 beginPair <= endPair false, memReadDone\n" ));
    		memReadDone = true;
		}
	}
   	// memReadDone = true; // test only 
	// prt(("s183728 handle here\n"));

	// N files
	// _goNext[i] == 1 files should be read next
	// _goNext[i] == -1 files already EOF 
	// _goNext[i] == 0 files not already EOF not to be read next
	int rc;
	jagint pos;
	for ( int i = 0; i < _readerPtrlen; ++i ) {
		_goNext[i] = 1;
	}

	//prt(("%0x s102938 _readerPtrlen=%d\n", this, _readerPtrlen ));
	for ( int i = 0; i < _readerPtrlen; ++i ) {
		//rc = _buffReaderPtr[i]->getNext( _buf+i*KEYVALLEN, pos );
		rc = _buffReaderPtr[i]->getNext( _buf, pos );
		//prt(("s293016 _buffReaderPtr(%d) getNext rc=%d\n", i, rc ));
		if ( rc ) {
			JagDBPair pair(_buf, KEYLEN, VALLEN );
			if ( pair <= endPair ) {
				_pqueue->push( i, pair );
				_goNext[i] = 0;
				//prt(("s210928 push filenum=%d pair=[%s][%s]\n", i, pair.key.c_str(), pair.value.c_str() )); 
			} else {
				_goNext[i] = -1;  // end of data for this filenum
				++_endcnt;
			}

		} else {
			_goNext[i] = -1;  // end of file for this filenum
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

	prt(("%0x s29300 getNext pop... \n", this ));
	if ( ! _pqueue->pop( filenum, pair ) ) {
		prt(("s29300 getNext pop got false return false\n"));
		return false;
		// break;
	}

	//prt(("%0x s29300 popped filenum=%d one pair [%s][%s]\n", this, filenum, pair.key.c_str(), pair.value.c_str() ));
	//use pair
	memcpy(buf, pair.key.c_str(), KEYLEN);
	memcpy(buf+KEYLEN, pair.value.c_str(), VALLEN);
	//prt(("%0x s29305 filenum=%d memReadDone=%d buf=[%s][%s]\n", this, filenum, memReadDone, buf, buf+KEYLEN ));
	//prt(("%0x s29306 filenum=%d pair.key=[%s] pair.value=[%s]\n", this, filenum, pair.key.c_str(), pair.value.c_str() ));

	// push next item
	if ( filenum == -1 ) {
		if ( ! memReadDone  ) {
			prevPos = currentPos;
			print("in getNext() prevPos: ", prevPos );
    		++currentPos;

    		JagDBPair nextpair;
    		nextpair.key = currentPos->first;
    		nextpair.value = currentPos->second;
    		_pqueue->push( -1, nextpair );
			//prt(("%0x from insertbuffer memory pushed pair=[%s][%s]\n", this, nextpair.key.c_str(), nextpair.value.c_str() ));
    		if ( currentPos == endPos ) {
    			memReadDone = true;
    			//prt(("%0x s29302 currentPos == endPos memReadDone set to true\n", this ));
			}
		}
	} else {
		// get next item from filenum
		_goNext[filenum] = 1; 

		rc = _buffReaderPtr[filenum]->getNext( _buf, pos );

		//prt(("s400928 getNext item from filenum=%d rc=%d\n", filenum, rc ));
		if ( rc ) {
			JagDBPair nextpair(_buf, KEYLEN, VALLEN );
			if ( nextpair <= endPair ) {
				_pqueue->push( filenum, nextpair );
				//prt(("s221101 _buffReaderPtr getNext true filenum=%d and nextpair=[%s] is <= endPair=[%s] good\n", filenum, nextpair.key.c_str(), endPair.key.c_str() ));
			} else {
				_goNext[filenum] = -1;  // end of data for this filenum
				//prt(("s222102 _buffReaderPtr getNext true filenum=%d but nextpair=[%s] is > endPair=[%s] end\n", filenum, nextpair.key.c_str(), endPair.key.c_str() ));
			}
		} else {
			_goNext[filenum] = -1;  // end of file for this filenum
			//prt(("s222303 _buffReaderPtr filenum=%d getNext rc false no more data\n", filenum ));
		}
	}
	return true;
}

#if 0
void JagMergeReader::initHeap()
{
	if ( _pqueue ) { delete _pqueue; }
    _pqueue = new JagPriorityQueue<int,JagDBPair>(256, JAG_MINQUEUE );
	setToEnd( currentPos );

	/**
	// memory buffer
	prt(("s300848 initHeap this=%0x _dbmap=%0x\n", this, this->_dbmap ));
	if ( this->_dbmap && this->_dbmap->size() > 0 ) {
		if ( beginPair <= endPair ) {
    		prt(("s300848 initHeap _dbmap=%0x _dbmap->size()=%d\n", this->_dbmap, this->_dbmap->size() ));
    		_pqueue->push( -1, beginPair );
    		// -1 mean memory
    		currentPos = beginPos;
    		if ( currentPos == endPos ) {
    			memReadDone = true;
    		}
    		//++ currentPos;
    		prt(("s830188 initHeap push -1 [%s]\n", beginPair.key.c_str() ));
		} else {
    		memReadDone = true;
		}
	}
   	// memReadDone = true; // test only 
	// prt(("s183728 handle here\n"));

	// N files
	// _goNext[i] == 1 files should be read next
	// _goNext[i] == -1 files already EOF 
	// _goNext[i] == 0 files not already EOF not to be read next
	int rc;
	jagint pos;
	for ( int i = 0; i < _readerPtrlen; ++i ) {
		_goNext[i] = 1;
	}

	prt(("s102938 _readerPtrlen=%d\n", _readerPtrlen ));
	for ( int i = 0; i < _readerPtrlen; ++i ) {
		//rc = _buffReaderPtr[i]->getNext( _buf+i*KEYVALLEN, pos );
		rc = _buffReaderPtr[i]->getNext( _buf, pos );
		prt(("s293016 _buffReaderPtr(%d) getNext rc=%d\n", i, rc ));
		if ( rc ) {
			JagDBPair pair(_buf, KEYLEN, VALLEN );
			if ( pair <= endPair ) {
				_pqueue->push( i, pair );
				_goNext[i] = 0;
				prt(("s210928 push pair i=%d\n", i )); 
			} else {
				_goNext[i] = -1;  // end of data for this filenum
				++_endcnt;
			}

		} else {
			_goNext[i] = -1;  // end of file for this filenum
			++_endcnt;
		}
	}
	**/
}

bool JagMergeReader::getNext( char *buf )
{
	JagDBPair pair;
	int  filenum;
	jagint pos;
	int rc;

	// push next item
	if ( filenum == -1 ) {
		if ( ! memReadDone  ) {
			if ( isAtEnd( currentPos ) ) {
				currentPos = beginPos;
			} else {
				++ currentPos;
			}

    		JagDBPair nextpair;
    		nextpair.key = currentPos->first;
    		nextpair.value = currentPos->second;
    		_pqueue->push( -1, nextpair );
    		prt(("s29301 push -1 %s\n", nextpair.key.c_str() ));
    		if ( currentPos == endPos ) {
    			memReadDone = true;
			}
		}
	} else {
		// get next item from filenum
		_goNext[filenum] = 1; 
		rc = _buffReaderPtr[filenum]->getNext( _buf, pos );
		prt(("s400928 get next item from filenum=%d rc=%d buf=[%s]\n", filenum, rc, buf ));
		if ( rc ) {
			JagDBPair nextpair(_buf, KEYLEN, VALLEN );
			if ( nextpair <= endPair ) {
				_pqueue->push( filenum, nextpair );
			} else {
				_goNext[filenum] = -1;  // end of data for this filenum
			}
		} else {
			_goNext[filenum] = -1;  // end of file for this filenum
		}
	}


	prt(("%0x s29300 getNext pop... \n", this ));
	if ( ! _pqueue->pop( filenum, pair ) ) {
		prt(("%0x s29300 getNext pop got false return false\n", this ));
		return false;
		// break;
	}

	// got popped data
	prt(("%0x s29300 popped one pair %s\n", this, pair.key.c_str() ));
	//use pair
	memcpy(buf, pair.key.c_str(), KEYLEN);
	memcpy(buf+KEYLEN, pair.value.c_str(), VALLEN);
	prt(("%0x s29305 filenum=%d memReadDone=%d buf=[%s]\n", this, filenum, memReadDone, buf ));
	prt(("%0x s29306 filenum=%d pair.key=[%s] pair.value=[%s]\n", this, filenum, pair.key.c_str(), pair.value.c_str() ));
	return true;
}
#endif


/***
// setup (remember) restart position for each buffreader ( as current pos when calling this method )
bool JagMergeReader::setRestartPos()
{
	if ( _setRestartPos ) {
		prt(("s4027613 JagMergeReader setRestartPos() already set, return false\n"));
		return false;
	}

	prt(("s4027614 JagMergeReader setMemRestartPos() _readerPtrlen=%d ... \n", _readerPtrlen));
	setMemRestartPos();

	for ( int i = 0; i < _readerPtrlen; ++i ) {
		prt(("s207882 _buffReaderPtr[%d]->setRestartPos()\n"));
		_buffReaderPtr[i]->setRestartPos();
	}
	_setRestartPos = 1;	
	return true;
}

// Move position to the restart position set by the above method (setRestartPos)
bool JagMergeReader::moveToRestartPos()
{
	// if not set, just return
	if ( !_setRestartPos ) return false;

	moveMemToRestartPos();

	for ( int i = 0; i < _readerPtrlen; ++i ) {
		_buffReaderPtr[i]->moveToRestartPos();
		if ( _goNext[i] < 0 ) --_endcnt;
		_goNext[i] = 1;
	}
	_setRestartPos = 0;
	return true;
}

// remove marking of positon is set. ie.e. no set positon to move to
void JagMergeReader::setClearRestartPosFlag() 
{ 
	_setRestartPos = 0; 

	prt(("s2348062 JagMergeReader::setClearRestartPosFlag() setMemClearRestartPosFlag _readerPtrlen=%d...\n", _readerPtrlen));
	setMemClearRestartPosFlag();

	for ( int i = 0; i < _readerPtrlen; ++i ) {
		_buffReaderPtr[i]->setClearRestartPosFlag();
	}
}
***/

void JagMergeReader::setRestartPos()
{
	prt(("%0x s4027614 JagMergeReader setMemRestartPos() _readerPtrlen=%d ... \n", this, _readerPtrlen));
	setMemRestartPos();
	for ( int i = 0; i < _readerPtrlen; ++i ) {
		prt(("s207882 _buffReaderPtr[%d]->setRestartPos()\n"));
		_buffReaderPtr[i]->setRestartPos();
	}
	_isMarkSet = true;
}

// Move position to the restart position set by the above method (setRestartPos)
void JagMergeReader::moveToRestartPos()
{
	// debug
	//if ( !_isMarkSet ) return;

	moveMemToRestartPos();
	//initHeap();
	//prt(("s029283 _pqueue-push beginPair\n"));
	currentPos = _restartMemPos;
	if ( ! isAtEnd( currentPos ) ) {
		prt(("%0x currentPos not atend, push currentPos.pair=[%s][%s]\n",  this, currentPos->first.c_str(), currentPos->second.c_str() ));
		JagDBPair restartPair( currentPos->first.c_str(), KEYLEN, currentPos->second.c_str(), VALLEN );
   		_pqueue->clear();
		//prt(("%x in moveToRestartPos() pushed restartPair=[%s][%s]\n",  this, restartPair.key.c_str(), restartPair.value.c_str() ));
   		_pqueue->push( -1, restartPair );
	} else {
		prt(("%0x in moveToRestartPos(), currentPos is at end. no push currentPos.pair\n", this ));
	}

	for ( int i = 0; i < _readerPtrlen; ++i ) {
		_buffReaderPtr[i]->moveToRestartPos();
	}
}

void JagMergeReader::findBeginPos( const char *minbuf, const char *maxbuf )
{
	//prt(("s300323 findBeginEndPos minbuf=[%s] maxbuf=[%s]\n", minbuf, maxbuf ));
	beginPair = JagDBPair( minbuf, KEYLEN ); // can be empty
	endPair = JagDBPair( maxbuf, KEYLEN );  // can be 255.....

	beginPos = _dbmap->getSuccOrEqual( beginPair ); //  can be end()
	if ( isAtEnd( beginPos) ) {
		memReadDone = true;
		return;
	}

	_dbmap->iterToPair( beginPos, beginPair );
	//prt(("s402934 beginPair=[%s]\n", beginPair.key.c_str() ));

	endPos = _dbmap->getPredOrEqual( endPair );   // can be end()
	//_dbmap->iterToPair( endPos, endPair );
	//prt(("s402935 endPair=[%s]\n", endPair.key.c_str() ));

	//if ( isAtEnd( beginPos) && isAtEnd( endPos ) ) 
	if ( isAtEnd( endPos ) ) {
		memReadDone = true;
	}

}

void JagMergeReader::setMemRestartPos()
{
	_restartMemPos = prevPos; 
	prt(("%0x s3339612 setMemRestartPos() _restartMemPos=prevPos=[%s][%s]\n", this, _restartMemPos->first.c_str(), _restartMemPos->second.c_str() ));
	if ( isAtEnd(prevPos) ) {
		prt(("%0x s33338772 setMemRestartPos isAtEnd() true\n", this ));
		memReadDone = true;
	}
}

void JagMergeReader::moveMemToRestartPos()
{
	prt(("%0x s3339613 moveMemToRestartPos()\n", this ));
	beginPos = _restartMemPos;
	currentPos = _restartMemPos;
	memReadDone = false;
	if ( isAtEnd(currentPos) ) {
		prt(("%0x s33338774 moveMemToRestartPos isAtEnd() true\n", this ));
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

