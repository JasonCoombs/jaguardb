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
#ifndef _jag_request_h_
#define _jag_request_h_

#include <JagSession.h>

class JagRequest
{
   public:
	JagRequest() { hasReply = true; batchReply = false; doCompress = false; opcode = 0; session = NULL; dorep=true; syncDataCenter = false; }
	~JagRequest() {}
	inline JagRequest& operator=( const JagRequest& req ) 
	{	
		hasReply = req.hasReply;
		doCompress = req.doCompress;
		batchReply = req.batchReply;
		opcode = req.opcode;
		session = req.session;
		syncDataCenter = req.syncDataCenter;
		return *this;
	}

	bool hasReply;
	bool doCompress;
	bool batchReply;
	bool dorep;
	bool syncDataCenter;
	short opcode;
	JagSession *session;
};

#endif
