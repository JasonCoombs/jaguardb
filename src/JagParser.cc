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
#include <JagParser.h>
#include <JagFileMgr.h>
#include <base64.h>
#include <JagTime.h>
#include "rapidjson/document.h"
#include "rapidjson/writer.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/error/en.h"


JagParser::JagParser( void *obj, bool isCli )
{
    _obj = obj;
	_isCli = isCli;
}

bool JagParser::parseCommand( const JagParseAttribute &jpa, const Jstr &cmd, JagParseParam *parseParam, Jstr &errmsg ) 
{
	int cmdcheck;
	Jstr em;
	char *pcmd = jagmalloc( cmd.size()+1 );
	memcpy( pcmd, cmd.c_str(), cmd.size() );
	pcmd [cmd.size()] = '\0';

	char *origpcmd = pcmd;

	try {
		cmdcheck = parseSQL( jpa, parseParam, pcmd, cmd.size() );
	} catch ( int a ) {
		em = Jstr("Parser exception ") + intToStr( a ) ;
        cmdcheck = 0;
	} catch ( ... ) {
		em = Jstr("Parser unknown exception");
		cmdcheck = 0;
	}

	if ( cmdcheck <= 0 ) {
		errmsg = Jstr("E12328 Error [") + cmd + "] " + em + " " + intToStr(cmdcheck) 
		          + ": " + jagerr(cmdcheck);
		free( origpcmd );
		return false;
	}

	free( origpcmd );
	return true;
}

int JagParser::parseSQL( const JagParseAttribute &jpa, JagParseParam *parseParam, char *pcmd, int len )
{
	int rc = 0;
	init( jpa, parseParam );
	_cfg = (JagCfg *)jpa.cfg;
	_ptrParam->origCmd = Jstr(pcmd, len);
	_ptrParam->origpos = pcmd;
	_ptrParam->dbName = jpa.dfdbname;

	char *p = pcmd + len - 1;
	while ( isspace(*p) && p >= pcmd ) { *p = '\0'; ++p; } 
	len = strlen(pcmd);
	if ( pcmd[len-1] == ';' ) {
		pcmd[len-1] = '\0';
	}

	_gettok = jag_strtok_r(pcmd, " \t\r\n", &_saveptr);
	if ( !_gettok ) {
		rc = -11010;
	} else if ( strncasecmp(_gettok, "insertsyncdconly|", 17) == 0 ) {
		_ptrParam->insertDCSyncHost = _gettok;
		_gettok = jag_strtok_r(NULL, " \t\r\n", &_saveptr);
	}

	if ( !_gettok ) {
		rc = -11010;
	} else if ( strcasecmp(_gettok, "select") == 0 ) {
		_ptrParam->opcode = JAG_SELECT_OP;
		_ptrParam->optype = 'R';
		rc = getAllClauses( 0 );
		if ( rc > 0 ) rc = setAllClauses( 0 );
	} else if ( strcasecmp(_gettok, "getfile") == 0 ) {
		_ptrParam->opcode = JAG_GETFILE_OP;
		_ptrParam->optype = 'R';
		rc = getAllClauses( 4 );
		if ( rc > 0 ) rc = setAllClauses( 4 );
	} else if ( strcasecmp(_gettok, "insert") == 0 ) {
		_ptrParam->opcode = JAG_INSERT_OP;
		_ptrParam->optype = 'W';
		_gettok = jag_strtok_r(NULL, " \t\r\n", &_saveptr);
		if ( _gettok && strcasecmp(_gettok, "into") == 0 ) {
			_ptrParam->tabidxpos = _saveptr; // command position before table/index name
			rc = setInsertVector();
			if ( 2 == rc ) {
				// insert into ... select ...
				_ptrParam->opcode = JAG_INSERTSELECT_OP;
				_ptrParam->optype = 'W';
				rc = getAllClauses( 3 );
				if ( rc > 0 ) rc = setAllClauses( 3 );
				if ( _ptrParam->objectVec.size() > 2 ) rc = -15;
			} else if ( rc < 0 ) {
				prt(("s0339 setInsertVector rc=%d error\n", rc ));
			}
		} else rc = -20;
	} else if ( strcasecmp(_gettok, "finsert") == 0 ) {
		_ptrParam->opcode = JAG_FINSERT_OP;
		_ptrParam->optype = 'W';
		_gettok = jag_strtok_r(NULL, " \t\r\n", &_saveptr);
		if ( _gettok && strcasecmp(_gettok, "into") == 0 ) {
			_ptrParam->tabidxpos = _saveptr; // command position before table/index name
			rc = setInsertVector();
		} else rc = -25;
	} else if ( strcasecmp(_gettok, "cinsert") == 0 ) {
		_ptrParam->opcode = JAG_CINSERT_OP;
		_ptrParam->optype = 'W';
		_gettok = jag_strtok_r(NULL, " \t\r\n", &_saveptr);
		if ( _gettok && strcasecmp(_gettok, "into") == 0 ) {
			_ptrParam->tabidxpos = _saveptr; // command position before table/index name
			rc = setInsertVector();
		} else rc = -30;
	} else if ( strcasecmp(_gettok, "dinsert") == 0 ) {
		_ptrParam->opcode = JAG_DINSERT_OP;
		_ptrParam->optype = 'W';
		_gettok = jag_strtok_r(NULL, " \t\r\n", &_saveptr);
		if ( _gettok ) {
			_ptrParam->timediff = atoi( _gettok );
			_gettok = jag_strtok_r(NULL, " \t\r\n", &_saveptr);
			if ( _gettok && strcasecmp(_gettok, "into") == 0 ) {
				_ptrParam->tabidxpos = _saveptr; // command position before table/index name
				rc = setInsertVector();
			} else rc = -40;
		} else rc = -50;
	} else if ( strcasecmp(_gettok, "update") == 0 ) {
		_ptrParam->opcode = JAG_UPDATE_OP;
		_ptrParam->optype = 'W';
		_ptrParam->tabidxpos = _saveptr; // command position before table/index name
		_gettok = jag_strtok_r(NULL, " \t\r\n", &_saveptr);
		if ( _gettok ) {
			_ptrParam->endtabidxpos = _saveptr; // command position after table/index name
			rc = setTableIndexList( 0 );
			if ( rc > 0 ) {
				_gettok = jag_strtok_r(NULL, " \t\r\n", &_saveptr);
				if ( _gettok && strcasecmp(_gettok, "set") == 0 ) {
					rc = setUpdateVector();
					if ( rc > 0 ) {
						rc = getAllClauses( 1 );
						if ( rc > 0 ) rc = setAllClauses( 1 );
					}
				} else rc = -2060;
			} 
		} else rc = -70;
	} else if ( strcasecmp(_gettok, "delete") == 0 ) {
		_ptrParam->opcode = JAG_DELETE_OP;
		_ptrParam->optype = 'W';
		_gettok = jag_strtok_r(NULL, " \t\r\n", &_saveptr);
		if ( _gettok && strcasecmp(_gettok, "from") == 0 ) {
			_ptrParam->tabidxpos = _saveptr; // command position before table/index name
			_gettok = jag_strtok_r(NULL, " \t\r\n", &_saveptr);
			if ( _gettok ) {
				_ptrParam->endtabidxpos = _saveptr; // command position after table/index name
				rc = setTableIndexList( 0 );
				if ( rc > 0 ) {
					// check if where clause exists or not
					// if not, regard as truncate table
					if ( !_saveptr ) {
						_ptrParam->opcode = JAG_TRUNCATE_OP;
						_ptrParam->optype = 'C';
						rc = 1;
					} else {
						while ( isspace(*_saveptr) ) ++_saveptr;
						if ( *_saveptr == '\0' ) {
							_ptrParam->opcode = JAG_TRUNCATE_OP; 
							_ptrParam->optype = 'C';
							rc = 1;
						} else {
							--_saveptr;
							*_saveptr = ' ';
							rc = getAllClauses( 1 );
							if ( rc > 0 ) rc = setAllClauses( 1 );
						}
					}
				}
			} else rc = -80;
		} else rc = -90;
	} else if ( strcasecmp(_gettok, "import") == 0 ) {
		_ptrParam->opcode = JAG_IMPORT_OP;
		_ptrParam->optype = 'C';
		_gettok = jag_strtok_r(NULL, " \t\r\n", &_saveptr);
		if ( _gettok && strcasecmp(_gettok, "into") == 0 ) {
			_ptrParam->tabidxpos = _saveptr; // command position before table/index name
			_gettok = jag_strtok_r(NULL, " \t\r\n", &_saveptr);
			if ( _gettok ) {
				_ptrParam->endtabidxpos = _saveptr; // command position after table/index name
				rc = setTableIndexList( 0 );
				if ( rc > 0 ) {
					_gettok = jag_strtok_r(NULL, " \t\r\n", &_saveptr);
					if ( !_gettok ) rc = 1;
					else if ( strcasecmp(_gettok, "complete") == 0 ) {
						_ptrParam->impComplete = 1;
						_gettok = jag_strtok_r(NULL, " \t\r\n", &_saveptr);
						if ( !_gettok ) rc = 1;
						else rc = -100;
					} else rc = -110;
				}
			} else rc = -120;
		} else rc = -130;
	} else if ( strcasecmp(_gettok, "create") == 0 ) {
		_gettok = jag_strtok_r(NULL, " \t\r\n", &_saveptr);
		if ( ! _gettok ) return -12330;
		if ( strcasecmp(_gettok, "table") == 0 ) {
			_ptrParam->opcode = JAG_CREATETABLE_OP;
			_ptrParam->optype = 'C';
			_ptrParam->tabidxpos = _saveptr; // command position before table/index name
			rc = setCreateVector( 0 );
		} else if ( strcasecmp(_gettok, "memtable") == 0 ) {
			_ptrParam->opcode = JAG_CREATEMEMTABLE_OP;
			_ptrParam->optype = 'C';
			_ptrParam->tabidxpos = _saveptr; // command position before table/index name
			rc = setCreateVector( 0 );
		} else if ( strcasecmp(_gettok, "chain") == 0 ) {
			_ptrParam->opcode = JAG_CREATECHAIN_OP;
			_ptrParam->optype = 'C';
			_ptrParam->tabidxpos = _saveptr; // command position before table/index name
			rc = setCreateVector( 0 );
		} else if ( strcasecmp(_gettok, "index") == 0 ) {
			_ptrParam->opcode = JAG_CREATEINDEX_OP;
			_ptrParam->optype = 'C';
			_gettok = jag_strtok_r(NULL, " \t\r\n", &_saveptr);
			if ( ! _gettok ) return -14080;

			if ( 0 == strcasecmp(_gettok, "if") ) {
				// has "if not exists"
				_gettok = jag_strtok_r(NULL, " \t\r\n", &_saveptr);
				if ( _gettok && 0==strcasecmp(_gettok, "not") ) {
					_gettok = jag_strtok_r(NULL, " \t\r\n", &_saveptr);
					if ( _gettok && 0==strncasecmp(_gettok, "exist", 5) ) {
						_gettok = jag_strtok_r(NULL, " \t\r\n", &_saveptr);
						if ( ! _gettok ) return -11401;
						_ptrParam->batchFileName = _gettok;
						_ptrParam->hasExist = true;

						_gettok = jag_strtok_r(NULL, " \t\r\n", &_saveptr);
						if  ( ! _gettok ) return -14411;

						if ( 0 == strcasecmp(_gettok, "ticks" ) ) {
							_ptrParam->value ="ticks";
							_gettok = jag_strtok_r(NULL, " \t\r\n", &_saveptr);
							if ( ! _gettok ) return -14423;
						}

						if ( strcasecmp(_gettok, "on") == 0 ) {
							_ptrParam->tabidxpos = _saveptr; // command position before table/index name
							rc = setCreateVector( 1 );
						} else rc = -15340;
					} else rc = -12400;
				} else rc = -12418;
			} else {
				// regular create index command
				_ptrParam->batchFileName = _gettok; // name of index: myindex123
				_gettok = jag_strtok_r(NULL, " \t\r\n", &_saveptr);
				if  ( ! _gettok ) return -14511;

				if ( 0 == strcasecmp(_gettok, "ticks" ) ) {
					_ptrParam->value ="ticks";
					_gettok = jag_strtok_r(NULL, " \t\r\n", &_saveptr);
					if ( ! _gettok ) return -14423;
				}

				if ( strcasecmp(_gettok, "on") == 0 ) {
					_ptrParam->tabidxpos = _saveptr; // command position before table/index name
					prt(("s50126 setCreateVector index=1\n"));
					rc = setCreateVector( 1 );
				} else {
					rc = -141;
				}
			}
		} else if ( 0==strncasecmp( _gettok, "database", 8 ) ) {
			_gettok = jag_strtok_r(NULL, " \t\r\n", &_saveptr); // dbname
			if ( ! _gettok ) return -7097;
			_ptrParam->opcode = JAG_CREATEDB_OP;
			_ptrParam->optype = 'C';
			_ptrParam->dbName = makeLowerString(_gettok);
			_ptrParam->dbNameCmd = Jstr("createdb ") + _ptrParam->dbName;
			_gettok = jag_strtok_r(NULL, " \t\r\n", &_saveptr);
			if ( ! _gettok ) rc = 1;
			else rc = -7090;
		} else if ( 0==strncasecmp( _gettok, "user", 4 ) ) {
			_gettok = jag_strtok_r(NULL, " \t\r\n", &_saveptr); // username:password
			if ( ! _gettok ) return -7097;
			_ptrParam->opcode = JAG_CREATEUSER_OP;
			_ptrParam->optype = 'C';
			Jstr uidpass(_gettok);
			JagStrSplit sp( uidpass, ':');
			if ( sp.size() == 2 &&_ptrParam->uid.size() <= JAG_USERID_LEN ) {
				_ptrParam->uid = sp[0];
				_ptrParam->passwd = sp[1];
				if (  _ptrParam->passwd.size() <= JAG_PASSWD_LEN ) {
					// _ptrParam->dbName = makeLowerString(_gettok);
					_ptrParam->dbNameCmd = Jstr("createuser ") + uidpass;
					_gettok = jag_strtok_r(NULL, " \t\r\n", &_saveptr);
					if ( ! _gettok ) rc = 1;
					else rc = -7080;
				} else rc = -7082;
			} else rc = -7081;
		} else rc = -200;
	} else if ( strcasecmp(_gettok, "drop") == 0 ) {
		_gettok = jag_strtok_r(NULL, " \t\r\n", &_saveptr);
		if ( _gettok ) {
			if ( strcasecmp(_gettok, "table") == 0 ) {
				_ptrParam->opcode = JAG_DROPTABLE_OP;
				_ptrParam->optype = 'C';
				_ptrParam->tabidxpos = _saveptr; // command position before table/index name
				_gettok = jag_strtok_r(NULL, " \t\r\n", &_saveptr);
				if ( _gettok ) {
					if ( strcasecmp(_gettok, "if") == 0 ) {
						_gettok = jag_strtok_r(NULL, " \t\r\n", &_saveptr);
						if ( _gettok && strncasecmp(_gettok, "exists", 5) == 0 ) {
							_ptrParam->hasExist = 1;
							_ptrParam->tabidxpos = _saveptr; // command position before table/index name
							_gettok = jag_strtok_r(NULL, " \t\r\n", &_saveptr);
							if ( _gettok ) {
								_ptrParam->endtabidxpos = _saveptr; // command position after table/index name
								rc = setTableIndexList( 0 );
								if ( rc > 0 ) {
									_gettok = jag_strtok_r(NULL, " \t\r\n", &_saveptr);
									if ( !_gettok ) rc = 1;
									else rc = -240;
								}
							} else rc = -250;
						} else rc = -260;
					} else {
						if ( strcasecmp(_gettok, "force") == 0 ) {
							_ptrParam->tabidxpos = _saveptr; // command position before table/index name
							_gettok = jag_strtok_r(NULL, " \t\r\n", &_saveptr);
							_ptrParam->hasForce = 1;
						}
						_ptrParam->endtabidxpos = _saveptr; // command position after table/index name
						rc = setTableIndexList( 0 );
						if ( rc > 0 ) {
							_gettok = jag_strtok_r(NULL, " \t\r\n", &_saveptr);
							if ( !_gettok ) rc = 1;
							else rc = -270;
						} 
					}
				} else rc = -280;
			} else if ( strcasecmp(_gettok, "index") == 0 ) {
				_ptrParam->opcode = JAG_DROPINDEX_OP;
				_ptrParam->optype = 'C';
				_gettok = jag_strtok_r(NULL, " \t\r\n", &_saveptr);
				if ( _gettok ) {
					if ( strcasecmp(_gettok, "if") == 0 ) {
						_gettok = jag_strtok_r(NULL, " \t\r\n", &_saveptr);
						if ( _gettok && strncasecmp(_gettok, "exists", 5) == 0 ) {
							_ptrParam->hasExist = 1;
							_gettok = jag_strtok_r(NULL, " \t\r\n", &_saveptr);
							p = _gettok;
						} else rc = -271;
					} else {
						p = _gettok;
					}

					_gettok = jag_strtok_r(NULL, " \t\r\n", &_saveptr);
					if ( _gettok && strcasecmp(_gettok, "on") == 0 ) {
							_ptrParam->tabidxpos = _saveptr; 
							_gettok = jag_strtok_r(NULL, " \t\r\n", &_saveptr);
							if ( _gettok ) {
								_ptrParam->endtabidxpos = _saveptr; 
								rc = setTableIndexList( 0 );
								if ( rc > 0 ) {
									_gettok = p;
									rc = setTableIndexList( 1 );
									if ( rc > 0 ) {
										_gettok = jag_strtok_r(NULL, " \t\r\n", &_saveptr);
										if ( !_gettok ) rc = 1;
										else rc = -290;
									}
								}
							} else rc = -300;
					} else rc = -310;

				} else rc = -320;
			} else if ( strcasecmp(_gettok, "chain") == 0 ) {
				// rc = -322;  to do in real production
			} else rc = -330;
		} else rc = -340;
	} else if ( strcasecmp(_gettok, "truncate") == 0 ) {
		_ptrParam->opcode = JAG_TRUNCATE_OP;
		_ptrParam->optype = 'C';
		_gettok = jag_strtok_r(NULL, " \t\r\n", &_saveptr);
		if ( _gettok && strcasecmp(_gettok, "table") == 0 ) {
			_ptrParam->tabidxpos = _saveptr; // command position before table/index name
			_gettok = jag_strtok_r(NULL, " \t\r\n", &_saveptr);
			if ( _gettok ) {
				_ptrParam->endtabidxpos = _saveptr; // command position after table/index name
				rc = setTableIndexList( 0 );
				if ( rc > 0 ) {
					_gettok = jag_strtok_r(NULL, " \t\r\n", &_saveptr);
					if ( !_gettok ) rc = 1;
					else rc = -350;
				}
			} else rc = -360;
		} else rc = -370;
	} else if ( strcasecmp(_gettok, "alter") == 0 ) {
		Jstr tabname;
		_ptrParam->opcode = JAG_ALTER_OP;
		_ptrParam->optype = 'C';
		_gettok = jag_strtok_r(NULL, " \t\r\n", &_saveptr); 
		if ( _gettok && strcasecmp(_gettok, "table") == 0 ) {
			_ptrParam->tabidxpos = _saveptr; // command position before table/index name
			_gettok = jag_strtok_r(NULL, " \t\r\n", &_saveptr);
			if ( _gettok ) {
				_ptrParam->endtabidxpos = _saveptr; // command position after table/index name
				tabname = _gettok;
				_gettok = jag_strtok_r(NULL, " \t\r\n", &_saveptr);
				if ( _gettok && strcasecmp(_gettok, "rename") == 0 ) {
					_ptrParam->cmd = JAG_SCHEMA_RENAME_COLUMN;
					_gettok = (char*)tabname.c_str();
					rc = setTableIndexList( 0 );
					if ( rc > 0 ) {
						rc = setTableIndexList( 0 );
						if ( rc > 0 ) {					
							_gettok = jag_strtok_r(NULL, " \t\r\n", &_saveptr);
							if ( _gettok ) {								
								_ptrParam->objectVec[0].colName = makeLowerString(_gettok);
								_gettok = jag_strtok_r(NULL, " \t\r\n", &_saveptr);
								if ( _gettok && strcasecmp(_gettok, "to") == 0 ) {
									_gettok = jag_strtok_r(NULL, " \t\r\n", &_saveptr);
									if ( _gettok ) {
										_ptrParam->objectVec[1].colName = makeLowerString(_gettok);
										_gettok = jag_strtok_r(NULL, " \t\r\n", &_saveptr);
										if ( !_gettok ) rc = 1;
										else rc = -380;
									} else rc = -382;
								} else rc = -384;
							} else rc = -386;
						}
					}
				} else if ( _gettok && strcasecmp(_gettok, "add") == 0 ) {
					_gettok = (char*)tabname.c_str();
					rc = setTableIndexList( 0 );
					if ( rc > 0 ) {
						CreateAttribute cattr;
						cattr.objName.dbName = _ptrParam->objectVec[0].dbName;
						cattr.objName.tableName = _ptrParam->objectVec[0].tableName;
						_gettok = jag_strtok_r(NULL, " (:,)\t\r\n", &_saveptr);
						if ( !_gettok ) return -10410;

						if ( strcasecmp(_gettok, "tick") == 0  ) {
							_ptrParam->cmd = JAG_SCHEMA_ADD_TICK;
							_gettok = jag_strtok_r(NULL, " \t\r\n", &_saveptr);
							if ( ! _gettok ) return -10412;
							_ptrParam->value = _gettok;
							_ptrParam->value.remove("()");
							_gettok = jag_strtok_r(NULL, " \t\r\n", &_saveptr);
							if ( !_gettok ) rc = 1; // ended properly
							else rc = -10413;
						} else {
							_ptrParam->cmd = JAG_SCHEMA_ADD_COLUMN;
							rc = setOneCreateColumnAttribute( cattr );
							if ( rc <= 0 ) return rc;
							_ptrParam->createAttrVec.append( cattr );
						}
					}
				} else if ( _gettok && strcasecmp(_gettok, "drop") == 0 ) {
					_gettok = (char*)tabname.c_str();
					rc = setTableIndexList( 0 );
					if ( rc > 0 ) {
						_gettok = jag_strtok_r(NULL, " (:,)\t\r\n", &_saveptr);
						if ( !_gettok ) return -10420;

						if ( strcasecmp(_gettok, "tick") == 0  ) {
							_ptrParam->cmd = JAG_SCHEMA_DROP_TICK;
							_gettok = jag_strtok_r(NULL, " \t\r\n", &_saveptr);
							if ( ! _gettok ) return -10422;
							_ptrParam->value = _gettok;
							_ptrParam->value.remove("()");
							_gettok = jag_strtok_r(NULL, " \t\r\n", &_saveptr);
							if ( !_gettok ) rc = 1; // ended properly
							else rc = -10423;
						} else { rc = -10424; }
					}
				} else if ( _gettok && strcasecmp(_gettok, "retention") == 0 ) {
					_gettok = (char*)tabname.c_str();
					rc = setTableIndexList( 0 );
					if ( rc > 0 ) {
						_gettok = jag_strtok_r(NULL, " (:,)\t\r\n", &_saveptr);
						if ( !_gettok ) return -10420;
						_ptrParam->value = _gettok;
						_ptrParam->value.remove("()");
						_ptrParam->cmd = JAG_SCHEMA_CHANGE_RETENTION;

						_gettok = jag_strtok_r(NULL, " \t\r\n", &_saveptr);
						if ( !_gettok ) rc = 1; 
						else rc = -10433;
					}
				} else if ( _gettok && strcasecmp(_gettok, "set") == 0 ) {
					_ptrParam->cmd = JAG_SCHEMA_SET;
					_gettok = (char*)tabname.c_str();
					rc = setTableIndexList( 0 );
					if ( rc > 0 ) {
						rc = setTableIndexList( 0 );
						if ( rc > 0 ) {					
							_gettok = jag_strtok_r(NULL, " \t\r\n", &_saveptr);
							if ( _gettok && strchr( _gettok, ':' ) ) {								
								_ptrParam->objectVec[0].colName = makeLowerString(_gettok);
								_gettok = jag_strtok_r(NULL, " \t\r\n", &_saveptr);
								if ( _gettok && strcasecmp(_gettok, "to") == 0 ) {
									_gettok = jag_strtok_r(NULL, " \t\r\n", &_saveptr);
									if ( _gettok ) {
										if ( 0==strcasecmp( _gettok, "wgs84" ) ) {
											_ptrParam->value = "4326";
										} else {
											_ptrParam->value = _gettok;
										}
										_gettok = jag_strtok_r(NULL, " \t\r\n", &_saveptr);
										if ( !_gettok ) rc = 1;
										else rc = -480;
									} else rc = -482;
								} else rc = -484;
							} else rc = -486;
						}
					}
				} else rc = -420;
			} else rc = -430;
		} else rc = -440;
	} else if ( strcasecmp(_gettok, "use") == 0 ) {
		_ptrParam->opcode = JAG_USEDB_OP;
		_ptrParam->optype = 'C';
		_gettok = jag_strtok_r(NULL, " \t\r\n", &_saveptr);
		if ( _gettok ) {
			_ptrParam->dbName = makeLowerString(_gettok);
			_ptrParam->dbNameCmd = Jstr("changedb ") + _ptrParam->dbName;
			_gettok = jag_strtok_r(NULL, " \t\r\n", &_saveptr);		
			if ( !_gettok ) rc = 1;
			else rc = -450;
		} else rc = -460;
	} else if ( strcasecmp(_gettok, "changedb") == 0 ) {
		_ptrParam->opcode = JAG_CHANGEDB_OP;
		_ptrParam->optype = 'C';
		_gettok = jag_strtok_r(NULL, " \t\r\n", &_saveptr);
		if ( _gettok ) {
			_ptrParam->dbName = makeLowerString(_gettok);
			_ptrParam->dbNameCmd = Jstr("changedb ") + _ptrParam->dbName;
			_gettok = jag_strtok_r(NULL, " \t\r\n", &_saveptr);
			if ( !_gettok ) rc = 1;
			else rc = -470;
		} else rc = -480;
	} else if ( strcasecmp(_gettok, "createdb") == 0 ) {
		_ptrParam->opcode = JAG_CREATEDB_OP;
		_ptrParam->optype = 'C';
		_gettok = jag_strtok_r(NULL, " \t\r\n", &_saveptr);
		if ( _gettok ) {
			_ptrParam->dbName = makeLowerString(_gettok);
			_ptrParam->dbNameCmd = Jstr("createdb ") + _ptrParam->dbName;
			_gettok = jag_strtok_r(NULL, " \t\r\n", &_saveptr);
			if ( !_gettok ) rc = 1;
			else rc = -490;
		} else rc = -500;
	} else if ( strcasecmp(_gettok, "dropdb") == 0 ) {
		_ptrParam->opcode = JAG_DROPDB_OP;
		_ptrParam->optype = 'C';
		_gettok = jag_strtok_r(NULL, " \t\r\n", &_saveptr);
		if ( _gettok ) {
			_ptrParam->dbNameCmd = "dropdb ";
			if ( strcasecmp(_gettok, "force") == 0 ) {
				_ptrParam->hasForce = true;
				_gettok = jag_strtok_r(NULL, " \t\r\n", &_saveptr);
				if ( ! _gettok ) {
					return -10511;
				}
				_ptrParam->dbNameCmd += "force ";
			}

			_ptrParam->dbName = makeLowerString(_gettok);
			_ptrParam->dbNameCmd += _ptrParam->dbName;
			_gettok = jag_strtok_r(NULL, " \t\r\n", &_saveptr);
			if ( !_gettok ) rc = 1;
			else rc = -510;
		} else rc = -520;
	} else if ( strcasecmp(_gettok, "createuser") == 0 ) {
		_ptrParam->opcode = JAG_CREATEUSER_OP;
		_ptrParam->optype = 'C';
		_gettok = jag_strtok_r(NULL, " \t\r\n", &_saveptr);
		if ( _gettok ) {
			Jstr uidpass( _gettok );
			JagStrSplit sp( uidpass, ':');
			if ( 2 == sp.size() && _ptrParam->uid.size() <= JAG_USERID_LEN ) {
				_ptrParam->uid = sp[0];
				_ptrParam->passwd = sp[1];
				if ( _ptrParam->passwd.size() <= JAG_PASSWD_LEN ) {
					_ptrParam->dbNameCmd = Jstr("createuser ") + uidpass;
    				_gettok = jag_strtok_r(NULL, " :\t\r\n", &_saveptr);
    				if ( !_gettok ) rc = 1;
    				else rc = -530;
				} else rc = -533;
			} else rc = -541;
		} else rc = -550;
	} else if ( strcasecmp(_gettok, "dropuser") == 0 ) {
		_ptrParam->opcode = JAG_DROPUSER_OP;
		_ptrParam->optype = 'C';
		_gettok = jag_strtok_r(NULL, " \t\r\n", &_saveptr);
		if ( _gettok ) {
			_ptrParam->uid = _gettok;
			_ptrParam->dbNameCmd = Jstr("dropuser ") + _ptrParam->uid;
			_gettok = jag_strtok_r(NULL, " \t\r\n", &_saveptr);
			if ( !_gettok ) rc = 1;
			else rc = -560;
		} else rc = -570;
	} else if ( strcasecmp(_gettok, "changepass") == 0 ) {
		_ptrParam->opcode = JAG_CHANGEPASS_OP;
		_ptrParam->optype = 'C';
		_gettok = jag_strtok_r(NULL, " :\t\r\n", &_saveptr);
		if ( _gettok ) {
			_ptrParam->uid = _gettok;
			_gettok = jag_strtok_r(NULL, " :\t\r\n", &_saveptr);
			if ( _gettok ) {
				_ptrParam->passwd = _gettok;
				_ptrParam->dbNameCmd = Jstr("changepass ") + _ptrParam->uid + ":" + _ptrParam->passwd;
				_gettok = jag_strtok_r(NULL, " :\t\r\n", &_saveptr);
				if ( !_gettok ) rc = 1;
				else rc = -580;
			} else rc = -590;
		} else rc = -2600;
	} else if ( strcasecmp(_gettok, "load") == 0 ) {
		_ptrParam->opcode = JAG_LOAD_OP;
		_ptrParam->optype = 'D';
		_gettok = jag_strtok_r(NULL, " \t\r\n", &_saveptr);
		if ( _gettok ) {
			_ptrParam->batchFileName = expandEnvPath(_gettok);
			_gettok = jag_strtok_r(NULL, " \t\r\n", &_saveptr);
			if ( _gettok && strcasecmp(_gettok, "into") == 0 ) {
				_ptrParam->tabidxpos = _saveptr;
				_gettok = jag_strtok_r(NULL, " \t\r\n", &_saveptr);
				if ( _gettok ) {
					_ptrParam->endtabidxpos = _saveptr; 
					rc = setTableIndexList( 0 );
					if ( rc > 0 ) {
						if ( !_saveptr || *_saveptr == '\0' ) {
							rc = 1;
						} else {
							--_saveptr;
							*_saveptr = ' ';
							rc = getAllClauses( 2 );
							if ( rc > 0 ) rc = setAllClauses( 2 );
						}
					}
				} else rc = -610;
			} else rc = -620;
		} else rc = -630;
	} else if ( strncasecmp(_gettok, "desc", 4) == 0 || strncasecmp(_gettok, "_desc", 5) == 0 ) {
		if ( *_gettok == '_' ) _ptrParam->opcode = JAG_EXEC_DESC_OP;
		else _ptrParam->opcode = JAG_DESCRIBE_OP;
		_ptrParam->optype = 'D';
		_ptrParam->tabidxpos = _saveptr; 
		_gettok = jag_strtok_r(NULL, " \t\r\n", &_saveptr);
		if ( _gettok ) {
			_ptrParam->endtabidxpos = _saveptr; 
			rc = setTableIndexList( 0 );
			if ( rc > 0 ) {
				_gettok = jag_strtok_r(NULL, " \t\r\n", &_saveptr);
				if ( !_gettok ) rc = 1;
				else {
					if ( strncasecmp(_gettok, "detail", 6) == 0 ) {
						_ptrParam->detail = true;
						rc = 1;
					} else {
						rc = -642;
					}
				}

			}
		} else rc = -650;
	} else if ( strncasecmp(_gettok, "show", 4) == 0 || strncasecmp(_gettok, "_show", 5) == 0 ) {
		_ptrParam->optype = 'D';
		if ( strcasecmp(_gettok, "showusers") == 0 ) {
			_ptrParam->opcode = JAG_SHOWUSER_OP;
			_gettok = jag_strtok_r(NULL, " \t\r\n", &_saveptr);
			if ( !_gettok ) rc = 1;
			else rc = -660;
		} else {
			p = _gettok;
			_gettok = jag_strtok_r(NULL, " \t\r\n", &_saveptr);
			if ( _gettok ) {
				if ( strncasecmp( _gettok, "tables", 3 ) == 0 ) {
					if ( *p == '_' ) _ptrParam->opcode = JAG_EXEC_SHOWTABLE_OP;
					else _ptrParam->opcode = JAG_SHOWTABLE_OP;
					_gettok = jag_strtok_r(NULL, " \t\r\n", &_saveptr);
					if ( !_gettok ) rc = 1;
					else if ( strcasecmp( _gettok, "like" ) == 0 ) {
						_gettok = jag_strtok_r(NULL, " \t\r\n", &_saveptr);
						if ( _gettok ) {
							_ptrParam->like = _gettok;
							if ( _ptrParam->like.containsChar('\'') ) {
								_ptrParam->like.remove( '\'');
							}
							rc = 1;
						} else { rc = -670; }
					} else rc = -671;
				} else if (  strncasecmp( _gettok, "chains", 5 ) == 0 ) {
					_ptrParam->opcode = JAG_SHOWCHAIN_OP;
					_gettok = jag_strtok_r(NULL, " \t\r\n", &_saveptr);
					if ( !_gettok ) rc = 1;
					else rc = -671;
				} else if ( 0==strncasecmp( _gettok, "indexes", 3 ) ) {
					if ( *p == '_' ) _ptrParam->opcode = JAG_EXEC_SHOWINDEX_OP;
					else _ptrParam->opcode = JAG_SHOWINDEX_OP;
					_gettok = jag_strtok_r(NULL, " \t\r\n", &_saveptr);
					if ( !_gettok ) rc = 1;
					else if ( strcasecmp( _gettok, "from" ) == 0 || strcasecmp( _gettok, "in" ) == 0 ) {
						_ptrParam->tabidxpos = _saveptr; // command position before table/index name
						_gettok = jag_strtok_r(NULL, " \t\r\n", &_saveptr);
						if ( _gettok ) {
							_ptrParam->endtabidxpos = _saveptr; // command position after table/index name
							rc = setTableIndexList( 0 );
							if ( rc > 0 ) {
								_gettok = jag_strtok_r(NULL, " \t\r\n", &_saveptr);
								if ( !_gettok ) rc = 1;
								else rc = -680;
							}
						} else rc = -690;
					} else if ( strcasecmp( _gettok, "like" ) == 0 ) {
						_gettok = jag_strtok_r(NULL, " \t\r\n", &_saveptr);
						if ( _gettok ) {
							_ptrParam->like = _gettok;
							if ( _ptrParam->like.containsChar('\'') ) {
								_ptrParam->like.remove( '\'');
							}
							rc = 1;
						} else { rc = -672; }
					} else rc = -700;
				} else if ( 0==strncasecmp( _gettok, "databases", 8 ) ) {
					if ( *p == '_' ) _ptrParam->opcode = JAG_EXEC_SHOWDB_OP;
					else _ptrParam->opcode = JAG_SHOWDB_OP;
					_gettok = jag_strtok_r(NULL, " \t\r\n", &_saveptr);
					if ( !_gettok ) rc = 1;
					else rc = -710;
				} else if ( 0==strcasecmp( _gettok, "task" ) ) {
					_ptrParam->opcode = JAG_SHOWTASK_OP;
					_gettok = jag_strtok_r(NULL, " \t\r\n", &_saveptr);
					if ( !_gettok ) rc = 1;
					else rc = -720;
				} else if ( 0==strcasecmp( _gettok, "currentdb" ) ) {
					_ptrParam->opcode = JAG_CURRENTDB_OP;
					_gettok = jag_strtok_r(NULL, " \t\r\n", &_saveptr);
					if ( !_gettok ) rc = 1;
					else rc = -730;
				} else if ( 0==strcasecmp( _gettok, "status" ) ) {
					_ptrParam->opcode = JAG_SHOWSTATUS_OP;
					_gettok = jag_strtok_r(NULL, " \t\r\n", &_saveptr);
					if ( !_gettok ) rc = 1;
					else rc = -740;
				} else if ( 0==strcasecmp( _gettok, "datacenter" ) ) {
					_ptrParam->opcode = JAG_SHOWDATACENTER_OP;
					_gettok = jag_strtok_r(NULL, " \t\r\n", &_saveptr);
					if ( !_gettok ) rc = 1;
					else rc = -742;
				} else if ( 0==strcasecmp( _gettok, "tools" ) ) {
					_ptrParam->opcode = JAG_SHOWTOOLS_OP;
					_gettok = jag_strtok_r(NULL, " \t\r\n", &_saveptr);
					if ( !_gettok ) rc = 1;
					else rc = -744;
				} else if ( 0==strcasecmp( _gettok, "user" ) ) {
					_ptrParam->opcode = JAG_CURRENTUSER_OP;
					_gettok = jag_strtok_r(NULL, " \t\r\n", &_saveptr);
					if ( !_gettok ) rc = 1;
					else rc = -750;
				} else if ( 0==strcasecmp( _gettok, "server" ) ) {
					_ptrParam->opcode = JAG_SHOWSVER_OP;
					_gettok = jag_strtok_r(NULL, " \t\r\n", &_saveptr);
					if ( _gettok && 0==strncasecmp( _gettok, "version", 3 ) ) {
						_gettok = jag_strtok_r(NULL, " \t\r\n", &_saveptr);
						if ( !_gettok ) rc = 1;
						else rc = -760;
					} else rc = -770;
				} else if ( 0==strcasecmp( _gettok, "client" ) ) {
					_ptrParam->opcode = JAG_SHOWCVER_OP;
					_gettok = jag_strtok_r(NULL, " \t\r\n", &_saveptr);
					if ( _gettok && 0==strncasecmp( _gettok, "version", 3 ) ) {
						_gettok = jag_strtok_r(NULL, " \t\r\n", &_saveptr);
						if ( !_gettok ) rc = 1;
						else rc = -780;
					} else rc = -790;
				} else if ( 0==strcasecmp( _gettok, "create" ) ) {
					_gettok = jag_strtok_r(NULL, " \t\r\n", &_saveptr);
					if ( _gettok && 0==strncasecmp( _gettok, "table", 5 ) ) {
						_ptrParam->tabidxpos = _saveptr; 
						_gettok = jag_strtok_r(NULL, " \t\r\n", &_saveptr);
						if ( _gettok ) {
							_ptrParam->opcode = JAG_SHOW_CREATE_TABLE_OP;
							_ptrParam->optype = 'D';
							_ptrParam->endtabidxpos = _saveptr; 
							rc = setTableIndexList( 0 );
							if ( rc > 0 ) {
								_gettok = jag_strtok_r(NULL, " \t\r\n", &_saveptr);
								if ( ! _gettok ) rc = 1;
								else rc = - 794;
							}
						} else rc = -795;
					} else if (  _gettok && 0==strncasecmp( _gettok, "chain", 5 ) ) {
						_ptrParam->tabidxpos = _saveptr; 
						_gettok = jag_strtok_r(NULL, " \t\r\n", &_saveptr);
						if ( _gettok ) {
							_ptrParam->opcode = JAG_SHOW_CREATE_CHAIN_OP;
							_ptrParam->optype = 'D';
							_ptrParam->endtabidxpos = _saveptr; 
							rc = setTableIndexList( 0 );
							if ( rc > 0 ) {
								_gettok = jag_strtok_r(NULL, " \t\r\n", &_saveptr);
								if ( ! _gettok ) rc = 1;
								else rc = -796;
							}
						} else rc = -797;
					} else rc = -799;
				} else if ( 0==strncasecmp( _gettok, "grant", 5 ) ) {
					_ptrParam->opcode = JAG_SHOWGRANT_OP;
					_gettok = jag_strtok_r(NULL, " \t\r\n", &_saveptr);
					if ( _gettok && 0==strncasecmp( _gettok, "for", 3 ) ) {
						_gettok = jag_strtok_r(NULL, " \t\r\n", &_saveptr);
						if ( !_gettok ) rc = -801;
						else {
							 _ptrParam->grantUser = _gettok;
							 rc = 1;
						}
					} else {
						_ptrParam->grantUser = "";
						rc = 1;
					}
				
				} else rc = -830;
			} else rc = -833;
		}
	} else if ( strcasecmp(_gettok, "help") == 0 || strcasecmp(_gettok, "?") == 0 ) {
		_ptrParam->opcode = JAG_HELP_OP;
		_ptrParam->optype = 'D';
		rc = 1;
	} else if ( strcasecmp(_gettok, "hello") == 0 ) {
		_ptrParam->optype = 'D';
		_gettok = jag_strtok_r(NULL, " \t\r\n", &_saveptr);
		if ( !_gettok ) rc = 1;
		else rc = -820;
	} else if ( 0 == strcasecmp(_gettok, "grant") || 0 == strcasecmp(_gettok, "revoke")  ) {
		// grant PERM [on] db.tab.col [to] userid;
		// grant select [on] db.tab.col [to] userid [where ...];
		// revoke select [on] db.tab.col [to/from] userid [where ...];
		_ptrParam->optype = 'C';
		if (  0 == strcasecmp(_gettok, "grant") ) {
			_ptrParam->opcode = JAG_GRANT_OP;
		} else {
			_ptrParam->opcode = JAG_REVOKE_OP;
		}

		_gettok = strcasestr(_saveptr, "on" );
		if ( !_gettok ) { rc = -900; }
		else {
			_ptrParam->grantPerm = Jstr( _saveptr, _gettok-_saveptr-1);
			if ( ! isValidGrantPerm( _ptrParam->grantPerm ) ) {
				rc = -902;
			} else {
				_saveptr = _gettok + 2;
				_gettok = jag_strtok_r(NULL, " \t\r\n", &_saveptr);
				if ( ! _gettok ) { rc = -904;
				} else {
					if ( ! _gettok ) { rc = -906;
					} else {
						_ptrParam->grantObj = makeLowerString(_gettok);
						if ( ! isValidGrantObj( _ptrParam->grantObj ) ) {
							rc = -908;
						} else {
							_gettok = jag_strtok_r(NULL, " \t\r\n", &_saveptr);
							if ( ! _gettok ) { rc = -910;
							} else {
								// may be to or from
								if ( 0 == strcasecmp( _gettok, "to" )  ||  0 == strcasecmp( _gettok, "from" ) ) {
									_gettok = jag_strtok_r(NULL, " \t\r\n", &_saveptr);
								}

								if ( ! _gettok ) { rc = -912;
								} else {
									_ptrParam->grantUser = _gettok;
									if ( _ptrParam->grantPerm == JAG_ROLE_SELECT ) {
										_gettok = jag_strtok_r(NULL, " \t\r\n", &_saveptr);
										if ( ! _gettok ) { rc = 1; }
										else {
											if ( 0 == strcasecmp( _gettok, "where" ) ) {
												while ( isblank(*_saveptr) || '\n' == *_saveptr ) ++_saveptr;
												if ( NULL==_saveptr|| '\0' == *_saveptr ) {
													rc = -913;
												} else {
													if ( strchr(_saveptr, '.' ) ) {
														// must have tab.col 
														if ( strchr(_saveptr, '|' ) ) {
															// where cannot have '|'
															rc = -914;
														} else {
														    _ptrParam->grantWhere = _saveptr;
														    rc = 1;
														}
													} else {
														rc = -915;
													}
												}
											} else { rc = -916; }
										}
									} else {
										_gettok = jag_strtok_r(NULL, " \t\r\n", &_saveptr);
										if ( _gettok ) {
											rc = -918;
										} else {
											// no more token, it is OK
											rc = 1;
										}
									}
								}
							}
						}
					}
				}
			}
		}
	} else if ( strncmp(_gettok, "_mon_", 5 ) == 0 ) {
		rc = 1;
	} else {
		rc = 0;
	}

	return rc;
}

int JagParser::init( const JagParseAttribute &jpa, JagParseParam *parseParam )
{
	_ptrParam = parseParam;
	_ptrParam->init( &jpa );
	_gettok = _saveptr = NULL;
}

int JagParser::setTableIndexList( short setType )
{
	const char *p, *q;
	ObjectNameAttribute oname;
	if ( 0 == setType ) {
		_split.init( _gettok, '.' );
		if ( _split.length() == 1 ) {
			oname.dbName = _ptrParam->jpa.dfdbname;
			oname.tableName = _split[0];
			if ( oname.tableName.containsChar('"') ) { oname.tableName.remove('"'); }
			oname.colName = _ptrParam->jpa.dfdbname; // save dfdbname for possible use
			_ptrParam->dbNameCmd = Jstr(_ptrParam->origCmd.c_str(), _ptrParam->tabidxpos-_ptrParam->origpos);
			_ptrParam->dbNameCmd +=	" " + oname.dbName + "." + oname.tableName + " " 
				+ (_ptrParam->origCmd.c_str()+(_ptrParam->endtabidxpos-_ptrParam->origpos));
		} else if ( _split.length() == 2 ) {
			oname.dbName = _split[0];
			oname.tableName = _split[1];
			if ( oname.tableName.containsChar('"') ) { oname.tableName.remove('"'); }
			oname.colName = _ptrParam->jpa.dfdbname; 
			if ( 1 || oname.dbName == oname.colName ) {				
				_ptrParam->dbNameCmd = Jstr(_ptrParam->origCmd.c_str(), _ptrParam->tabidxpos-_ptrParam->origpos) 
					+ " " + oname.dbName + "." + oname.tableName + " " 
					+ (_ptrParam->origCmd.c_str()+(_ptrParam->endtabidxpos-_ptrParam->origpos));
			} else {
				_ptrParam->dbNameCmd = Jstr(_ptrParam->origCmd.c_str(), _ptrParam->tabidxpos-_ptrParam->origpos) 
					+ " " + oname.colName + "." + oname.dbName + "." + oname.tableName + " " 
					+ (_ptrParam->origCmd.c_str()+(_ptrParam->endtabidxpos-_ptrParam->origpos));
			}
		} else {
			if ( JAG_EXEC_DESC_OP == _ptrParam->opcode || JAG_DESCRIBE_OP == _ptrParam->opcode ) {
				oname.dbName = _split[0];
				oname.tableName = _split[1];
				if ( oname.tableName.containsChar('"') ) { oname.tableName.remove('"'); }
				oname.indexName = _split[2];
				_ptrParam->dbNameCmd = Jstr(_ptrParam->origCmd.c_str(), _ptrParam->tabidxpos-_ptrParam->origpos) 
					+ " " + oname.dbName + "." + oname.tableName + "." + oname.indexName + " " 
					+ (_ptrParam->origCmd.c_str()+(_ptrParam->endtabidxpos-_ptrParam->origpos));
			} else return -2010;
		}

		if ( oname.tableName == "TableSchema" || oname.tableName ==  "IndexSchema" ) return -2020;
		_ptrParam->objectVec.append(oname);
	} else if ( 1 == setType ) {
		_split.init( _gettok, '.' );
		if ( _split.length() == 1 && _ptrParam->objectVec.length() == 1 ) {
			oname.dbName = _ptrParam->objectVec[0].dbName;
			oname.tableName = _ptrParam->objectVec[0].tableName;
			oname.indexName = _split[0];
		} else if ( _split.length() == 2 && _ptrParam->objectVec.length() == 1 ) {
			oname.dbName = _ptrParam->objectVec[0].dbName;
			oname.tableName = _ptrParam->objectVec[0].tableName;
			oname.indexName = _split[1];
		} else if ( _split.length() == 3 && _ptrParam->objectVec.length() == 1 ) {
			oname.dbName = _ptrParam->objectVec[0].dbName;
			oname.tableName = _ptrParam->objectVec[0].tableName;
			oname.indexName = _split[2];
		} else {
			return -2040;
		}

		if ( oname.indexName == "TableSchema" || oname.indexName ==  "IndexSchema" ) return -2050;
		_ptrParam->objectVec.append(oname);
	} else {
		bool selectConst;
		if ( _ptrParam->isSelectConst() ) {
			selectConst = true;
		} else {
			selectConst = false;
			if ( _ptrParam->selectTablistClause.length() < 1 ) return -2060;
		}

		char *pmultab, *pjoin, *pon;
		pmultab = (char*)strchr( _ptrParam->selectTablistClause.c_str(), ',' );
		pjoin = (char*)strcasestrskipquote( _ptrParam->selectTablistClause.c_str(), " join " );
		pon = (char*)strcasestrskipquote( _ptrParam->selectTablistClause.c_str(), " on " );
		if ( !pmultab && !pjoin && !pon ) {
			_split.init( _ptrParam->selectTablistClause.c_str(), '.' );
			if ( _split.length() == 1 ) {
				oname.dbName = _ptrParam->jpa.dfdbname;
				oname.tableName = _split[0];
				oname.colName = _ptrParam->jpa.dfdbname; 
				_ptrParam->objectVec.append(oname);
				_ptrParam->dbNameCmd = Jstr(_ptrParam->origCmd.c_str(), _ptrParam->tabidxpos-_ptrParam->origpos) 
					+ " " + oname.dbName + "." + oname.tableName + " " 
					+ (_ptrParam->origCmd.c_str()+(_ptrParam->endtabidxpos-_ptrParam->origpos));
			} else if ( _split.length() == 2 ) {
				oname.dbName = _split[0];
				oname.tableName = _split[1];
				oname.colName = _ptrParam->jpa.dfdbname; 
				_ptrParam->objectVec.append(oname);
				if ( 1 || oname.dbName == oname.colName ) {				
					_ptrParam->dbNameCmd = Jstr(_ptrParam->origCmd.c_str(), _ptrParam->tabidxpos-_ptrParam->origpos) 
						+ " " + oname.dbName + "." + oname.tableName + " " 
						+ (_ptrParam->origCmd.c_str()+(_ptrParam->endtabidxpos-_ptrParam->origpos));
				} else {
					_ptrParam->dbNameCmd = Jstr(_ptrParam->origCmd.c_str(), _ptrParam->tabidxpos-_ptrParam->origpos) 
						+ " " + oname.colName + "." + oname.dbName + "." + oname.tableName + " " 
						+ (_ptrParam->origCmd.c_str()+(_ptrParam->endtabidxpos-_ptrParam->origpos));
				}
			} else if ( _split.length() == 3 ) {
				oname.dbName = _split[0];
				oname.tableName = _split[1];
				oname.indexName = _split[2];
				_ptrParam->objectVec.append(oname);
				_ptrParam->dbNameCmd = Jstr(_ptrParam->origCmd.c_str(), _ptrParam->tabidxpos-_ptrParam->origpos) 
					+ " " + oname.dbName + "." + oname.tableName + "." + oname.indexName + " " 
					+ (_ptrParam->origCmd.c_str()+(_ptrParam->endtabidxpos-_ptrParam->origpos));
			} else {
				if ( ! selectConst ) return -2080;
			}

			if ( JAG_INSERTSELECT_OP != _ptrParam->opcode ) {
				if ( JAG_GETFILE_OP == _ptrParam->opcode ) {
					_ptrParam->opcode = JAG_GETFILE_OP;
				} else {
					_ptrParam->opcode = JAG_SELECT_OP;
				}
			}
		} else if ( pmultab && !pjoin && !pon ) {
			Jstr oneobj, ttlist;
			_splitwq.init( _ptrParam->selectTablistClause.c_str(), ',' );
			for ( int i = 0; i < _splitwq.length(); ++i ) {
				if ( i != 0 ) ttlist += ",";
				oneobj = trimChar( _splitwq[i], ' ' );
				oname.init();
				_split.init( oneobj.c_str(), '.' );
				if ( _split.length() == 1 ) {
					oname.dbName = _ptrParam->jpa.dfdbname;
					oname.tableName = _split[0];
					oname.colName = _ptrParam->jpa.dfdbname; 
					_ptrParam->objectVec.append(oname);
					ttlist += oname.dbName + "." + oname.tableName;
				} else if ( _split.length() == 2 ) {
					oname.dbName = _split[0];
					oname.tableName = _split[1];
					oname.colName = _ptrParam->jpa.dfdbname;
					_ptrParam->objectVec.append(oname);
					if ( 1 || oname.dbName == oname.colName ) {
						ttlist += oname.dbName + "." + oname.tableName;
					} else {
						ttlist += oname.colName + "." + oname.dbName + "." + oname.tableName;
					}
				} else if ( _split.length() == 3 ) {
					oname.dbName = _split[0];
					oname.tableName = _split[1];
					oname.indexName = _split[2];
					_ptrParam->objectVec.append(oname);
					ttlist += oname.dbName + "." + oname.tableName + "." + oname.indexName;
				} else return -2100;
			}
			_ptrParam->opcode = JAG_INNERJOIN_OP;
			_ptrParam->dbNameCmd = Jstr(_ptrParam->origCmd.c_str(), _ptrParam->tabidxpos-_ptrParam->origpos) 
				+ " " + ttlist + " " + (_ptrParam->origCmd.c_str()+(_ptrParam->endtabidxpos-_ptrParam->origpos));
		} else if ( !pmultab && pjoin && pon ) {
			Jstr ttlist;
			q = p = _ptrParam->selectTablistClause.c_str();
			while ( isValidNameChar(*q) || *q == ':' || *q == '.' ) ++q;
			Jstr str = Jstr( p, q-p ), str1, str2;
			_split.init( str.c_str(), '.' );
			oname.init();
			if ( _split.length() == 1 ) {
				oname.dbName = _ptrParam->jpa.dfdbname;
				oname.tableName = _split[0];
				oname.colName = _ptrParam->jpa.dfdbname; 
				_ptrParam->objectVec.append(oname);
				ttlist += oname.dbName + "." + oname.tableName;
			} else if ( _split.length() == 2 ) {
				oname.dbName = _split[0];
				oname.tableName = _split[1];
				oname.colName = _ptrParam->jpa.dfdbname;
				_ptrParam->objectVec.append(oname);
				if ( 1 || oname.dbName == oname.colName ) {
					ttlist += oname.dbName + "." + oname.tableName;
				} else {
					ttlist += oname.colName + "." + oname.dbName + "." + oname.tableName;
				}
			} else if ( _split.length() == 3 ) {
				oname.dbName = _split[0];
				oname.tableName = _split[1];
				oname.indexName = _split[2];
				_ptrParam->objectVec.append(oname);
				ttlist += oname.dbName + "." + oname.tableName + "." + oname.indexName;
			} else {
				return -2120;
			}
			
			p = q;
			while ( isspace(*p) ) ++p;
			str = Jstr( p, pjoin+5-p );
			_split.init( str.c_str(), ' ', true );
			if ( _split.length() == 1 && makeUpperString(_split[0]) == "JOIN" ) {
				_ptrParam->opcode = JAG_INNERJOIN_OP;
				ttlist += " join ";
			} else if ( _split.length() == 2 && makeUpperString(_split[1]) == "JOIN" ) {
				str2 = makeUpperString(_split[0]);
				if ( str2 == "LEFT" ) {
					_ptrParam->opcode = JAG_LEFTJOIN_OP;
					ttlist += " left join ";
					return -2122; // current unsupport left join
				} else if ( str2 == "RIGHT" ) {
					_ptrParam->opcode = JAG_RIGHTJOIN_OP;
					ttlist += " right join ";
					return -2124; // current unsupport right join
				} else if ( str2 == "FULL" ) {
					_ptrParam->opcode = JAG_FULLJOIN_OP;
					ttlist += " full join ";
					return -2126; // current unsupport full join
				} else if ( str2 == "INNER" ) {
					ttlist += " join ";
					_ptrParam->opcode = JAG_INNERJOIN_OP;
				} else return -2130;
			} else if ( _split.length() == 3 && makeUpperString(_split[2]) == "JOIN" ) {
				str1 = makeUpperString(_split[0]);
				str2 = makeUpperString(_split[1]);
				if ( str1 == "LEFT" && str2 == "OUTER" ) {			
					_ptrParam->opcode = JAG_LEFTJOIN_OP;
					ttlist += " left outer join ";
					return -2132; // current unsupport left join
				} else if ( str1 == "RIGHT" && str2 == "OUTER" ) {				
					_ptrParam->opcode = JAG_RIGHTJOIN_OP;
					ttlist += " right outer join ";
					return -2134; // current unsupport right join
				} else if ( str1 == "FULL" && str2 == "OUTER" ) {
					_ptrParam->opcode = JAG_FULLJOIN_OP;
					ttlist += " full outer join ";
					return -2136; // current unsupport full join
				} else return -2140;			
			} else return -2150;
			
			// set second to be joined table/index
			p = pjoin + 5;
			while ( isspace(*p) ) ++p;
			q = p;
			while ( isValidNameChar(*q) || *q == ':' || *q == '.' ) ++q;
			str = Jstr( p, q-p );
			_split.init( str.c_str(), '.' );
			oname.init();
			p = q; 
			if ( _split.length() == 1 ) {
				oname.dbName = _ptrParam->jpa.dfdbname;
				oname.tableName = _split[0];
				oname.colName = _ptrParam->jpa.dfdbname; 
				_ptrParam->objectVec.append(oname);
				ttlist += oname.dbName + "." + oname.tableName;
			} else if ( _split.length() == 2 ) {
				oname.dbName = _split[0];
				oname.tableName = _split[1];
				oname.colName = _ptrParam->jpa.dfdbname;
				_ptrParam->objectVec.append(oname);
				if ( 1 || oname.dbName == oname.colName ) {
					ttlist += oname.dbName + "." + oname.tableName;
				} else {
					ttlist += oname.colName + "." + oname.dbName + "." + oname.tableName;
				}
			} else if ( _split.length() == 3 ) {
				oname.dbName = _split[0];
				oname.tableName = _split[1];
				oname.indexName = _split[2];
				_ptrParam->objectVec.append(oname);
				ttlist += oname.dbName + "." + oname.tableName + "." + oname.indexName;
			} else return -2170;

			while ( isspace(*p) ) ++p;
			_ptrParam->dbNameCmd = Jstr(_ptrParam->origCmd.c_str(), _ptrParam->tabidxpos-_ptrParam->origpos) 
				+ " " + ttlist + " " + p + " " + (_ptrParam->origCmd.c_str()+(_ptrParam->endtabidxpos-_ptrParam->origpos));

			p = q;
			while ( isspace(*p) ) ++p;
			if ( p != pon+1 ) return -2180;
			p += 2;
			while ( isspace(*p) ) ++p;
			OnlyTreeAttribute ota;
			ota.init( _ptrParam->jpa, _ptrParam );
			_ptrParam->initJoinColMap();
			_ptrParam->joinOnVec.append( ota );
			_ptrParam->joinOnVec[_ptrParam->joinOnVec.length()-1].tree->init( _ptrParam->jpa, _ptrParam );
			_ptrParam->setupCheckMap();
			_ptrParam->joinOnVec[_ptrParam->joinOnVec.length()-1].tree->parse( this, p, 1, *_ptrParam->treeCheckMap, _ptrParam->joinColMap,
			_ptrParam->joinOnVec[_ptrParam->joinOnVec.length()-1].colList );
		} else return -2190;
	}
	return 1;
}

int JagParser::getAllClauses( short setType )
{	
	if ( !_saveptr ) return -2200;
	while ( isspace(*_saveptr) ) ++_saveptr;
	if ( 2 != setType && *_saveptr == '\0' ) return -2205;
	--_saveptr;
	if ( _saveptr && *_saveptr == '\0' ) *_saveptr = ' ';
	if ( 0 == setType ) {
		char *pcol, *plist, *pwhere, *pgroup, *phaving, *porder, *plimit, *ptimeout, *pexport;
		pcol = _saveptr;
		while ( isspace(*pcol) ) ++pcol; // new
		plist = (char*)strcasestrskipquote( _saveptr, " from " );
		pwhere = (char*)strcasestrskipquote( _saveptr, " where " );
		pgroup = (char*)strcasestrskipquote( _saveptr, " group " );
		phaving = (char*)strcasestrskipquote( _saveptr, " having " );
		porder = (char*)strcasestrskipquote( _saveptr, " order " );
		plimit = (char*)strcasestrskipquote( _saveptr, " limit " );
		ptimeout = (char*)strcasestrskipquote( _saveptr, " timeout " );
		// ppivot = (char*)strcasestrskipquote( _saveptr, " pivot " );
		pexport = (char*)strcasestrskipquote( _saveptr, " export" );
		
		if ( plist ) {
			*plist = '\0'; 
			plist += 5;
			_ptrParam->tabidxpos = plist; 
			while ( isspace(*plist) ) ++plist;
		} else {
		}

		if ( pwhere ) {
			*pwhere = '\0'; 
			pwhere += 6;
			while ( isspace(*pwhere) ) ++pwhere;
		}
		if ( pgroup ) {
			*pgroup = '\0';
			pgroup += 6;
			while ( isspace(*pgroup) ) ++pgroup;
			if ( strncasecmp( pgroup, "by ", 3 ) != 0 ) return -2220;
			pgroup += 2;
			while ( isspace(*pgroup) ) ++pgroup;
		}		
		if ( phaving ) {
			*phaving = '\0'; 
			phaving += 7;
			while ( isspace(*phaving) ) ++phaving;
		}
		if ( porder ) {
			*porder = '\0';
			porder += 6;
			while ( isspace(*porder) ) ++porder;
			if ( strncasecmp( porder, "by ", 3 ) != 0 ) return -2230;
			porder += 2;
			while ( isspace(*porder) ) ++porder;
		}
		if ( plimit ) {
			*plimit = '\0'; 
			plimit += 6;
			while ( isspace(*plimit) ) ++plimit;
		}
		if ( ptimeout ) {
			*ptimeout = '\0'; 
			ptimeout += 8;
			while ( isspace(*ptimeout) ) ++ptimeout;
		}
		/***
		if ( ppivot ) {
			*ppivot = '\0'; 
			ppivot += 6;
			while ( *ppivot == ' ' ) ++ppivot;
		}
		***/
		if ( pexport ) {
			*pexport = '\0';
			pexport += 1;
		}
		
		// store clauses into parseParam
		if ( pcol ) _ptrParam->selectColumnClause = pcol;
		if ( plist ) _ptrParam->selectTablistClause = plist;
		char *ppp = plist; if ( ppp) { while ( *ppp != '\0' ) ++ppp; }
		_ptrParam->endtabidxpos = ppp; // command position after table/index name
		if ( pwhere ) { _ptrParam->selectWhereClause = pwhere; }
		if ( pgroup ) _ptrParam->selectGroupClause = pgroup;
		if ( phaving ) _ptrParam->selectHavingClause = phaving;
		if ( porder ) _ptrParam->selectOrderClause = porder;
		if ( plimit ) _ptrParam->selectLimitClause = plimit;
		if ( ptimeout ) _ptrParam->selectTimeoutClause = ptimeout;
		// if ( ppivot ) _ptrParam->selectPivotClause = ppivot;
		if ( pexport ) _ptrParam->selectExportClause = pexport;
		
		// trim space at the beginning and end of string
		if ( pcol && strchr(pcol, ' ') ) {
			_ptrParam->selectColumnClause = trimChar( _ptrParam->selectColumnClause, ' ' );
		}

		if ( plist ) {
			_ptrParam->selectTablistClause = trimChar( _ptrParam->selectTablistClause, ' ' );
			if ( _ptrParam->selectTablistClause.containsChar('"') ) {
				_ptrParam->selectTablistClause.remove('"');
			}

			if ( _ptrParam->selectTablistClause.containsChar('\'') ) {
				_ptrParam->selectTablistClause.remove('\'');
			}
		}

		if ( pwhere ) _ptrParam->selectWhereClause = trimChar( _ptrParam->selectWhereClause, ' ' );
		if ( pgroup ) _ptrParam->selectGroupClause = trimChar( _ptrParam->selectGroupClause, ' ' );
		if ( phaving ) _ptrParam->selectHavingClause = trimChar( _ptrParam->selectHavingClause, ' ' );
		if ( porder ) _ptrParam->selectOrderClause = trimChar( _ptrParam->selectOrderClause, ' ' );
		if ( plimit ) _ptrParam->selectLimitClause = trimChar( _ptrParam->selectLimitClause, ' ' );
		if ( ptimeout ) _ptrParam->selectTimeoutClause = trimChar( _ptrParam->selectTimeoutClause, ' ' );
		// if ( ppivot ) _ptrParam->selectPivotClause = trimChar( _ptrParam->selectPivotClause, ' ' );
		if ( pexport ) _ptrParam->selectExportClause = trimChar( _ptrParam->selectExportClause, ' ' );
		
	} else if ( 1 == setType ) {	
		char *pwhere, *pgroup, *phaving, *porder, *plimit, *ptimeout, *pexport;
		pwhere = (char*)strcasestrskipquote( _saveptr, " where " );
		pgroup = (char*)strcasestrskipquote( _saveptr, " group " );
		phaving = (char*)strcasestrskipquote( _saveptr, " having " );
		porder = (char*)strcasestrskipquote( _saveptr, " order " );
		plimit = (char*)strcasestrskipquote( _saveptr, " limit " );
		ptimeout = (char*)strcasestrskipquote( _saveptr, " timeout " );
		// ppivot = (char*)strcasestrskipquote( _saveptr, " pivot " );
		pexport = (char*)strcasestrskipquote( _saveptr, " export" );
		
		if ( pwhere ) {
			if ( pwhere != _saveptr ) return -2240;
			pwhere += 6;
			while ( isspace(*pwhere) ) ++pwhere;
		} else return -2250;

		if ( pgroup ) return -2260;		
		if ( phaving ) return -2270;
		if ( porder ) return -2280;
		if ( plimit ) return -2290;
		if ( ptimeout ) return -2300;
		// if ( ppivot ) return -2310;
		if ( pexport ) return -2320;
		
		if ( pwhere ) _ptrParam->selectWhereClause = pwhere;
		if ( pwhere ) _ptrParam->selectWhereClause = trimChar( _ptrParam->selectWhereClause, ' ' );
		
	} else if ( 2 == setType ) {
		char *pcol, *pline, *pquote;
		pcol = (char*)strcasestrskipquote( _saveptr, " columns " );
		pline = (char*)strcasestrskipquote( _saveptr, " lines " );
		pquote = (char*)strcasestrskipquote( _saveptr, " quote " );
		
		if ( pcol ) {
			*pcol = '\0';
			pcol += 8;
			while ( isspace(*pcol) ) ++pcol;		
			if ( 0 == strncasecmp( pcol, "terminated ", 11 ) ) { 
				pcol += 10;
			}
			while ( isspace(*pcol) ) ++pcol;		
			if ( strncasecmp( pcol, "by ", 3 ) != 0 ) return -2340;
			pcol += 2;
			while ( isspace(*pcol) ) ++pcol;		
		}
		if ( pline ) {
			*pline = '\0';
			pline += 6;
			while ( isspace(*pline) ) ++pline;		
			if ( 0 == strncasecmp( pline, "terminated ", 11 ) ) { 
				pline += 10;
			}
			while ( isspace(*pline) ) ++pline;		
			if ( strncasecmp( pline, "by ", 3 ) != 0 ) return -2360;
			pline += 2;
			while ( isspace(*pline) ) ++pline;		
		}
		if ( pquote ) {
			*pquote = '\0';
			pquote += 6;
			while ( isspace(*pquote) ) ++pquote;		
			if ( 0 == strncasecmp( pquote, "terminated ", 11 ) ) { 
				pquote += 10;
			}
			while ( isspace(*pquote) ) ++pquote;		
			if ( strncasecmp( pquote, "by ", 3 ) != 0 ) return -2380;
			pquote += 2;
			while ( isspace(*pquote) ) ++pquote;		
		}
		
		// store clauses into parseParam
		if ( pcol ) _ptrParam->loadColumnClause = pcol;
		if ( pline ) _ptrParam->loadLineClause = pline;
		if ( pquote ) _ptrParam->loadQuoteClause = pquote;
		
		// trim space at the beginning and end of string
		if ( pcol ) _ptrParam->loadColumnClause = trimChar( _ptrParam->loadColumnClause, ' ' );
		if ( pline ) _ptrParam->loadLineClause = trimChar( _ptrParam->loadLineClause, ' ' );
		if ( pquote ) _ptrParam->loadQuoteClause = trimChar( _ptrParam->loadQuoteClause, ' ' );
		
	} else if ( 3 == setType ) {
		// insert_select cmd clauses
		char *pcol, *plist, *pwhere, *pgroup, *phaving, *porder, *plimit, *ptimeout, *pexport;
		pcol = _saveptr;
		plist = (char*)strcasestrskipquote( _saveptr, " from " );
		pwhere = (char*)strcasestrskipquote( _saveptr, " where " );
		pgroup = (char*)strcasestrskipquote( _saveptr, " group " );
		phaving = (char*)strcasestrskipquote( _saveptr, " having " );
		porder = (char*)strcasestrskipquote( _saveptr, " order " );
		plimit = (char*)strcasestrskipquote( _saveptr, " limit " );
		ptimeout = (char*)strcasestrskipquote( _saveptr, " timeout " );
		// ppivot = (char*)strcasestrskipquote( _saveptr, " pivot " );
		pexport = (char*)strcasestrskipquote( _saveptr, " export" );
		
		// plist clause must exists, otherwise syntax error;
		// only pwhere clause, plimit clause, and select column clause can exist, otherwise syntax error
		if ( plist ) {
			*plist = '\0'; 
			plist += 5;
			_ptrParam->tabidxpos = plist; 
			while ( isspace(*plist) ) ++plist;
		} else return -2310;
		if ( pwhere ) {
			*pwhere = '\0'; 
			pwhere += 6;
			while ( isspace(*pwhere) ) ++pwhere;
		}
		if ( plimit ) {
			*plimit = '\0'; 
			plimit += 6;
			while ( isspace(*plimit) ) ++plimit;
		}
		if ( pgroup ) return -2381;		
		if ( phaving ) return -2382;
		if ( porder ) return -2383;
		if ( ptimeout ) return -2385;
		// if ( ppivot ) return -2386;
		if ( pexport ) return -2387;
		
		// store exist clauses into parseParam
		if ( pcol ) _ptrParam->selectColumnClause = pcol;
		if ( plist ) _ptrParam->selectTablistClause = plist;
		// get end pos of plist
		char *ppp = plist; while ( *ppp != '\0' ) ++ppp;
		_ptrParam->endtabidxpos = ppp; // command position after table/index name
		if ( pwhere ) _ptrParam->selectWhereClause = pwhere;
		if ( plimit ) _ptrParam->selectLimitClause = plimit;
		
		// trim space at the beginning and end of string
		if ( pcol ) _ptrParam->selectColumnClause = trimChar( _ptrParam->selectColumnClause, ' ' );
		if ( plist ) _ptrParam->selectTablistClause = trimChar( _ptrParam->selectTablistClause, ' ' );
		if ( pwhere ) _ptrParam->selectWhereClause = trimChar( _ptrParam->selectWhereClause, ' ' );
		if ( plimit ) _ptrParam->selectLimitClause = trimChar( _ptrParam->selectLimitClause, ' ' );
	} else if ( 4 == setType ) {	
		// select cmd clauses
		char *pcol, *plist, *pwhere, *pgroup, *phaving, *porder, *plimit, *ptimeout, *pexport;
		// get all column start position
		pcol = _saveptr;
		plist = (char*)strcasestrskipquote( _saveptr, " from " );
		pwhere = (char*)strcasestrskipquote( _saveptr, " where " );
		pgroup = (char*)strcasestrskipquote( _saveptr, " group " );
		phaving = (char*)strcasestrskipquote( _saveptr, " having " );
		porder = (char*)strcasestrskipquote( _saveptr, " order " );
		plimit = (char*)strcasestrskipquote( _saveptr, " limit " );
		ptimeout = (char*)strcasestrskipquote( _saveptr, " timeout " );
		// ppivot = (char*)strcasestrskipquote( _saveptr, " pivot " );
		pexport = (char*)strcasestrskipquote( _saveptr, " export" );

		// pcol, plist and pwhere clauses must exists, otherwise syntax error; other clauses must not exist
		if ( !pcol ) return -2388;
		if ( plist ) {
			*plist = '\0'; 
			plist += 5;
			_ptrParam->tabidxpos = plist; // command position before table/index name
			while ( isspace(*plist) ) ++plist;
		} else return -2389;
		if ( pwhere ) {
			*pwhere = '\0'; 
			pwhere += 6;
			while ( isspace(*pwhere) ) ++pwhere;
		} else return -2390;
		if ( pgroup ) return -2391;
		if ( phaving ) return -2392;
		if ( porder ) return -2393;
		if ( plimit ) return -2394;
		if ( ptimeout ) return -2395;
		/***
		if ( ppivot ) else return -2396;
		***/
		if ( pexport ) return -2397;
		
		// store clauses into parseParam
		if ( pcol ) _ptrParam->selectColumnClause = pcol;
		if ( plist ) _ptrParam->selectTablistClause = plist;
		// get end pos of plist
		char *ppp = plist; while ( *ppp != '\0' ) ++ppp;
		_ptrParam->endtabidxpos = ppp; // command position after table/index name
		if ( pwhere ) _ptrParam->selectWhereClause = pwhere;
		// trim space at the beginning and end of string
		if ( pcol ) _ptrParam->selectColumnClause = trimChar( _ptrParam->selectColumnClause, ' ' );
		if ( plist ) _ptrParam->selectTablistClause = trimChar( _ptrParam->selectTablistClause, ' ' );
		if ( pwhere ) _ptrParam->selectWhereClause = trimChar( _ptrParam->selectWhereClause, ' ' );

	} else return -2390;
	return 1;
}

int JagParser::setAllClauses( short setType )
{
	int rc;
	if ( 0 == setType ) {
		// read related, such as select
		if ( ! _ptrParam->isSelectConst() ) {
			if ( _ptrParam->selectTablistClause.length() < 1 ) return -2400;
		}

		rc = setTableIndexList( 2 ); if ( rc < 0 ) return rc;
		if ( _ptrParam->selectColumnClause.length() < 1 ) return -2410;
		rc = setSelectColumn(); if ( rc < 0 ) return rc;
		if ( _ptrParam->selectWhereClause.length() > 0 ) {
			rc =  _ptrParam->setSelectWhere();
			if ( rc < 0 ) return rc;
		}

		if ( _ptrParam->selectGroupClause.length() > 0 ) {
			rc = setSelectGroupBy();
			if ( rc < 0 ) return rc;
		}

		/***
		if ( _ptrParam->selectHavingClause.length() > 0 ) {
			rc = setSelectHaving();
			if ( rc < 0 ) return rc;
		}
		***/

		if ( _ptrParam->selectOrderClause.length() > 0 ) {
			rc = setSelectOrderBy();
			if ( rc < 0 ) return rc;
		}

		if ( _ptrParam->selectLimitClause.length() > 0 ) {
			rc = setSelectLimit();
			if ( rc < 0 ) return rc;
		}
		if ( _ptrParam->selectTimeoutClause.length() > 0 ) {
			rc = setSelectTimeout();
			if ( rc < 0 ) return rc;
		}
		/***
		if ( _ptrParam->selectPivotClause.length() > 0 ) {
			rc = setSelectPivot();
			if ( rc < 0 ) return rc;
		}
		***/
		if ( _ptrParam->selectExportClause.length() > 0 ) {
			rc = setSelectExport();
			if ( rc < 0 ) return rc;
		}	
	} else if ( 1 == setType ) {
		// only where clause
		if ( _ptrParam->selectWhereClause.length() < 1 ) return -2420;
		rc =  _ptrParam->setSelectWhere( );
		// prt(("s5812 setSelectWhere rc=%d\n", rc ));
		if ( rc < 0 ) return rc;		
	} else if ( 2 == setType ) {
		if ( _ptrParam->loadColumnClause.length() > 0 ) {
			rc = setLoadColumn();
			if ( rc < 0 ) return rc;
		}
		if ( _ptrParam->loadLineClause.length() > 0 ) {
			rc = setLoadLine();
			if ( rc < 0 ) return rc;
		}
		if ( _ptrParam->loadQuoteClause.length() > 0 ) {
			rc = setLoadQuote();
			if ( rc < 0 ) return rc;
		}
	} else if ( 3 == setType ) {
		if ( _ptrParam->selectTablistClause.length() < 1 ) return -2422;
		rc = setTableIndexList( 2 );
		if ( rc < 0 ) return rc;
		if ( _ptrParam->selectColumnClause.length() < 1 ) return -2424;
		rc = setSelectColumn();
		if ( rc < 0 ) return rc;
		if ( _ptrParam->selectWhereClause.length() > 0 ) {
			rc =  _ptrParam->setSelectWhere( );
			if ( rc < 0 ) return rc;
		}
		if ( _ptrParam->selectLimitClause.length() > 0 ) {
			rc = setSelectLimit();
			if ( rc < 0 ) return rc;
		}	
	} else if ( 4 == setType ) {
		if ( _ptrParam->selectTablistClause.length() < 1 ) return -2426;
		rc = setTableIndexList( 2 );
		if ( rc < 0 ) return rc;
		if ( _ptrParam->selectColumnClause.length() < 1 ) return -2428;
		rc = setGetfileColumn();
		if ( rc < 0 ) return rc;
		if ( _ptrParam->selectWhereClause.length() < 1 ) return -2429;
		rc =  _ptrParam->setSelectWhere( );
		if ( rc < 0 ) return rc;
	} else return -2430;
	
	return 1;
}

int JagParser::setSelectColumn()
{
	if ( _ptrParam->selectColumnClause.length() < 1 ) return -22440;

	const char *scc = _ptrParam->selectColumnClause.c_str();
	// If "select .... window(5m,ccc) ,  from...."  strip "window(5m,ccc) or window(5m,ccc) ,"
	const char *pwindow = strcasestr( scc, "window(" );
	if ( pwindow ) {
		Jstr s1( scc, pwindow-scc);
		const char *pbra = strchr(pwindow, ')');
		if ( ! pbra ) return -12347;
		++pbra;
		_ptrParam->window = Jstr( pwindow, pbra-pwindow);
		if ( ! _ptrParam->isWindowValid(  _ptrParam->window ) ) {
			return -12350;
		}

		while ( isspace(*pbra) ) ++pbra;
		if ( *pbra == ',' ) ++pbra;
		Jstr rest(pbra);
		_ptrParam->selectColumnClause = s1 + " " + rest;
		scc = _ptrParam->selectColumnClause.c_str();
	}


	if ( _ptrParam->selectColumnClause.length() < 1 ) return -12440;
	// select count(*) or select *, no column need to be set
	if ( strncasecmp(scc, "count(*)", 8) == 0 ||
	     strncasecmp(scc, "count()", 7) == 0
	   ) {
	     _ptrParam->hasCountAll = true;
		if ( JAG_SELECT_OP == _ptrParam->opcode && _ptrParam->selectWhereClause.length() < 1 ) {
			_ptrParam->opcode = JAG_COUNT_OP;
			return 1;
		} else {
			char *changep = (char*)scc+6;
			*changep = '1';
		}
	} else if ( *scc == '*' ) {
		_ptrParam->_selectStar = true;
		return 1;
	}

	char *p, *q, *r;

	_splitwq.init(scc, ',');

	for ( int i = 0; i < _splitwq.length(); ++i ) {
		r = (char*)_splitwq[i].c_str();
		if ( 0==strncasecmp(r, "window(", 7) ) {
			_ptrParam->window = r;
			continue;
		}

		SelColAttribute selColTemp(_ptrParam);
		selColTemp.init( _ptrParam->jpa );
		_ptrParam->selColVec.append(selColTemp);
		_ptrParam->selColVec[i].tree->init( _ptrParam->jpa, _ptrParam );

		if ( (p = (char*)strcasestrskipquote(r, " as ")) ) {
			// remove unnecessary spaces at the end of select tree
			q = p;
			while ( isspace(*p) ) --p; ++p; *p = '\0';
			p = q;
			p += 4;
			while ( isspace(*p) ) ++p;
			if ( *p == '\0' ) return -2450;
		} else {
			if ( (p = (char*)strrchrWithQuote(r, ' ')) ) {
				q = p++;
				while ( isspace(*q) && q - r > 0 ) --q;
				if ( q - r == 0 || BinaryOpNode::isMathOp(*q) || *q == '=' || *q == '<' || *q == '>' ||
					( q - 4 > r && strncasecmp(q-4, " like", 5) == 0 ) ) { 
					p = r;
				} else {
					while ( isspace(*p) ) ++p;
					if ( *p == '\0' ) return -2460;
				}
			} else {
				p = r;
			}
		}
		
		if ( p - r != 0 ) { // has separate as name
			_ptrParam->selColVec[i].name = r;
			q = p;
			--p; while ( isspace(*p) ) --p; ++p; *p = '\0';
			p = q;
			if ( *p == '\'' || *p == '"' ) {
				if ( (q = (char*)jumptoEndQuote(q)) && *q == '\0' ) return -2470;
				++p;
				_ptrParam->selColVec[i].asName = Jstr(p, q-p);
				_ptrParam->selColVec[i].givenAsName = 1;
				++q;
			} else {
				while ( *q != ' ' && *q != '\0' ) ++q;
				_ptrParam->selColVec[i].asName = Jstr(p, q-p);
				_ptrParam->selColVec[i].givenAsName = 1;
			}
			while ( isspace(*q) ) ++q;
			if ( *q != '\0' ) return -2480;	
		} else { // as name as whole
			_ptrParam->selColVec[i].asName = r;
			_ptrParam->selColVec[i].name = r;
		}

		if ( 0==strncasecmp(_ptrParam->selColVec[i].name.c_str(), "all(", 4) ) {
			Jstr nm = _ptrParam->objectVec[0].dbName + "." + _ptrParam->objectVec[0].tableName 
	                    + "." + trimChar(_ptrParam->selColVec[i].name.substr( '(', ')' ), ' ');
			_ptrParam->selAllColVec.append( nm );
		} else {
		}
		
		_ptrParam->selColVec[i].origFuncStr = r;
		_ptrParam->setupCheckMap();
		_ptrParam->initJoinColMap();
		_ptrParam->selColVec[i].tree->parse( this, r, 0, *_ptrParam->treeCheckMap, _ptrParam->joinColMap, 
											 _ptrParam->selColVec[i].colList ); 
	}
	
	_ptrParam->hasColumn = 1;

	return 1;
}

int JagParser::setGetfileColumn()
{
	if ( _ptrParam->selectColumnClause.length() < 1 ) return -2481;

	char *p, *q, *r;
	int getAttributeOrData = 0;
	_splitwq.init(_ptrParam->selectColumnClause.c_str(), ',');

	for ( int i = 0; i < _splitwq.length(); ++i ) {
		SelColAttribute selColTemp(_ptrParam);
		selColTemp.init( _ptrParam->jpa );
		_ptrParam->selColVec.append(selColTemp);
		_ptrParam->selColVec[i].tree->init( _ptrParam->jpa, _ptrParam );
		r = (char*)_splitwq[i].c_str();
		if ( (p=(char*)strcasestrskipquote(r, " sizegb")) || (p=(char*)strcasestrskipquote(r, " sizemb") ) || 
		     (p = (char*)strcasestrskipquote(r, " size")) || (p = (char*)strcasestrskipquote(r, " time")) ||
			 (p = (char*)strcasestrskipquote(r, " md5")) || ( p = (char*)strcasestrskipquote(r, " fpath") ) ||
			 (p = (char*)strcasestrskipquote(r, " type")) ||
			 (p = (char*)strcasestrskipquote(r, " hostfpath")) ||
			 (p = (char*)strcasestrskipquote(r, " host") )
			) {
			if ( 2 == getAttributeOrData ) {
				return -5482;
			} else {
				getAttributeOrData = 1;
			}
		} else if ( (p = (char*)strcasestrskipquote(r, " into ")) ) {
			if ( 1 == getAttributeOrData ) {
				return -5483;
			} else {
				getAttributeOrData = 2;
			}
		} else {
			return -15484;
		}

		// store colname name
		q = p;
		while ( isspace(*r) ) ++r;
		while ( isspace(*q) && q-r > 0 ) --q; ++q;
		_ptrParam->selColVec[i].getfileCol = Jstr(r, q-r);
		if ( strchr(_ptrParam->selColVec[i].getfileCol.c_str(), '.') ) return -2483;
		// store attributes or outpath
		if ( 1 == getAttributeOrData ) {
			if ( strncasecmp( p, " sizegb", 7 ) == 0 ) {
				_ptrParam->selColVec[i].getfileType = JAG_GETFILE_SIZEGB;
				_ptrParam->selColVec[i].asName = "sizegb";
				_ptrParam->selColVec[i].givenAsName = 1;
				q = p+7;
			} else if ( strncasecmp( p, " sizemb", 7 ) == 0 ) {
				_ptrParam->selColVec[i].getfileType = JAG_GETFILE_SIZEMB;
				_ptrParam->selColVec[i].asName = "sizemb";
				_ptrParam->selColVec[i].givenAsName = 1;
				q = p+7;
			} else if ( strncasecmp( p, " size", 5 ) == 0 ) {
				_ptrParam->selColVec[i].getfileType = JAG_GETFILE_SIZE;
				_ptrParam->selColVec[i].asName = "size";
				_ptrParam->selColVec[i].givenAsName = 1;
				q = p+5;
			}  else if ( strncasecmp( p, " time", 5 ) == 0 ) {
				_ptrParam->selColVec[i].getfileType = JAG_GETFILE_TIME;
				_ptrParam->selColVec[i].asName = "time";
				_ptrParam->selColVec[i].givenAsName = 1;
				q = p+5;
			}  else if ( strncasecmp( p, " type", 5 ) == 0 ) {
				_ptrParam->selColVec[i].getfileType = JAG_GETFILE_TYPE;
				_ptrParam->selColVec[i].asName = "type";
				_ptrParam->selColVec[i].givenAsName = 1;
				q = p+5;
			}  else if ( strncasecmp( p, " hostfpath", 10 ) == 0 ) {
				_ptrParam->selColVec[i].getfileType = JAG_GETFILE_HOSTFPATH;
				_ptrParam->selColVec[i].asName = "hostfpath";
				_ptrParam->selColVec[i].givenAsName = 1;
				q = p+10;
			}  else if ( strncasecmp( p, " host", 5 ) == 0 ) {
				_ptrParam->selColVec[i].getfileType = JAG_GETFILE_HOST;
				_ptrParam->selColVec[i].asName = "host";
				_ptrParam->selColVec[i].givenAsName = 1;
				q = p+5;
			} else if ( strncasecmp( p, " md5", 4 ) == 0 ) {
				_ptrParam->selColVec[i].getfileType = JAG_GETFILE_MD5SUM;
				_ptrParam->selColVec[i].asName = "md5";
				_ptrParam->selColVec[i].givenAsName = 1;
				q = p+4;
			} else if ( strncasecmp( p, " fpath", 6 ) == 0 ) {
				_ptrParam->selColVec[i].getfileType = JAG_GETFILE_FPATH;
				_ptrParam->selColVec[i].asName = "fpath";
				_ptrParam->selColVec[i].givenAsName = 1;
				q = p+6;
			} else return -25484;
		} else {
			// store outpath
			_ptrParam->selColVec[i].getfileType = JAG_GETFILE_ACTDATA;
			_ptrParam->getfileActualData = 1;
			p += 5;
			while ( isspace(*p) ) ++p;		
			if ( *p == '\'' || *p == '"' ) {
				if ( (q = (char*)jumptoEndQuote(p)) && *q == '\0' ) return -2484;
				++p;
				_ptrParam->selColVec[i].getfilePath = expandEnvPath( Jstr(p, q-p));
				++q;
			} else {
				q = (char*)(_splitwq[i].c_str()+_splitwq[i].length()); --q;
				while ( isspace(*q) && q-p > 0 ) --q; ++q;
				_ptrParam->selColVec[i].getfilePath = expandEnvPath( Jstr(p, q-p));
			}
		}
		while ( isspace(*q) ) ++q;
		if ( *q != '\0' ) return -2485;	
		
		_ptrParam->selColVec[i].origFuncStr = r;
	}
	
	return 1;
}

int JagParser::setSelectGroupBy()
{
	if ( _ptrParam->selectGroupClause.length() < 1 ) return -2510;	
	const char *p = _ptrParam->selectGroupClause.c_str();
	if ( strncasecmp( p, "lastvalue ", 10 ) == 0 ) {
		_ptrParam->groupType = JAG_GROUPBY_LAST;
		p += 9;
	} else {
		_ptrParam->groupType = JAG_GROUPBY_FIRST;
	}
	
	while ( isspace(*p) ) ++p;
	_splitwq.init( p, ',' );
	GroupOrderVecAttribute gov;
	for ( int i = 0; i < _splitwq.length(); ++i ) {
		gov.name = trimChar( _splitwq[i], ' ' );
		_ptrParam->groupVec.append(gov);
	}
	_ptrParam->hasGroup = 1;
	return 1;
}

/***
// method to set select having tree
// return value <= 0 error; 1 OK
int JagParser::setSelectHaving()
{
	if ( _ptrParam->selectHavingClause.length() < 1 ) return -2520;
	const char *p = _ptrParam->selectHavingClause.c_str();
	OnlyTreeAttribute ota;
	ota.init( _ptrParam->jpa, _ptrParam );
	_ptrParam->havingVec.append( ota );
	_ptrParam->havingVec[_ptrParam->havingVec.length()-1].tree->init( _ptrParam->jpa, _ptrParam );
	// setupCheckMap();
	_ptrParam->setupCheckMap();
	_ptrParam->havingVec[_ptrParam->havingVec.length()-1].tree->parse( this, p, 1, _ptrParam->treecheckmap, _ptrParam->joincolmap,
		_ptrParam->havingVec[_ptrParam->havingVec.length()-1].colList );
	_ptrParam->hasHaving = 1;
	return 1;	
}
***/

int JagParser::setSelectOrderBy()
{
	if ( _ptrParam->selectOrderClause.length() < 1 ) return -2530;
	const char *p = _ptrParam->selectOrderClause.c_str();
	while ( isspace(*p) ) ++p;
	_splitwq.init( p, ',' );
	Jstr upper;
	GroupOrderVecAttribute gov;
	for ( int i = 0; i < _splitwq.length(); ++i ) {
		gov.name = trimChar( _splitwq[i], ' ' );
		_split.init( gov.name.c_str(), ' ', true );
		if (_split.length() < 1 || _split.length() > 2 ) { return -2540; }
		// 1 or 2: "uid" or "uid asc"
		gov.name = _split[0];
		if ( _split.length() == 1 ) {
			gov.isAsc = true;
		} else {
			upper = makeUpperString(_split[1]);
			if ( upper == "ASC" ) {
				gov.isAsc = true;
			} else if ( upper == "DESC" ) {
				gov.isAsc = false;
			} else return -2550;
		}
		_ptrParam->orderVec.append(gov);
	}
	
	_ptrParam->hasOrder = 1;
	return 1;
}

int JagParser::setSelectLimit()
{
	if ( _ptrParam->selectLimitClause.length() < 1 ) return -2560;
	const char *q, *p = _ptrParam->selectLimitClause.c_str();
	while ( isspace(*p) ) ++p;
	q = p;
	while ( isdigit(*q) || *q == ',' || isspace(*q) ) ++q;
	if ( *q != '\0' ) return -2570;
	_split.init( p, ',' );
	if ( 1 == _split.length() ) {
		_ptrParam->limit = jagatoll(_split[0].c_str());
	} else if ( 2 == _split.length() ) {
		_ptrParam->limitStart = jagatoll(_split[0].c_str());
		_ptrParam->limit = jagatoll(_split[1].c_str());
	} else return -2580;
	if ( _ptrParam->limitStart < 0 || _ptrParam->limit < 0 ) return -2590;
	
	_ptrParam->hasLimit = 1;
	return 1;
}

int JagParser::setSelectTimeout()
{
	if ( _ptrParam->selectTimeoutClause.length() < 1 ) return -2600;
	const char *q, *p = _ptrParam->selectTimeoutClause.c_str();
	while ( isspace(*p) ) ++p;
	q = p;
	while ( isdigit(*q) ) ++q;
	if ( *q != '\0' ) return -2610;
	_ptrParam->timeout = jagatoll(p);
	_ptrParam->hasTimeout = 1;
	return 1;
}

// method to set select pivot
// return value <= 0 error; 1 OK
int JagParser::setSelectPivot()
{
	if ( _ptrParam->selectPivotClause.length() < 1 ) return -2620;
	_ptrParam->selectPivotClause.c_str();
	// pivot to add when needed
	_ptrParam->hasPivot = 1;
	return 1;
}

// method to set select export
// return value <= 0 error; 1 OK
int JagParser::setSelectExport()
{
	if ( _ptrParam->selectExportClause.length() < 1 ) return -2630;
	const char *p = _ptrParam->selectExportClause.c_str();
	if ( strcasecmp( p, "export" ) == 0 ) {
		_ptrParam->exportType = JAG_EXPORT;
	} else if ( strcasecmp( p, "exportsql" ) == 0 ) {
		_ptrParam->exportType = JAG_EXPORT_SQL;
	} else if ( strcasecmp( p, "exportcsv" ) == 0 ) {
		_ptrParam->exportType = JAG_EXPORT_CSV;
	} else return -2640;
	_ptrParam->hasExport = 1;
	return 1;
}

// method to set load column by
// return value <= 0 error; 1 OK
int JagParser::setLoadColumn()
{
	if ( _ptrParam->loadColumnClause.length() < 1 ) return -2650;
	const char *p = _ptrParam->loadColumnClause.c_str();
	int hq = 0, sp = 0;
	if ( *p == '\'' ) {
		hq = (int)*p;
		++p;
	} else if ( *p == '"' ) {
		hq = (int)*p;
		++p;
	}
	
	if ( *p == '\\' ) {
		++p;
		sp = getSimpleEscapeSequenceIndex( *p );
		++p;
	} else {
		sp = (int)*p;
		++p;
	}
	if ( hq != 0 && *p != hq ) return -2660;
	if ( 0 == sp ) return -2670;
	_ptrParam->fieldSep = sp;
	return 1;
}

// method to set load line by
// return value <= 0 error; 1 OK
int JagParser::setLoadLine()
{
	if ( _ptrParam->loadLineClause.length() < 1 ) return -2680;
	const char *p = _ptrParam->loadLineClause.c_str();
	int hq = 0, sp = 0;
	if ( *p == '\'' ) {
		hq = (int)*p;
		++p;
	} else if ( *p == '"' ) {
		hq = (int)*p;
		++p;
	}
	
	if ( *p == '\\' ) {
		++p;
		sp = getSimpleEscapeSequenceIndex( *p );
		++p;
	} else {
		sp = (int)*p;
		++p;
	}
	if ( hq != 0 && *p != hq ) return -2690;
	if ( 0 == sp ) return -2700;
	_ptrParam->lineSep = sp;
	return 1;
}

// method to set load quote by
// return value <= 0 error; 1 OK
int JagParser::setLoadQuote()
{
	if ( _ptrParam->loadQuoteClause.length() < 1 ) return -2710;
	const char *p = _ptrParam->loadQuoteClause.c_str();
	int hq = 0, sp = 0;
	if ( *p == '\'' ) {
		hq = (int)*p;
		++p;
	} else if ( *p == '"' ) {
		hq = (int)*p;
		++p;
	}
	
	if ( *p == '\\' ) {
		++p;
		sp = getSimpleEscapeSequenceIndex( *p );
		++p;
	} else {
		sp = (int)*p;
		++p;
	}
	if ( hq != 0 && *p != hq ) return -2720;
	if ( 0 == sp ) return -2730;
	_ptrParam->quoteSep = sp;
	return 1;
}

int JagParser::setInsertVector()
{
	int rc;
	char *p, *q;
	short c1 = 0, c2 = 0, endSignal = 0, paraCount = 0;
	// c1 relates to first parenthesis: insert into t (c1) values (c2), c2 relates to second 
	ObjectNameAttribute oname;
	OtherAttribute other;
	Jstr outStr;
	p = _saveptr;
	while ( isspace(*p) ) ++p;
	q = p;
	// get table name and check its validation
	while ( *q != ' ' && *q != '(' ) ++q;
	if ( *q != ' ' && *q != '(' ) return -2740;
	else if ( *q == '(' ) c1 = 1;
	_ptrParam->endtabidxpos = q; // command position after table/index name
	p[q-p] = '\0'; // startpos should be table name
	_split.init( p, '.' );
	++q;
	if ( _split.length() == 1 ) {
		oname.dbName = _ptrParam->jpa.dfdbname;
		oname.tableName = _split[0];
	} else if ( _split.length() == 2 ) {
		oname.dbName = _split[0];
		oname.tableName = _split[1];
	} else if ( _split.length() == 3 ) {
		oname.dbName = _split[0];
		oname.tableName = _split[1];
		oname.indexName = _split[2];
	} else return -2750;

	if ( 1 == _split.length() || 2 == _split.length() ) {
		//oname.toLower();
		_ptrParam->objectVec.append(oname);
		other.objName.dbName = _ptrParam->objectVec[0].dbName;
		other.objName.tableName = _ptrParam->objectVec[0].tableName;
	} else if ( 3 == _split.length() ) {
		//oname.toLower();
		_ptrParam->objectVec.append(oname);
		other.objName.dbName = _ptrParam->objectVec[0].dbName;
		other.objName.tableName = _ptrParam->objectVec[0].tableName;
		other.objName.indexName = _ptrParam->objectVec[0].indexName;
	}

	// get to next field
	if ( !c1 ) {
		while ( isspace(*q) ) ++q;
		if ( *q == '(' ) { c1 = 1; ++q; }
	}
	
	char *cq;
	if ( c1 ) {
		c1 = 0;
		while ( 1 ) {
			while ( isspace(*q) ) ++q;
			if ( *q == '\0' ) return -2760;
			else if ( *q == ',' ) ++q;
			else if ( *q == ')' ) break;
			while ( isspace(*q) ) ++q;

			p = q;
			if ( *q == '\'' || *q == '"' ) {
				++p;
				if ( (q = (char*)jumptoEndQuote(q)) && *q == '\0' ) return -2770;
				
			} else {
				while ( *q != ' ' && *q != '\0' && *q != ',' && *q != ')' ) ++q;
				if ( *q == '\0' ) return -2780;
				else if ( *q == ')' ) endSignal = 1;	
			}
			*q = '\0';
			++q;
			cq = strrchr(p, '.');
			if ( cq ) {
				p = cq+1;
			}
			
			other.objName.colName = p;
			_ptrParam->otherVec.append(other);
			_ptrParam->initInsColMap();
			c2 = _ptrParam->insColMap->addKeyValue( p, c1 );
			++c1;
			if ( c2 == 0 ) return -12800;
			if ( endSignal ) break;
		}
		++q; // pass the )
	} // end if having first ()
	
	while ( isspace(*q) ) ++q;
	if ( *q == '\0' ) return -12810;
	int check1 = 0, check2 = 0, check3 = 0;
	check1 = strncasecmp(q, "values ", 7);
	check2 = strncasecmp(q, "values(", 7);
	check3 = strncasecmp(q, "select ", 7);
	if ( check1 && check2 && check3 ) { return -2812; } 
	if ( !check3 ) {
		_ptrParam->opcode = JAG_INSERT_OP;
		if ( JAG_INSERT_OP == _ptrParam->opcode ) {
			_saveptr = q+7;
			return 2;
		} else {
			return -2815;
		}
	}
		
	q += 6;
	while ( isspace(*q) ) ++q;
	if ( *q != '(' ) return -2830;
	paraCount = 1;
	++q;  // values (q
	if ( *q == '\0' ) return -2831;
	
	endSignal = 0;
	c2 = 0;
	bool hquote;
	int dim;
	while ( 1 ) {
		while ( isspace(*q) ) ++q;
		if ( *q == '\0' ) break;

		if ( *q == ',' ) ++q;
		while ( isspace(*q) ) ++q;
		if ( *q == ')' ) {
			--paraCount;
			if ( paraCount <= 0 ) {
				break;
			}
		}

		while ( isspace(*q) ) ++q;
		p = q;
		if ( *q == '\'' || *q == '"' ) {
			++p;
			if ( (q = (char*)jumptoEndQuote(q)) && *q == '\0' ) { return -2850; }
			hquote = 1;
		} else {
			while ( *q != '\0' ) {
				if ( *q == '(' ) { 
					++paraCount; 
				} else if ( *q == ')' ) {
					--paraCount; 
					if ( paraCount < 1 ) {
						endSignal = 1;
						break;
					}
				} else if ( *q == ',' ) {
					if ( 1 == paraCount ) {
						// values ( 28, point(2,3),
						break;
					} else {
						// *q = ' '; // replace , with ' '
					}
				}
				++q;
			}

			hquote = 0;
		}

		*q = '\0';
		++q;
		_saveptr = q; 

		// load_file(pathfile) command in values column
		// insert into t1 values ( 'ssss', load_file(/path/file.jpg), 'fff' );
		if ( 0 == hquote ) {
			if ( strncasecmp( p, "load_file(", 10 ) == 0 ) {
				stripEndSpace(p, ')' );
				JagStrSplit sp(p, '(');
				if ( sp.length() > 1 ) {
					Jstr envfpath = trimChar(sp[1], ' ');
					Jstr fpath = expandEnvPath( envfpath );
					FILE *inf = fopen( fpath.c_str(), "r" );
					if ( inf ) {
						base64EncodeFile2String( inf, outStr );
						p = (char*)outStr.c_str();
					} else { 
						// prt(("s0394 p=[%s] sp[1]=[%s] fpath=[%s]\n", p, sp[1].c_str(), fpath.c_str() ));
						return -2861;
					}
				} else {
					return -2862;
				}
			} else if (  strncasecmp( p, "point(", 6 ) == 0 ) {
				while ( *p != '(' ) ++p; ++p;  // (p
				if ( *p == 0 ) return -2859;
				q = p;
				while ( *q != ')' && *q != '\0' ) ++q;  // *q is ')'
				if ( *q == 0 ) return -2854;
				*q = '\0';
				JagStrSplitWithQuote sp( p, ' ', true, true );
				*q = ')';
				if ( sp.length() < 2 ) { return -2865; }
				if ( sp[0].size() >= JAG_POINT_LEN || sp[1].size() >= JAG_POINT_LEN ) { return -2868; }
				other.type =  JAG_C_COL_TYPE_POINT;
				strcpy(other.point.x, sp[0].s() );
				strcpy(other.point.y, sp[1].s() );
				strcpy(other.point.z, "0");
				prt(("s2352028 point sp.print():\n"));
				//sp.print(); 
				if ( ! getMetrics( sp, other.point.metrics ) ) return -2870;
				q = _saveptr;
			} else if (  strncasecmp( p, "point3d(", 8 ) == 0 ) {
				// This is in insert
				while ( *p != '(' ) ++p; ++p;  // (p
				if ( *p == 0 ) return -2855;
				replaceChar(p, ',', ' ', ')' );
				JagStrSplitWithQuote sp( p, ' ', true, true );
				if ( sp.length() < 3 ) { return -2866; }
				if ( sp[0].size() >= JAG_POINT_LEN || sp[1].size() >= JAG_POINT_LEN 
					  || sp[2].size() >= JAG_POINT_LEN ) { return -2869; }
				other.type =  JAG_C_COL_TYPE_POINT3D;
				strcpy(other.point.x, sp[0].c_str() );
				strcpy(other.point.y, sp[1].c_str() );
				strcpy(other.point.z, sp[2].c_str() );
				if ( ! getMetrics( sp, other.point.metrics ) ) return -2871;
				q = _saveptr;
			} else if ( strncasecmp( p, "circle(", 7 ) == 0 || strncasecmp( p, "square(", 7 )==0 ) {
				// circle( x y radius)
				// square( x y a nx )
				if ( strncasecmp( p, "circ", 4 )==0 ) {
					other.type =  JAG_C_COL_TYPE_CIRCLE;
				} else {
					other.type =  JAG_C_COL_TYPE_SQUARE;
				}

				while ( *p != '(' ) ++p; ++p;  // (p
				if ( *p == 0 ) return -2856;
				replaceChar(p, ',', ' ', ')' );
				JagStrSplitWithQuote sp( p, ' ', true, true );
				if (  sp.length() < 3 ) { return -2875; }
				for ( int k=0; k < sp.length(); ++k ) {
					if ( sp[k].length() >= JAG_POINT_LEN ) { return -2780; }
				}

				strcpy(other.point.x, sp[0].c_str() );
				strcpy(other.point.y, sp[1].c_str() );
				strcpy(other.point.z, "0");
				strcpy(other.point.a, sp[2].c_str() );
				strcpy(other.point.nx, "0.0" );
				if ( other.type == JAG_C_COL_TYPE_CIRCLE ) {
					if ( ! getMetrics( sp, other.point.metrics ) ) return -2852;
				} else {
					if ( sp.length() >= 4 ) { 
						strcpy(other.point.nx, sp[3].c_str() ); 
					}
					if ( ! getMetrics( sp, other.point.metrics ) ) return -2872;
				}
				q = _saveptr;
			} else if (  strncasecmp( p, "cube(", 5 )==0 || strncasecmp( p, "sphere(", 7 )==0 ) {
				// cube( x y z radius )   inner circle radius
				if ( strncasecmp( p, "cube", 4 )==0 ) {
					other.type =  JAG_C_COL_TYPE_CUBE;
				} else {
					other.type =  JAG_C_COL_TYPE_SPHERE;
				}

				while ( *p != '(' ) ++p; ++p;  // (p
				if ( *p == 0 ) return -2857;
				replaceChar(p, ',', ' ', ')' );
				JagStrSplitWithQuote sp( p, ' ', true, true );
				if (  sp.length() < 4 ) { return -3175; }
				for ( int k=0; k < sp.length(); ++k ) {
					if ( sp[k].length() >= JAG_POINT_LEN ) { return -3080; }
				}

				strcpy(other.point.x, sp[0].c_str() );
				strcpy(other.point.y, sp[1].c_str() );
				strcpy(other.point.z, sp[2].c_str() );
				strcpy(other.point.a, sp[3].c_str() );
				if ( other.type == JAG_C_COL_TYPE_CUBE ) {
					strcpy(other.point.nx, "0.0" );
					strcpy(other.point.ny, "0.0" );
					if ( sp.length() >= 5 ) { strcpy(other.point.nx, sp[4].c_str() ); }
					if ( sp.length() >= 6 ) { strcpy(other.point.ny, sp[5].c_str() ); }
				}
				if ( ! getMetrics( sp, other.point.metrics ) ) return -2872;
				q = _saveptr;
			} else if ( strncasecmp( p, "circle3d(", 9 )==0
						|| strncasecmp( p, "square3d(", 9 )==0 ) {
				// circle3d( x y z radius nx ny )   inner circle radius
				if ( strncasecmp( p, "circ", 4 )==0 ) {
					other.type =  JAG_C_COL_TYPE_CIRCLE3D;
				} else {
					other.type =  JAG_C_COL_TYPE_SQUARE3D;
				} 

				while ( *p != '(' ) ++p; ++p;  // (p
				if ( *p == 0 ) return -2858;
				replaceChar(p, ',', ' ', ')' );
				JagStrSplitWithQuote sp( p, ' ', true, true );
				if (  sp.length() < 4 ) { return -3155; }
				for ( int k=0; k < sp.length(); ++k ) {
					if ( sp[k].length() >= JAG_POINT_LEN ) { return -3154; }
				}

				strcpy(other.point.x, sp[0].c_str() );
				strcpy(other.point.y, sp[1].c_str() );
				strcpy(other.point.z, sp[2].c_str() );
				strcpy(other.point.a, sp[3].c_str() );

				strcpy(other.point.nx, "0.0" );
				strcpy(other.point.ny, "0.0" );
				if ( sp.length() >=5 ) { strcpy(other.point.nx, sp[4].c_str() ); }
				if ( sp.length() >=6 ) { strcpy(other.point.ny, sp[5].c_str() ); }
				if ( ! getMetrics( sp, other.point.metrics ) ) return -2874;
				q = _saveptr;
			} else if (  strncasecmp( p, "rectangle(", 10 )==0 || strncasecmp( p, "ellipse(", 8 )==0 ) {
				// rectangle( x y width height nx ny) 
				if ( strncasecmp( p, "rect", 4 )==0 ) {
					other.type =  JAG_C_COL_TYPE_RECTANGLE;
				} else {
					other.type =  JAG_C_COL_TYPE_ELLIPSE;
				}
				while ( *p != '(' ) ++p; ++p;  // (p
				if ( *p == 0 ) return -2558;
				//prt(("s2091 p=[%s]\n", p ));
				replaceChar(p, ',', ' ', ')' );
				//prt(("s2092 p=[%s]\n", p ));
				JagStrSplitWithQuote sp( p, ' ', true, true );
				if (  sp.length() < 4 ) { return -3275; }
				//sp.print();
				for ( int k=0; k < sp.length(); ++k ) {
					if ( sp[k].length() >= JAG_POINT_LEN ) { return -3380; }
				}
				//prt(("c3308 origcmd=[%s] \n", _ptrParam->origCmd.c_str() ));

				strcpy(other.point.x, sp[0].c_str() );
				strcpy(other.point.y, sp[1].c_str() );
				strcpy(other.point.a, sp[2].c_str() );
				strcpy(other.point.b, sp[3].c_str() );
				strcpy(other.point.c, sp[3].c_str() );  // dup c from b

				strcpy(other.point.nx, "0.0" );
				if ( sp.length() >= 5 ) {
					strcpy(other.point.nx, sp[4].c_str() );
				}
				if ( ! getMetrics( sp, other.point.metrics ) ) return -2874;
				q = _saveptr;
			} else if ( strncasecmp( p, "rectangle3d(", 12 )==0 || strncasecmp( p, "ellipse3d(", 10 )==0 ) {
				// rectangle( x y z width height nx ny ) 
				if ( strncasecmp( p, "rect", 4 )==0 ) {
					other.type =  JAG_C_COL_TYPE_RECTANGLE3D;
				} else {
					other.type =  JAG_C_COL_TYPE_ELLIPSE3D;
				}
				while ( *p != '(' ) ++p; ++p;  // (p
				if ( *p == 0 ) return -2552;
				replaceChar(p, ',', ' ', ')' );
				JagStrSplitWithQuote sp( p, ' ', true, true );
				if (  sp.length() < 5 ) { return -3331; }
				for ( int k=0; k < sp.length(); ++k ) {
					if ( sp[k].length() >= JAG_POINT_LEN ) { return -3373; }
				}

				strcpy(other.point.x, sp[0].c_str() );
				strcpy(other.point.y, sp[1].c_str() );
				strcpy(other.point.z, sp[2].c_str() );
				strcpy(other.point.a, sp[3].c_str() );
				strcpy(other.point.b, sp[4].c_str() );
				strcpy(other.point.c, sp[4].c_str() );

				strcpy(other.point.nx, "0.0" );
				strcpy(other.point.ny, "0.0" );
				if ( sp.length() >=6 ) { strcpy(other.point.nx, sp[5].c_str() ); }
				if ( sp.length() >=7 ) { strcpy(other.point.ny, sp[6].c_str() ); }
				if ( ! getMetrics( sp, other.point.metrics ) ) return -2878;
				q = _saveptr;
				//prt(("s1051 other.point.x=[%s] other.point.y=[%s]\n", other.point.x, other.point.y ));
				//prt(("s1051 other.point.z=[%s] other.point.z=[%s]\n", other.point.z, other.point.a ));
			} else if (  strncasecmp( p, "box(", 4 )==0 || strncasecmp( p, "ellipsoid(", 10 )==0 ) {
				// box( x y z width depth height nx ny ) 
				if ( strncasecmp( p, "box", 3 )==0 ) {
					other.type =  JAG_C_COL_TYPE_BOX;
				} else {
					other.type =  JAG_C_COL_TYPE_ELLIPSOID;
				}
				while ( *p != '(' ) ++p;
				++p;  // (p
				if ( *p == 0 ) return -2553;
				replaceChar(p, ',', ' ', ')' );
				JagStrSplitWithQuote sp( p, ' ', true, true );
				if (  sp.length() < 6 ) { return -3375; }
				for ( int k=0; k < sp.length(); ++k ) {
					if ( sp[k].length() >= JAG_POINT_LEN ) { return -3380; }
				}

				strcpy(other.point.x, sp[0].c_str() );
				strcpy(other.point.y, sp[1].c_str() );
				strcpy(other.point.z, sp[2].c_str() );
				strcpy(other.point.a, sp[3].c_str() );
				strcpy(other.point.b, sp[4].c_str() );
				strcpy(other.point.c, sp[5].c_str() );

				strcpy(other.point.nx, "0.0" );
				strcpy(other.point.ny, "0.0" );
				if ( sp.length() >=7 ) { strcpy(other.point.nx, sp[6].c_str() ); }
				if ( sp.length() >=8 ) { strcpy(other.point.ny, sp[7].c_str() ); }
				if ( ! getMetrics( sp, other.point.metrics ) ) return -2879;
				q = _saveptr;
			} else if ( strncasecmp( p, "cylinder(", 9 )==0 || strncasecmp( p, "cone(", 5 )==0 ) {
				// cylinder( x y z r height  nx ny 
				if ( strncasecmp( p, "cone", 4 )==0 ) {
					other.type =  JAG_C_COL_TYPE_CONE;
				} else {
					other.type =  JAG_C_COL_TYPE_CYLINDER;
				}
				while ( *p != '(' ) ++p;
				++p;  // (p
				if ( *p == 0 ) return -2551;
				replaceChar(p, ',', ' ', ')' );
				JagStrSplitWithQuote sp( p, ' ', true, true );
				if (  sp.length() < 5 ) { return -3475; }
				for ( int k=0; k < sp.length(); ++k ) {
					if ( sp[k].length() >= JAG_POINT_LEN ) {
						return -3415;
					}
				}

				strcpy(other.point.x, sp[0].c_str() );
				strcpy(other.point.y, sp[1].c_str() );
				strcpy(other.point.z, sp[2].c_str() );
				strcpy(other.point.a, sp[3].c_str() );
				strcpy(other.point.b, sp[4].c_str() );
				strcpy(other.point.c, sp[4].c_str() );

				strcpy(other.point.nx, "0.0" );
				strcpy(other.point.ny, "0.0" );
				if ( sp.length() >=6 ) { strcpy(other.point.nx, sp[5].c_str() ); }
				if ( sp.length() >=7 ) { strcpy(other.point.ny, sp[6].c_str() ); }
				if ( ! getMetrics( sp, other.point.metrics ) ) return -3890;
				q = _saveptr;
			} else if ( strncasecmp( p, "line(", 5 )==0 ) {
				// line( x1 y1 x2 y2)
				while ( *p != '(' ) ++p; ++p;  // (p
				if ( *p == 0 ) return -2651;
				replaceChar(p, ',', ' ', ')' );
				JagStrSplitWithQuote sp( p, ' ', true, true );
				if (  sp.length() < 4 ) { return -3575; }
				for ( int k=0; k < sp.length(); ++k ) {
					if ( sp[k].length() >= JAG_POINT_LEN ) { return -3415; }
				}
				other.type =  JAG_C_COL_TYPE_LINE;
				dim = 2;
				if ( 4 == sp.size() ) {
					JagPoint p1( sp[0].c_str(), sp[1].c_str() );
					JagPoint p2( sp[2].c_str(), sp[3].c_str() );
					other.linestr.point.append(p1);
					other.linestr.point.append(p2);
				} else {
					// one line, one metrics
					JagPoint p1( sp[0].c_str(), sp[1].c_str() );

					int start = dim;
					JagPoint p2( sp[start+0].c_str(), sp[start+1].c_str() );
					if ( ! getMetrics( sp, p1.metrics ) ) return -2891;
					other.linestr.point.append(p1);
					other.linestr.point.append(p2);
				}
				q = _saveptr;
			} else if ( strncasecmp( p, "line3d(", 7 )==0 ) {
				// line3d(x1 y1 z1 x2 y2 z2)
				while ( *p != '(' ) ++p; ++p;  // (p
				if ( *p == 0 ) return -2654;
				replaceChar(p, ',', ' ', ')' );
				JagStrSplitWithQuote sp( p, ' ', true, true );
				if (  sp.length() < 6 ) { return -3577; }
				for ( int k=0; k < sp.length(); ++k ) {
					if ( sp[k].length() >= JAG_POINT_LEN ) { return -3416; }
				}
				other.type =  JAG_C_COL_TYPE_LINE3D;

				if ( 6 == sp.size() ) {
					JagPoint p1( sp[0].c_str(), sp[1].c_str(), sp[2].c_str() );
					JagPoint p2( sp[3].c_str(), sp[4].c_str(), sp[5].c_str() );
					other.linestr.point.append(p1);
					other.linestr.point.append(p2);
				} else {
					// line3d(0 0 1 1 1000 2000 3000)
					dim = 3;
					JagPoint p1( sp[0].c_str(), sp[1].c_str(), sp[2].c_str() );
					//if ( ! getMetrics( sp, dim, p1.metrics ) ) return -6011;

					//int start = dim +  p1.metrics.size();
					int start = dim;
					JagPoint p2( sp[start+0].c_str(), sp[start+1].c_str(), sp[start+2].c_str() );
					if ( ! getMetrics( sp, p1.metrics ) ) return -5810;
					other.linestr.point.append(p1);
					other.linestr.point.append(p2);
				}

				q = _saveptr;
			} else if ( strncasecmp( p, "linestring(", 11 )==0 ) {
				// linestring( x1 y1, x2 y2, x3 y3, x4 y4)
				//prt(("s2834 linestring( p=[%s]\n", p ));
				while ( *p != '(' ) ++p;  ++p;
				if ( *p == 0 ) return -2754;
				//int rc = addLineStringData( other, p );
				//if ( rc < 0 ) return rc;
				rc = checkLineStringData( p );
				if ( rc < 0 ) return rc;
				other.type =  JAG_C_COL_TYPE_LINESTRING;
				q = _saveptr;
				_ptrParam->hasPoly = true;
				if ( 2 > _ptrParam->polyDim ) _ptrParam->polyDim = 2;
			} else if ( strncasecmp( p, "linestring3d(", 13 )==0 ) {
				// linestring( x1 y1 z1, x2 y2 z2, x3 y3 z3, x4 y4 z4)
				//prt(("s2836 linestring3d( p=[%s]\n", p ));
				while ( *p != '(' ) ++p; ++p;
				if ( *p == 0 ) return -2744;
				//int rc = addLineString3DData( other, p );
				//if ( rc < 0 ) return rc;
				rc = checkLineString3DData( p );
				if ( rc < 0 ) return rc;
				other.type =  JAG_C_COL_TYPE_LINESTRING3D;
				q = _saveptr;
				_ptrParam->hasPoly = true;
				if ( 3 > _ptrParam->polyDim ) _ptrParam->polyDim = 3;
			} else if ( strncasecmp( p, "multipoint(", 11 )==0 ) {
				// multipoint( x1 y1, x2 y2, x3 y3, x4 y4)
				//prt(("s2834 multipoint( p=[%s]\n", p ));
				while ( *p != '(' ) ++p;  ++p;
				if ( *p == 0 ) return -2743;
				rc = checkLineStringData( p );
				if ( rc < 0 ) return rc;
				other.type =  JAG_C_COL_TYPE_MULTIPOINT;
				q = _saveptr;
				_ptrParam->hasPoly = true;
				if ( 2 > _ptrParam->polyDim ) _ptrParam->polyDim = 2;
			} else if ( strncasecmp( p, "multipoint3d(", 13 )==0 ) {
				// multipoint3d( x1 y1 z1, x2 y2 z2, x3 y3 z3, x4 y4 z4)
				//prt(("s2836 multipoint3d( p=[%s]\n", p ));
				while ( *p != '(' ) ++p; ++p;
				if ( *p == 0 ) return -2747;
				rc = checkLineString3DData( p );
				if ( rc < 0 ) return rc;
				other.type =  JAG_C_COL_TYPE_MULTIPOINT3D;
				q = _saveptr;
				_ptrParam->hasPoly = true;
				if ( 3 > _ptrParam->polyDim ) _ptrParam->polyDim = 3;
			} else if ( strncasecmp( p, "polygon(", 8 )==0 ) {
				// polygon( ( x1 y1, x2 y2, x3 y3, x4 y4), ( 2 3, 3 4, 9 8, 2 3 ), ( ...) )
				//prt(("s3834 polygon( p=[%s]\n", p ));
				while ( *p != '(' ) ++p; ++p;
				if ( *p == 0 ) return -2447;
				rc = checkPolygonData( p, true );
				if ( rc < 0 ) return rc;
				other.type =  JAG_C_COL_TYPE_POLYGON;
				q = _saveptr;
				_ptrParam->hasPoly = true;
				if ( 2 > _ptrParam->polyDim ) _ptrParam->polyDim = 2;
				//prt(("s3990 polygon p=[%s]\n", p ));
			} else if ( strncasecmp( p, "polygon3d(", 10 )==0 ) {
				// polygon( ( x1 y1 z1, x2 y2 z2, x3 y3 z3, x4 y4 z4), ( 2 3 8, 3 4 0, 9 8 2, 2 3 8 ), ( ...) )
				//prt(("s3835 polygon3d( p=[%s] )\n", p ));
				while ( *p != '(' ) ++p; ++p;
				if ( *p == 0 ) return -2417;
				rc = checkPolygon3DData( p, true );
				if ( rc < 0 ) return rc;
				other.type =  JAG_C_COL_TYPE_POLYGON3D;
				q = _saveptr;
				_ptrParam->hasPoly = true;
				if ( 2 > _ptrParam->polyDim ) _ptrParam->polyDim = 2;
			} else if ( strncasecmp( p, "multipolygon(", 13 )==0 ) {
				// multipolygon( (( x1 y1, x2 y2, x3 y3, x4 y4), ( 2 3, 3 4, 9 8, 2 3 ), ( ...)), ( (..), (..) ) )
				//prt(("s3834 multipolygon( p=[%s]\n", p ));
				while ( *p != '(' ) ++p;  // p: "(p ((...), (...), (...)), (...), ... )
				rc = checkMultiPolygonData( p, true, false );
				if ( rc < 0 ) return rc;
				other.type =  JAG_C_COL_TYPE_MULTIPOLYGON;
				q = _saveptr;
				_ptrParam->hasPoly = true;
				if ( 2 > _ptrParam->polyDim ) _ptrParam->polyDim = 2;
			} else if ( strncasecmp( p, "multipolygon3d(", 10 )==0 ) {
				// polygon( ( x1 y1 z1, x2 y2 z2, x3 y3 z3, x4 y4 z4), ( 2 3 8, 3 4 0, 9 8 2, 2 3 8 ), ( ...) )
				//prt(("s3835 polygon3d( p=[%s] )\n", p ));
				while ( *p != '(' ) ++p;  // "(p ((...), (...), (...)), (...), ... ) 
				rc = checkMultiPolygonData( p, true, true );
				if ( rc < 0 ) return rc;
				other.type =  JAG_C_COL_TYPE_MULTIPOLYGON3D;
				q = _saveptr;
				_ptrParam->hasPoly = true;
				if ( 2 > _ptrParam->polyDim ) _ptrParam->polyDim = 2;
			} else if ( strncasecmp( p, "multilinestring(", 16 )==0 ) {
				// multilinestring( ( x1 y1, x2 y2, x3 y3, x4 y4), ( 2 3, 3 4, 9 8, 2 3 ), ( ...) )
				//prt(("s3834 polygon( p=[%s]\n", p ));
				while ( *p != '(' ) ++p; ++p;
				if ( *p == 0 ) return -2414;
				rc = checkPolygonData( p, false );
				if ( rc < 0 ) return rc;
				other.type =  JAG_C_COL_TYPE_MULTILINESTRING;
				q = _saveptr;
				_ptrParam->hasPoly = true;
				if ( 2 > _ptrParam->polyDim ) _ptrParam->polyDim = 2;
			} else if ( strncasecmp( p, "multilinestring3d(", 10 )==0 ) {
				// multilinestring3d( ( x1 y1 z1, x2 y2 z2, x3 y3 z3, x4 y4 z4), ( 2 3 8, 3 4 0, 9 8 2, 2 3 8 ), ( ...) )
				//prt(("s3835 multilinestring3d( p=[%s] )\n", p ));
				while ( *p != '(' ) ++p; ++p;
				if ( *p == 0 ) return -2411;
				rc = checkPolygon3DData( p, false );
				if ( rc < 0 ) return rc;
				other.type =  JAG_C_COL_TYPE_MULTILINESTRING3D;
				q = _saveptr;
				_ptrParam->hasPoly = true;
				if ( 2 > _ptrParam->polyDim ) _ptrParam->polyDim = 2;
			} else if ( strncasecmp( p, "range(", 6 )==0 ) {
				while ( *p != '(' ) ++p; ++p;
				if ( *p == 0 ) return -2421;
				other.type =  JAG_C_COL_TYPE_RANGE;
				q = _saveptr;
			} else if ( strncasecmp( p, "triangle(", 9 )==0 ) {
				// triangle(x1 y1 x2 y2 x3 y3 )
				while ( *p != '(' ) ++p; ++p;  // (p
				if ( *p == 0 ) return -2412;
				//prt(("s3481 triangle p=[%s]\n", p ));
				replaceChar(p, ',', ' ', ')' );
				JagStrSplitWithQuote sp( p, ' ', true, true );
				if (  sp.length() < 6 ) { return -3587; }
				for ( int k=0; k < sp.length(); ++k ) {
					if ( sp[k].length() >= JAG_POINT_LEN ) { return -3418; }
				}
				other.type =  JAG_C_COL_TYPE_TRIANGLE;
				if ( 6 == sp.length() ) {
					JagPoint p1( sp[0].c_str(), sp[1].c_str() );
					JagPoint p2( sp[2].c_str(), sp[3].c_str() );
					JagPoint p3( sp[4].c_str(), sp[5].c_str() );
					other.linestr.point.append(p1);
					other.linestr.point.append(p2);
					other.linestr.point.append(p3);
				} else {
					// ( x1 y1 x2 y2 x3 y3 m1 m2 m3 m4 )
					dim = 2;
					JagPoint p1( sp[0].c_str(), sp[1].c_str() );
					//if ( ! getMetrics( sp, dim, p1.metrics ) ) return -6001;

					//int start = dim +  p1.metrics.size();
					int start = dim;
					JagPoint p2( sp[start+0].c_str(), sp[start+1].c_str() );
					//if ( ! getMetrics( sp, start+dim, p2.metrics ) ) return -6002;

					//start += dim + p2.metrics.size();
					start += dim;
					JagPoint p3( sp[start+0].c_str(), sp[start+1].c_str() );
					if ( ! getMetrics( sp, p1.metrics ) ) return -6003;

					other.linestr.point.append(p1);
					other.linestr.point.append(p2);
					other.linestr.point.append(p3);
				}

				q = _saveptr;
			} else if ( strncasecmp( p, "triangle3d(", 11 )==0 ) {
				// triangle3d(x1 y1 z1 x2 y2 z2 x3 y3 z3 )
				while ( *p != '(' ) ++p; ++p;  // (p
				if ( *p == 0 ) return -2402;
				replaceChar(p, ',', ' ', ')' );
				// prt(("s0831 triangle3d p=[%s]\n", p ));
				JagStrSplitWithQuote sp( p, ' ', true, true );
				if (  sp.length() != 9 ) { 
					// prt(("s3590 sp.length()=%d p=[%s]\n", sp.length(), p ));
					return -3590; 
				}
				for ( int k=0; k < sp.length(); ++k ) {
					if ( sp[k].length() >= JAG_POINT_LEN ) { return -3592; }
				}
				other.type =  JAG_C_COL_TYPE_TRIANGLE3D;
				if ( 9 == sp.size() ) {
					JagPoint p1( sp[0].c_str(), sp[1].c_str(), sp[2].c_str() );
					JagPoint p2( sp[3].c_str(), sp[4].c_str(), sp[5].c_str() );
					JagPoint p3( sp[6].c_str(), sp[7].c_str(), sp[8].c_str() );
					other.linestr.point.append(p1);
					other.linestr.point.append(p2);
					other.linestr.point.append(p3);
				} else {
					dim = 3;
					JagPoint p1( sp[0].c_str(), sp[1].c_str(), sp[2].c_str() );
					//if ( ! getMetrics( sp, dim, p1.metrics ) ) return -6004;

					int start = dim;
					JagPoint p2( sp[start+0].c_str(), sp[start+1].c_str(), sp[start+2].c_str() );
					//if ( ! getMetrics( sp, start+dim, p2.metrics ) ) return -6005;

					start += dim;
					JagPoint p3( sp[start+0].c_str(), sp[start+1].c_str(), sp[start+2].c_str() );
					if ( ! getMetrics( sp, p1.metrics ) ) return -6006;

					other.linestr.point.append(p1);
					other.linestr.point.append(p2);
					other.linestr.point.append(p3);
				}
				q = _saveptr;
				// prt(("s5124 q=_saveptr=[%s]\n", q ));
			} else if ( strncasecmp( p, "json(", 5 )==0 ) {
				while ( *p != '(' ) ++p;
				//prt(("s2830 p=[%s]\n", p ));
				while ( isspace(*p) || *p == '\'' || *p == '"' || *p == '(' ) ++p;
				// p points to first '{'
				//prt(("s4831 p=[%s]\n", p ));
				if ( *p != '{' ) return -3001;
				char *q;
				for (q = p; *q != ')' && *q != '\0'; ++q );
				if ( *q == '\0' ) return -2377;
				if ( q == p ) return -2378;
				*q = '\0';
				rc = convertJsonToOther( other, p, q-p );
				*q = ')';
				if ( rc < 0 ) return rc;
				_saveptr = q+1;
				q = _saveptr;
				//prt(("s3748 after json() q=[%s]\n", q ));
			} else if ( strncasecmp( p, "now()", 5 )==0 ) {
				q = p + 5;
				_saveptr = q+1;
				q = _saveptr;
				other.valueData = JagTime::makeNowTimeStringMicroSeconds();
			} else if ( strncasecmp( p, "time()", 6 )==0 ) {
				q = p + 6;
				_saveptr = q+1;
				q = _saveptr;
				other.valueData = longToStr( time(NULL) );
			} else {
				other.valueData = "";
				other.type = "";
			}
		}
		
		if ( c1 ) {
			// has first ( c1 ) values ( )
			//prt(("s4000 otherVec[%d] before replacement print:\n", c2 ));
			//_ptrParam->otherVec[c2].print();
			//ObjectNameAttribute save = _ptrParam->otherVec[c2].objName;
			oname = _ptrParam->otherVec[c2].objName;
			_ptrParam->otherVec[c2] = other; 
			_ptrParam->otherVec[c2].objName = oname; 

			//prt(("s4001 otherVec[%d] after replacement print:\n", c2 ));
			//_ptrParam->otherVec[c2].print();

			//prt(("s3033 before removeEndUnevenBracket c2=%d p=valuedata=[%s]\n", c2, p ));
			removeEndUnevenBracket(p);
			stripEndSpace(p, ' ' );
			//prt(("s3033 after removeEndUnevenBracket c2=%d p=valuedata=[%s]\n", c2, p ));
			_ptrParam->otherVec[c2].valueData = p;  // set the value of c2 index
			_ptrParam->otherVec[c2].hasQuote = hquote;

			#if 0
			prt(("s0397 colname=[%s] type=[%s] valueData=p=[%s] c1=%d set->c2=%d\n", 
				 _ptrParam->otherVec[c2].objName.colName.c_str(),  _ptrParam->otherVec[c2].type.c_str(), 
				 p, c1, c2 ));
			#endif

			//prt(("s3936 print:\n" ));
			//_ptrParam->insColMap.print();
			//_ptrParam->otherVec.print();

			other.init();
			c2++;
			if ( c2 >= c1 ) break;
		} else {
			// just insert into t123 values ( ... )
			//prt(("s5345 _saveptr=[%s] p=[%s]\n", _saveptr, p ));
			other.objName.colName = "";
			removeEndUnevenBracket(p);
			stripEndSpace(p, ' ' );
			//prt(("s5346 after removeEndUnevenBracket p=[%s]\n", p ));
			//prt(("s5346 other.valueData=[%s]\n", other.valueData.c_str() ));
			if ( other.valueData.size() < 1 ) {
				other.valueData = p;
			}
			//prt(("s5347 other.valueData=[%s]\n", other.valueData.c_str() ));

			other.hasQuote = hquote;

			//prt(("s5014 ****** otherVec.append(valueData=[%s]) otherVec.size=%d\n", p, _ptrParam->otherVec.size() ));
			_ptrParam->otherVec.append(other);

			//prt(("s0821 _ptrParam=%0x print:\n", _ptrParam ));
			//_ptrParam->otherVec.print();
			//prt(("s5014 ****** _ptrParam->otherVec.size=%d\n", _ptrParam->otherVec.size() ));
			//prt(("s5014 ****** _ptrParam->otherVec.append(other.point.x=[%s])\n", other.point.x ));
			//prt(("s5014 ****** _ptrParam->otherVec.append(other.point.y=[%s])\n", other.point.y ));
			//prt(("s5014 ****** _ptrParam->otherVec.append(other.point.z=[%s])\n", other.point.z ));
			/***
			prt(("s0395 colName=blank type=[%s] valueData=p=[%s] endSignal=%d otherVec.append(other) vec.size=%d\n", 
			  	 other.type.c_str(), p, endSignal, _ptrParam->otherVec.size()  ));

		    prt(("s0395 _ptrParam=%0x other.x=[%s] other.y=[%s] other.z=[%s] other.a=[%s]\n", 
				_ptrParam, other.point.x, other.point.y, other.point.z, other.point.a ));

		    prt(("s0395 _ptrParam=%0x other.a=[%s] other.b=[%s] other.c=[%s]\n", 
				_ptrParam, other.point.a, other.point.b, other.point.c ));
			
			for ( int i=0; i < _ptrParam->otherVec.size(); ++i ) {
				prt(("s4019 i=%d otherVec[i].point.x=[%s] otherVec[i].point.y=[%s] otherVec[i].point.z=[%s]\n",
						i, _ptrParam->otherVec[i].point.x, _ptrParam->otherVec[i].point.y, _ptrParam->otherVec[i].point.z ));
			}
			***/

			other.init( );  
		}  // insert into t123 values ( ... )

		if ( endSignal ) break;
	}

	/**
	prt(("s0822 _ptrParam=%0x print:\n", _ptrParam ));
	//_ptrParam->otherVec.print();
	**/

	return 1;
}

int JagParser::setUpdateVector()
{
	char *p, *q;
	bool reachEqual;
	p = _saveptr;
	while ( isspace(*p) ) ++p;
	if ( *p == '\0' ) return -2870;
	if ( !(q = (char*)strcasestrskipquote(p, " where ")) ) {
		return -2880;
	}
	
	while ( isspace(*q) ) --q;
	++q;
	p[q-p] = '\0';
	_splitwq.init(p, ',');
	p[q-p] = ' ';
	_saveptr = q;
	
	for ( int i = 0; i < _splitwq.length(); ++i ) {		
		UpdSetAttribute updSetTemp;
		updSetTemp.init( _ptrParam->jpa );
		_ptrParam->updSetVec.append(updSetTemp);
		_ptrParam->updSetVec[i].tree->init( _ptrParam->jpa, _ptrParam );
	}
	
	for ( int i = 0; i < _splitwq.length(); ++i ) {
		reachEqual = 0;
		p = (char*)_splitwq[i].c_str();
		while ( isspace(*p) ) ++p;
		q = p;
		if ( *q == '\'' || *q == '"' ) {
			++p;
			if ( (q = (char*)jumptoEndQuote(q)) && *q == '\0' ) return -2890;
		} else {
			while ( !isspace(*q) && *q != '\0' && *q != '=' ) ++q;
			if ( *q == '\0' ) return -2900;
			else if ( *q == '=' ) reachEqual = 1;
		}
		*q = '\0';
		++q;
		if ( strchr(p, '.') ) return -2910;
		// _ptrParam->updSetVec[i].colName = makeLowerString(p);
		_ptrParam->updSetVec[i].colName = p;
		
		while ( isspace(*q) ) ++q;
		if ( *q == '=' && !reachEqual ) ++q;
		else if ( (*q == '=' && reachEqual) || (*q != '=' && !reachEqual) ) return -2920;
		// setupCheckMap();
		_ptrParam->setupCheckMap();
		/**
		BinaryOpNode *rt = _ptrParam->updSetVec[i].tree->parse( this, q, 2, *_ptrParam->treeCheckMap, *_ptrParam->joinColMap,
												_ptrParam->updSetVec[i].colList ); 
		**/
		_ptrParam->initJoinColMap();
		_ptrParam->updSetVec[i].tree->parse( this, q, 2, *_ptrParam->treeCheckMap, _ptrParam->joinColMap,
											_ptrParam->updSetVec[i].colList ); 
		// prt(("s2036 colList=[%s]\n", _ptrParam->updSetVec[i].colList.c_str() ));
	}
	
	return 1;
}

int JagParser::setCreateVector( short setType )
{
	if ( ! endWithSQLRightBra( _saveptr ) ) {
		prt(("s44112 setCreateVector endWithSQLRightBra false error\n"));
		return -10327;
	}

	if ( !_saveptr || *_saveptr == '\0' ) { return -2930; }
	int rc;

	char *q;
	if ( strncasecmp( _saveptr, "timeseries(", 11 ) == 0 ) {
		char *LB = (char*)strchr( _saveptr, '(');
		LB ++;
		char *RB = (char*)strchr( _saveptr, ')');
		if ( NULL == RB ) return -13820;
		if ( RB == LB + 1 ) return -13821;
		Jstr retainTimeser(LB, RB-LB);
		Jstr timeser;
		if ( retainTimeser.containsChar('|') ) {
			JagStrSplit sp( retainTimeser, '|' );
			if ( sp.size() != 2 ) {
				return -12823;
			} else {
				timeser = sp[0];
				_ptrParam->retain = sp[1];
				char lastChar = _ptrParam->retain.lastChar();
				if ( lastChar != '0' && ! JagSchemaRecord::validRetention( lastChar ) ) {
					 return -13822;
				}
			}
		} else {
			timeser = retainTimeser;
			_ptrParam->retain = "0";
		}

		if ( _ptrParam->retain.containsChar(':') || _ptrParam->retain.containsChar(',') ) {
			return  -12824;
		}

		_ptrParam->timeSeries = JagSchemaRecord::translateTimeSeries( timeser );
		timeser =  _ptrParam->timeSeries;

		Jstr normalizedSeries;
		int rc = JagSchemaRecord::normalizeTimeSeries( timeser, normalizedSeries );
		if ( 0 == rc ) {
			// OK 
			_ptrParam->timeSeries = normalizedSeries;
			prt(("s33334772 normalizedSeries=[%s]\n", normalizedSeries.s() ));
		} else {
			prt(("s2222038 normalizeTimeSeries got error rc=%d timeser=[%s]\n", rc, timeser.s() ));
			return -13828;
		}

		_saveptr = RB + 1;
		while ( *_saveptr == ' ' || *_saveptr == '\t' ) ++_saveptr;
		if ( *_saveptr == '\0' ) return -13824;
		_ptrParam->tabidxpos = _saveptr; // command position before table/index name
		prt(("s333390 _saveptr=[%s]\n", _saveptr ));
	}

	q = (char*)strchr( _saveptr, '(' );
	if ( !q ) return -12931;
	_ptrParam->endtabidxpos = q; // command position after table/index name
	*q = '\0';
	Jstr tabstr = _saveptr, defValue;
	*q = '(';
	_saveptr = q;
	prt(("s2038 tabstr=[%s]\n", tabstr.c_str() ));
	// if ( strchr(tabstr.c_str(), ' ') ) tabstr.remove(' ');
	if ( strchr(tabstr.c_str(), '\n') ) tabstr.remove('\n');
	prt(("s2038 tabstr=[%s]\n", tabstr.c_str() ));
	_split.init( tabstr.c_str(), ' ', true );
	prt(("s2238 _split.length=%d\n", _split.length() ));
	prt(("s203939 tabstr=[%s] \n", tabstr.c_str() ));
	if ( _split.length() != 1 && _split.length() != 4 ) {
		prt(("s203939 tabstr=[%s] -2932\n", tabstr.c_str() ));
		return -12932;
	}
	if ( _split.length() == 4 ) {
		// "if not exists db.tab"
		if ( _ptrParam->opcode != JAG_CREATETABLE_OP ) return -2933;
		if ( strcasecmp(_split[0].c_str(), "if") == 0 && strcasecmp(_split[1].c_str(), "not") == 0 &&
			 strncasecmp(_split[2].c_str(), "exists", 5 ) == 0 ) {
			_ptrParam->hasExist = 1;
			// move tabidxpos over to tablename
			_ptrParam->tabidxpos = strcasestr( _ptrParam->tabidxpos, "exists" );
			if ( ! _ptrParam->tabidxpos ) { return -5933; }
			_ptrParam->tabidxpos += 6;
		} else return -12934;
		tabstr = _split[3];
	} else {
		tabstr = _split[0];
	}
	tabstr = trimChar( tabstr, ' ' );
	
	// get table name and check its validation
	q = (char*)tabstr.c_str();
	prt(("s209937 tabstr q=[%s]\n", q ));
	_gettok = (char*)tabstr.c_str();
	prt(("s2039 before setTableIndexList _gettok=[%s]\n", _gettok ));
	rc = setTableIndexList( 0 );
	if ( rc <= 0 ) return rc;

	if ( _ptrParam->opcode == JAG_CREATEINDEX_OP ) {
		_gettok = (char*)_ptrParam->batchFileName.c_str();
		rc = setTableIndexList( 1 );
		_ptrParam->batchFileName = "";
		if ( rc <= 0 ) return rc;
	}

	// 0 is create table
	if ( 0 == setType ) {
		int coloffset = 0, missingKey = 0;
		bool isValue = 0, hasSpare = 0;
		char keymode = 0;
		int  polyDim = 0;

		bool hasPoly = hasPolyGeoType( _saveptr, polyDim );

		// get keymode
		char *p = _saveptr;
		while( isspace(*p) ) ++p;
		if ( *p == '(' ) ++p;
		while ( isspace(*p) ) ++p;
    	keymode = JAG_ASC;  // default is ascending mode
		if ( strncasecmp(p, "key:", 4) != 0 ) {
			missingKey = 1;
			prt(("s50883 missingKey =1 \n"));
		} else {
			p += 3;
    		while ( isspace(*p) ) ++p;
    		if ( *p != ':' ) {
    			while ( *p != ' ' && *p != ':' ) ++p;
    			while ( isspace(*p) ) ++p;
    			if ( *p != ':' ) return -12960;
    		}
    		_saveptr = p+1;
		}
		
		CreateAttribute cattr;
		cattr.objName.dbName = _ptrParam->objectVec[0].dbName;
		cattr.objName.tableName = _ptrParam->objectVec[0].tableName;	

		// if no key is given, use uuid as key, rest are values
		//prt(("s0292 missingKey=%d p=[%s]\n", missingKey, p ));
		if ( missingKey ) {
			cattr.objName.colName = "_id";
			cattr.spare[JAG_SCHEMA_SPARE_LEN] = '\0';
			memset( cattr.spare, JAG_S_COL_SPARE_DEFAULT, JAG_SCHEMA_SPARE_LEN );
			cattr.type = JAG_C_COL_TYPE_STR;
			cattr.length = JAG_UUID_FIELD_LEN;		
			*(cattr.spare) = JAG_C_COL_KEY;
			*(cattr.spare+1) = JAG_C_COL_TYPE_UUID[0];
			*(cattr.spare+2) = JAG_RAND;
			_ptrParam->createAttrVec.append( cattr );
			_ptrParam->keyLength = JAG_UUID_FIELD_LEN;
			prt(("s3921882 missingKey  createAttrVec.append( cattr ) JAG_C_COL_TYPE_STR JAG_C_COL_TYPE_UUID \n" ));
			isValue = 1;
			coloffset += JAG_UUID_FIELD_LEN;
		}

		int firstCol = 1;
		int doneAddBBox = 0;
		int numCol = 0;
		int keyCols = 0;
		int valCols = 0;
		int geoCols = 0;
		int timeStampCols = 0;
		while ( 1 ) {
			_gettok = jag_strtok_r(NULL, " \t\r\n", &_saveptr);
			if ( ! _gettok ) break;
			//prt(("\ns1354 _gettok=[%s] _saveptr=[%s]\n", _gettok, _saveptr ));
			stripEndSpace(_gettok, ',' );
			if ( strcasecmp(_gettok, "(") == 0 ) { continue; }

			//prt(("s1394 _gettok=[%s] _ptrParam->keyLength=%d _saveptr=[%s]\n", _gettok, _ptrParam->keyLength, _saveptr ));
			while ( isspace(*_gettok) ) ++_gettok;
			if ( *_saveptr == ',' ) { ++_saveptr; }
			//prt(("s1395 _gettok=[%s] _ptrParam->keyLength=%d _saveptr=[%s]\n", _gettok, _ptrParam->keyLength, _saveptr ));

			if ( strcasecmp(_gettok, "value:") == 0 ) { 
				if (  missingKey || ! isValue ) { 
					isValue = 1; 
					if ( hasPoly && ! doneAddBBox ) {
						int offset;
						//prt(("s8340 before addBBoxGeomKeyColumns _ptrParam->keyLength=%d\n",  _ptrParam->keyLength ));
						//prt(("s3002 numCol=%d\n", numCol ));
						if ( numCol < 1 ) {
							addBBoxGeomKeyColumns( cattr, polyDim, true, offset ); // update cattr.length
						} else {
							addBBoxGeomKeyColumns( cattr, polyDim, false, offset ); // update cattr.length
						}
						//prt(("s8340 after addBBoxGeomKeyColumns _ptrParam->keyLength=%d\n",  _ptrParam->keyLength ));
						prt(("s0341 done addBBoxGeomKeyColumns coloffset=%d cattr.length=%d polyDim=%d\n", 
							 coloffset, cattr.length, polyDim ));
						coloffset =  offset;
						_ptrParam->hasPoly = 1;
						_ptrParam->polyDim = polyDim;
						//prt(("s0342 coloffset=%d cattr.length=%d\n", coloffset, cattr.length ));
					}
				}
				continue;
			}

			if ( ! strcasecmp( _gettok, "NOT" ) ) {
				_gettok = jag_strtok_r(NULL, " (:,)\t\r\n", &_saveptr);
				continue;
			} else if ( !strcasecmp( _gettok, "NULL" ) || !strcasecmp( _gettok, "UNIQUE" ) ) {
				continue;
			}

			prt(("s0281 before setOneCreateColumnAttribute\n" ));
			rc = setOneCreateColumnAttribute( cattr );
			prt(("s8342 after setOneCreateColumnAttribute _ptrParam->keyLength=%d\n",  _ptrParam->keyLength ));

			if ( rc <= 0 ) { prt(("s2883 rc <= 0 return rc=%d\n", rc )); return rc; }
			prt(("s5040 isValue=%d\n", isValue ));
			if ( !isValue ) {
				*(cattr.spare) = JAG_C_COL_KEY;
				++keyCols;

				if ( *(cattr.spare+7) == JAG_ROLL_UP ) {
					// key column cannot be rolled up
					prt(("s222288 it is JAG_ROLL_UP  key col, error\n"));
					return  -30144;
				} else {
					prt(("s222288 it is not JAG_ROLL_UP  key col\n"));
				}

			} else {
				*(cattr.spare) = JAG_C_COL_VALUE;
				++valCols;
			}

			if ( ! missingKey ) { *(cattr.spare+2) = keymode; }
			prt(("s0361 coloffset=%d cattr.length=%d\n", coloffset, cattr.length ));
			cattr.offset = coloffset;
			coloffset += cattr.length;
			prt(("s0391 cattr.offset=%d coloffset=%d cattr.length=%d\n", cattr.offset, coloffset, cattr.length ));
			
			// check if spare_ column exist
			if ( _cfg && cattr.objName.colName == "spare_" ) {
				cattr.defValues = "";
				if ( cattr.type != JAG_C_COL_TYPE_STR || hasSpare ) return -30145;
				hasSpare = 1;
			}

			if ( firstCol ) {
				firstCol = 0;
				int dim = getPolyDimension( cattr.type );
				//prt(("s2033 getPolyDimension cattr.type=[%s] dim=%d\n", cattr.type.c_str(), dim ));
				if ( dim >= 2 ) {
					CreateAttribute save = cattr;
					int offset;
					//prt(("s2208 before addBBoxGeomKeyColumns  _ptrParam->keyLength=%d\n",  _ptrParam->keyLength ));
					//prt(("s2031 numCol=%d\n", numCol ));
					if ( numCol < 1 ) {
						// leading bbox params
						addBBoxGeomKeyColumns( cattr, polyDim, true, offset ); // update cattr.length
					} else {
						// bbox params after some key-col(s)
						addBBoxGeomKeyColumns( cattr, polyDim, false, offset ); // update cattr.length
					}
					//prt(("s0341 done addBBoxGeomKeyColumns coloffset=%d cattr.length=%d polyDim=%d\n", coloffset, cattr.length, polyDim ));
					//prt(("s2228 after addBBoxGeomKeyColumns  _ptrParam->keyLength=%d\n",  _ptrParam->keyLength ));
					_ptrParam->hasPoly = 1;
					_ptrParam->polyDim = polyDim;
					//prt(("s0342 coloffset=%d cattr.length=%d\n", coloffset, cattr.length ));
					doneAddBBox = 1;
					cattr = save;
					coloffset = offset;
					cattr.offset = coloffset;
					++geoCols;
				}

			}

			//add one column
			prt(("s2328 before addCreateAttrAndColumn  _ptrParam->keyLength=%d\n",  _ptrParam->keyLength ));
			addCreateAttrAndColumn(isValue, cattr, coloffset );
			++ numCol;
			prt(("s1038 addCreateAttrAndColumn isValue=%d numCol=%d\n", isValue, numCol ));
			cattr.defValues = "";
			//prt(("s2328 after addCreateAttrAndColumn  _ptrParam->keyLength=%d\n",  _ptrParam->keyLength ));

		
			Jstr ct = cattr.type;
			if ( isDateAndTime(ct) ) {
				 ++ timeStampCols;
			}

		} // while(1) loop

		if ( hasPoly && valCols < 1 && ! doneAddBBox ) {
			return -15315;
		}

		if ( _ptrParam->timeSeries.size() > 0 && timeStampCols < 1 ) {
			return -13052;
		}

		for ( int i = 0; i < _ptrParam->createAttrVec.length(); ++i ) {
			for ( int j = i+1; j < _ptrParam->createAttrVec.length(); ++j ) {
				if (  _ptrParam->createAttrVec[i].objName.colName == _ptrParam->createAttrVec[j].objName.colName ) {
					return -13050;
				}
			}
		}

		// if spare column does not exist, add one to the end
		// prt(("c3387 _cfg=%0x hasSpare=%d\n", _cfg, hasSpare ));
		if ( _cfg && ! hasSpare ) {
			cattr.spare[JAG_SCHEMA_SPARE_LEN] = '\0';
			memset( cattr.spare, JAG_S_COL_SPARE_DEFAULT, JAG_SCHEMA_SPARE_LEN );
			*(cattr.spare) = JAG_C_COL_VALUE;
			cattr.objName.colName = "spare_";
			cattr.type = JAG_C_COL_TYPE_STR;
			cattr.offset = _ptrParam->keyLength+_ptrParam->valueLength;
			cattr.length = _cfg->getLongValue("SPARE_COLUMN_PERCENT", 30)*cattr.offset/100; // percent, can be 30, 50, 200, 500, etc.
			cattr.sig = 0;
			_ptrParam->valueLength += cattr.length;
			_ptrParam->createAttrVec.append( cattr );
		}

	} else if ( 1 == setType ) {
		prt(("s38143 createindex ...\n" ));
		int numCols = 0;
		_ptrParam->fieldSep = JAG_RAND;
		OtherAttribute other;
		while ( 1 ) {
			_gettok = jag_strtok_r(NULL, " (,)\t\r\n", &_saveptr);
			prt(("s2031 _gettok=[%s] _saveptr=[%s] numCols=%d\n", _gettok, _saveptr, numCols ));
			if ( !_gettok ) {
				if ( !numCols ) return -13060;
				break;
			} 
			
			if ( strcasecmp(_gettok, "key:") == 0 ) {
				//_gettok = jag_strtok_r(NULL, " :\t\r\n", &_saveptr);
				_gettok = jag_strtok_r(NULL, " ,\t\r\n", &_saveptr);
				prt(("s2051 _gettok=[%s] _saveptr=[%s] numCols=%d\n", _gettok, _saveptr, numCols ));
				if ( !_gettok ) return -13070;

				if ( strcasecmp(_gettok, "asc") == 0 || strcasecmp(_gettok, "ascend") == 0 ) {
					_ptrParam->fieldSep = JAG_ASC;
				} else {
					if ( strchr( _gettok, '.' ) ) return -13080;
					other.objName.colName = makeLowerString(_gettok);
					prt(("s1098 key other.objName.colName=[%s] otherVec.append(other)\n", other.objName.colName.c_str() ));
					_ptrParam->otherVec.append(other);
					++numCols;
				}
				continue;
			} else if ( strcasecmp(_gettok, "value:") == 0 ) {
				if ( !numCols ) return -13090;
				_ptrParam->limit = numCols;
				continue;
			}

			if ( !_gettok && !numCols ) return -13100;
			else if ( !_gettok ) break;
			else if ( strchr( _gettok, '.' ) ) return -13110;

			other.objName.colName = makeLowerString(_gettok);
			_ptrParam->otherVec.append(other);
			++numCols;
			prt(("s12039 other.objName.colName=[%s] numCols=%d append(other)\n", other.objName.colName.c_str(), numCols ));
			const JagColumn *pcol = getColumn( _ptrParam, other.objName.colName );
			if ( pcol ) {
				prt(("s3043 addExtraOtherCols ...\n" ));
				addExtraOtherCols( pcol, other, numCols );
			} else {
				/***
				prt(("s2031 pcol NULL for db=[%s] tab=[%s] col=[%s]\n",
						_ptrParam->objectVec[0].dbName.c_str(), _ptrParam->objectVec[0].tableName.c_str(), 
						other.objName.colName.c_str() ));
				***/
			}

		}  // end while(1) loop
		if ( ! _ptrParam->limit ) _ptrParam->limit = numCols;
	} else {
		return -13120;
	}
	
	return 1;
}


int JagParser::setOneCreateColumnAttribute( CreateAttribute &cattr )
{
	//prt(("\ns10282 setOneCreateColumnAttribute _gettok=[%s]  _saveptr=[%s]\n", _gettok, _saveptr ));
	int rc, collen, metrics; 
	char *p; 
	Jstr defValue, s, rcs, colType, typeArg;
	int srid = JAG_DEFAULT_SRID;
	int  argcollen=0, argsig = 0;
	bool isEnum = false;

	metrics = 0;
	cattr.spare[JAG_SCHEMA_SPARE_LEN] = '\0';
	memset( cattr.spare, JAG_S_COL_SPARE_DEFAULT, JAG_SCHEMA_SPARE_LEN );
	*(cattr.spare) = JAG_C_COL_VALUE;	
	
	// save colName
	// if name is too long(over 32 bytes), error
	if ( strlen(_gettok) >= JAG_COLNAME_LENGTH_MAX ) return -19000;
	if ( strchr(_gettok, '.') ) return -19001;
	//if ( strchr( _gettok, ':' ) ) return -19002;
	if ( ! isValidCol( _gettok ) ) { 
		prt(("s51187 _gettok=[%s] ! isValidCol\n", _gettok ));
	    return -19003; 
	}

	cattr.objName.colName = makeLowerString(_gettok);
	/***
	//prt(("s2833 cattr.objName.colName=[%s]\n", _gettok ));
	if ( cattr.objName.colName.containsChar('(') ) {
		prt(("s51203 !!!!!!!!!!!!!!!! colName.contains (\n"));
		cattr.objName.colName.remove('(');
		exit(1);
	}
	***/
	
	// get column type
	//prt(("s5212 colName=[%s], get coltype ...\n", _gettok ));
	_gettok = jag_strtok_r_bracket(NULL, ",", &_saveptr);
	if ( ! _gettok ) return -19004; 
	while ( isspace(*_gettok) ) ++_gettok;
	prt(("s2434 _gettok=[%s]\n", _gettok ));
	while ( *_saveptr == ')' ) ++_saveptr;
	prt(("s2033 _gettok=[%s] saveptr=[%s]\n", _gettok, _saveptr ));
	stripEndSpace(_gettok, ',' );
	prt(("s2034 _gettok=[%s] saveptr=[%s]\n", _gettok, _saveptr ));
	if ( 0 == strncasecmp( _gettok, "mute ", 5 ) ) {
		*(cattr.spare+5) = JAG_KEY_MUTE;
		_gettok += 5;
		// colname1 mute TYPE  
	} else if ( 0 == strncasecmp( _gettok, "rollup", 6 )  ) {
		//if ( *(cattr.spare) == JAG_C_COL_KEY ) { return -9006; }
		prt(("s23339 cattr.spare=[%c]\n", *(cattr.spare) ));
		// _gettok = (char *)parseRollup( _gettok, cattr );
		*(cattr.spare+7) = JAG_ROLL_UP;
		_gettok += 6;
		if ( NULL == _gettok ) { return -19013; }
		while (isspace(*_gettok) ) ++_gettok;
		prt(("s222208 _gettok = [%s]\n", _gettok ));
		prt(("s222208 _saveptr = [%s]\n", _saveptr ));
	}

	// _gettok: "char(30) default ......."  no colname here
	// _gettok: "enum('a','b', 'c', 'd') default 'b' )
	prt(("s29034 _gettok=[%s] saveptr=[%s]\n", _gettok, _saveptr ));
	JagStrSplitWithQuote args(_gettok, ' ');
	//prt(("s11282 _gettok =[%s] _saveptr=[%s] args=\n", _gettok, _saveptr ));
	//args.print();

	// rc = getTypeNameArg( args[0].c_str(), colType, typeArg, argcollen, argsig );
	rc = getTypeNameArg( _gettok, colType, typeArg, argcollen, argsig, metrics );
	if ( rc < 0 ) {
		prt(("s20992 getTypeNameArg rc<0 rc=%d\n", rc ));
		return rc;
	}
	srid = argcollen;
	cattr.metrics = metrics;

	prt(("s1121 _gettok=[%s] colType=[%s] srid=%d metrics=%d\n", _gettok, colType.c_str(), srid, metrics ));
	if ( strchr(colType.c_str(), ' ') ) colType.remove(' ');
	if ( strchr(colType.c_str(), '\n') ) colType.remove('\n');
	if ( ! strchr( colType.c_str(), '(') && strchr(colType.c_str(), ')') ) {
		colType.remove(')');
	}

	prt(("s12122 _gettok=[%s] colType=[%s]\n", _gettok, colType.c_str() ));
	rcs = fillDataType( colType.c_str() );
	prt(("s19123 rcs=[%s]\n", rcs.c_str() ));
	if ( rcs.size() < 1  ) {
		prt(("s10286 setOneCreateColumnAttribute _gettok=[%s]  _saveptr=[%s]\n", _gettok, _saveptr ));
		return -90030;
	}
	setToRealType( rcs, cattr );
	//prt(("s3408 rcs=[%s] typeArg=[%s]\n", rcs.c_str(), typeArg.c_str() ));
	
	// if type is enum, need to format each available given strings for default strings
	if ( rcs == JAG_C_COL_TYPE_ENUM  ) {
		//prt(("s48024 JAG_C_COL_TYPE_ENUM ... typeArg=[%s]\n", typeArg.c_str() ));
		//prt(("s22393 typeArg.dump:\n"));
		//typeArg.dump();

		isEnum = true;
		collen = 0;
		cattr.type = JAG_C_COL_TYPE_STR;
		*(cattr.spare+1) = JAG_C_COL_TYPE_ENUM[0];
		p = (char*)typeArg.c_str();
		// jump possible ' \t\r\n' and '(' is exists, find valid beginning spot
		while ( isspace(*p) ) ++p;
		if ( *p == '\0' ) return -18000;
		else if ( *p == '(' ) ++p;
		while ( isspace(*p) ) ++p;
		if ( *p == '\0' ) return -18010;
		JagStrSplit sp( p, ',', true );
		for ( int i=0; i < sp.length(); ++i ) {
			if ( ! strchr( sp[i].c_str(), '\'' ) ) return -18030;
			s = trimChar( sp[i], ' ');
			s = trimChar( s, '\'');
			defValue = Jstr("'") + s + "'";
			if ( s.size() > collen ) collen = s.size();
			if ( 0==i ) {
				cattr.defValues = defValue;
			} else {
				cattr.defValues += Jstr("|") + defValue;
			}
			//prt(("s009930 i=%d cattr.defValue=[%s]\n", i, cattr.defValues.s() ));
		}
		//prt(("s5605 enum cattr.defValues=[%s]\n", cattr.defValues.c_str() ));

		cattr.offset = 0;
		cattr.length = collen;
		cattr.sig =	0;
	} else {
		// rcs != JAG_C_COL_TYPE_ENUM  
		/// prt(("s5502 _gettok=[%s] saveptr=[%s] ...\n", _gettok, _saveptr ));
		//prt(("s0230 rcs is not JAG_C_COL_TYPE_ENUM\n" ));
		//prt(("s2029 getColumnLength(%s) collen=%d _saveptr=[%s]\n",  rcs.c_str(), collen, _saveptr ));
		collen = getColumnLength( rcs );
		if ( collen < 0 ) {
			collen = argcollen;
		}
		cattr.offset = 0;
		bool isgeo = isGeoType( rcs );
		if ( isgeo ) {
			cattr.length = 0;
			cattr.srid = srid;
			if ( srid > 2000000000 ) { return -19042; }
		} else if ( rcs == JAG_C_COL_TYPE_RANGE ) {
			cattr.length = 0;
			if ( srid < 1 ) {
				srid = JAG_RANGE_DATETIME;
			}
			cattr.srid = srid;
		} else {
			cattr.length = collen;
		}

		// get sig for float/double
		if ( rcs == JAG_C_COL_TYPE_FLOAT ) {
			if ( typeArg.size() > 0 ) {
				cattr.sig = argsig;
			} else {
				cattr.length = JAG_FLOAT_FIELD_LEN;
				cattr.sig = JAG_FLOAT_SIG_LEN;
			}
			cattr.length += 2; // . and sign
			if ( cattr.sig > cattr.length ) cattr.sig = cattr.length-6;
		} else if ( rcs == JAG_C_COL_TYPE_LONGDOUBLE ) {
			if ( typeArg.size() > 0 ) {
				cattr.sig = argsig;
			} else {
				cattr.length = JAG_LONGDOUBLE_FIELD_LEN;
				cattr.sig = JAG_LONGDOUBLE_SIG_LEN;
			}
			cattr.length += 2; // . and sign
			if ( cattr.sig > cattr.length ) cattr.sig = cattr.length-10;
		} else if ( rcs == JAG_C_COL_TYPE_DOUBLE ) {
			if ( typeArg.size() > 0 ) {
				cattr.sig = argsig;
			} else {
				cattr.length = JAG_DOUBLE_FIELD_LEN;
				cattr.sig = JAG_DOUBLE_SIG_LEN;
			}
			cattr.length += 2; // . and sign
			if ( cattr.sig > cattr.length ) cattr.sig = cattr.length-8;
		} else if ( rcs == JAG_C_COL_TEMPTYPE_REAL  ) {
			cattr.sig = 8;
		} else {
			cattr.sig = 0;
		}
	}

	prt(("s0982 args.length()=%d\n", args.length() ));
	if ( args.length()>= 1 ) {
		if ( args.length()>=2 && strcasecmp(args[1].c_str(), "default"  ) == 0 ) {
			if ( cattr.type == JAG_C_COL_TYPE_STR && 
				(JAG_C_COL_TYPE_UUID[0] == *(cattr.spare+1) || JAG_C_COL_TYPE_FILE[0] == *(cattr.spare+1)) ) {
				// column of UUID/FILE type does not accept default value
				return -19060;
			}

			if ( args.length()>= 3 ) {
					if ( isDateAndTime(cattr.type) || cattr.type == JAG_C_COL_TYPE_DATE ) {
					if ( strcasecmp( args[2].c_str(), "current_timestamp") == 0 ) {
						if ( args.length()>= 4 && strcasecmp( args[3].c_str(), "on") == 0 ) {
							if ( args.length()>=5 && strcasecmp(args[4].c_str(), "update") == 0 ) {	
								if ( args.length()>=6 && strcasecmp(args[5].c_str(), "current_timestamp") == 0 ) {
									if ( cattr.type == JAG_C_COL_TYPE_DATE ) { 
										*(cattr.spare+4) = JAG_CREATE_DEFUPDATE_DATE;
									} else if ( cattr.type == JAG_C_COL_TYPE_DATETIMESEC || cattr.type == JAG_C_COL_TYPE_TIMESTAMPSEC ) {
										*(cattr.spare+4) = JAG_CREATE_DEFUPDATE_DATETIMESEC;
									} else if ( cattr.type == JAG_C_COL_TYPE_DATETIMEMILL || cattr.type == JAG_C_COL_TYPE_TIMESTAMPMILL ) {
										*(cattr.spare+4) = JAG_CREATE_DEFUPDATE_DATETIMEMILL;
									} else if ( cattr.type == JAG_C_COL_TYPE_DATETIME || cattr.type == JAG_C_COL_TYPE_TIMESTAMP ) {
										*(cattr.spare+4) = JAG_CREATE_DEFUPDATE_DATETIME;
									} else if ( cattr.type == JAG_C_COL_TYPE_TIMESTAMPNANO || cattr.type == JAG_C_COL_TYPE_DATETIMENANO ) {
										*(cattr.spare+4) = JAG_CREATE_DEFUPDATE_DATETIMENANO;
									} else {
										return -19071;
									}
								} else { return -19070; }
							} else { return -19080; }
						} else {
							if ( cattr.type == JAG_C_COL_TYPE_DATE ) {
								*(cattr.spare+4) = JAG_CREATE_DEFDATE;
							} else if ( cattr.type == JAG_C_COL_TYPE_DATETIMESEC || cattr.type == JAG_C_COL_TYPE_TIMESTAMPSEC  ) {
								*(cattr.spare+4) = JAG_CREATE_DEFDATETIMESEC;
							} else if ( cattr.type == JAG_C_COL_TYPE_DATETIMEMILL || cattr.type == JAG_C_COL_TYPE_TIMESTAMPMILL  ) {
								*(cattr.spare+4) = JAG_CREATE_DEFDATETIMEMILL;
							} else if ( cattr.type == JAG_C_COL_TYPE_DATETIME || cattr.type == JAG_C_COL_TYPE_TIMESTAMP ) {
								*(cattr.spare+4) = JAG_CREATE_DEFDATETIME;
							} else if ( cattr.type == JAG_C_COL_TYPE_TIMESTAMPNANO || cattr.type == JAG_C_COL_TYPE_DATETIMENANO ) {
								*(cattr.spare+4) = JAG_CREATE_DEFDATETIMENANO;
							} else {
								return -19081;
							}
						}
					} else { return -19090; }
				} else if ( cattr.type == JAG_C_COL_TYPE_DBIT ) {
					if ( strchr( args[2].c_str(), '1' ) ) {
						defValue = "'1'";
					} else {
						defValue = "'0'";
					}
					*(cattr.spare+4) = JAG_CREATE_DEFINSERTVALUE;
					if ( isEnum ) {
						cattr.defValues += Jstr("|") + defValue;
					} else {
						cattr.defValues = defValue;
					}
					*(cattr.spare+4) = JAG_CREATE_DEFINSERTVALUE;
				} else {
					if ( strchr( args[2].c_str(), '\'' ) || strchr( args[2].c_str(), '"' ) ) {
						defValue = args[2];
						prt(("s3041 defValue=[%s]\n", defValue.c_str() ));
						if ( defValue.size() > 32 ) { return -19153; } // max of 32 bytes in default value !!!!!
						if ( JAG_C_COL_TYPE_ENUM[0] == *(cattr.spare+1) && cattr.length < defValue.size() ) {
							cattr.length = defValue.size();
						}
						//prt(("s5303 cattr.defValues=[%s] defValue=[%s]\n", cattr.defValues.c_str(), defValue.c_str() ));
						if ( isEnum ) {
							// cattr.defValues
							JagStrSplit sp(cattr.defValues, '|', true );
							if ( sp.length()<1) return -19147;
							bool ok = false;
							for ( int i=0; i < sp.length(); ++i ) {
								if ( defValue == sp[i] ) { ok = true; break; }
							}
							if ( ! ok ) return -19148;
							cattr.defValues += Jstr("|") + defValue;
							// previous ones are choices; last one is default value;
							// *(cattr.spare+1) ===> indicates enum type
						} else {
							cattr.defValues = defValue;
						}
						*(cattr.spare+4) = JAG_CREATE_DEFINSERTVALUE;
						#if 0
						prt(("s3042 cattr.colName=[%s] cattr.defValues=[%s]\n", 
							  cattr.objName.colName.c_str(), cattr.defValues.c_str() ));
					    #endif
					} else { return -19150; }
				}
			} else { return -19160; }
		} else if ( args.length()>=2 && strcasecmp( args[1].c_str(), "on" ) == 0 ) {
			if ( isDateAndTime(cattr.type) || cattr.type == JAG_C_COL_TYPE_DATE ) {
				if ( args.length()>=3 && strcasecmp(args[2].c_str(), "update") == 0 ) {	
					if ( args.length()>=4 && strcasecmp(args[3].c_str(), "current_timestamp") == 0 ) {
						if ( cattr.type == JAG_C_COL_TYPE_DATE ) {
							*(cattr.spare+4) = JAG_CREATE_UPDATE_DATE;
						} else if ( cattr.type == JAG_C_COL_TYPE_DATETIMESEC || cattr.type == JAG_C_COL_TYPE_TIMESTAMPSEC ) {
							*(cattr.spare+4) = JAG_CREATE_UPDATE_DATETIMESEC;
						} else if ( cattr.type == JAG_C_COL_TYPE_DATETIMENANO || cattr.type == JAG_C_COL_TYPE_TIMESTAMPNANO ) {
							*(cattr.spare+4) = JAG_CREATE_UPDATE_DATETIMENANO;
						} else if ( cattr.type == JAG_C_COL_TYPE_DATETIMEMILL || cattr.type == JAG_C_COL_TYPE_TIMESTAMPMILL ) {
							*(cattr.spare+4) = JAG_CREATE_UPDATE_DATETIMEMILL;
						} else if ( cattr.type == JAG_C_COL_TYPE_DATETIME || cattr.type == JAG_C_COL_TYPE_TIMESTAMP ) {
							*(cattr.spare+4) = JAG_CREATE_UPDATE_DATETIME;
						} else {
							return -19163;
						}
					} else { return -19170; }
				} else { return -19170; }
			} else { return -19180; }
		} 
	} 

	//prt(("s2482 _saveptr=[%s]\n", _saveptr ));
	while ( isspace(*_saveptr) ) ++_saveptr;
	while ( *_saveptr == ')' ) ++_saveptr;
	while ( *_saveptr == ',' ) ++_saveptr;
	return 1;
}


Jstr JagParser::fillDataType( const char* gettok )
{
	Jstr rc;
	if (strcasecmp(gettok, "char") == 0) {
		rc = JAG_C_COL_TYPE_STR;
	} else if (strcasecmp(gettok, "varchar") == 0 || strcasecmp(gettok, "varchar2") == 0) {
		rc = JAG_C_COL_TYPE_STR;
	} else if (strcasecmp(gettok, "string") == 0) {
		rc = JAG_C_COL_TEMPTYPE_STRING;
	} else if (strcasecmp(gettok, "text") == 0 || strcasecmp(gettok, "blob") == 0 || strcasecmp(gettok, "clob") == 0 ) {
		rc = JAG_C_COL_TEMPTYPE_TEXT;
	} else if (strcasecmp(gettok, "longtext") == 0 || strcasecmp(gettok, "longblob") == 0 ) {
		rc = JAG_C_COL_TEMPTYPE_LONGTEXT;
	} else if (strcasecmp(gettok, "mediumtext") == 0 || strcasecmp(gettok, "mediumblob") == 0 ) {
		rc = JAG_C_COL_TEMPTYPE_MEDIUMTEXT;
	} else if (strcasecmp(gettok, "tinytext") == 0 || strcasecmp(gettok, "tinyblob") == 0 ) {
		rc = JAG_C_COL_TEMPTYPE_TINYTEXT;
	} else if (strcasecmp(gettok, "boolean") == 0) {
		rc = JAG_C_COL_TYPE_DBOOLEAN;
	} else if (strcasecmp(gettok, "bool") == 0) {
		rc = JAG_C_COL_TYPE_DBOOLEAN;
	} else if (strcasecmp(gettok, "bit") == 0) {
		rc = JAG_C_COL_TYPE_DBIT;
	} else if (strcasecmp(gettok, "tinyint") == 0) {
		rc = JAG_C_COL_TYPE_DTINYINT;
	} else if (strcasecmp(gettok, "smallint") == 0) {
		rc = JAG_C_COL_TYPE_DSMALLINT;
	} else if (strcasecmp(gettok, "mediumint") == 0) {
		rc = JAG_C_COL_TYPE_DMEDINT;
	} else if (strcasecmp(gettok, "bigint") == 0) {
		rc = JAG_C_COL_TYPE_DBIGINT;
	} else if (strcasecmp(gettok, "int") == 0 || strcasecmp(gettok, "integer") == 0) {
		rc = JAG_C_COL_TYPE_DINT;
	} else if (strcasecmp(gettok, "float") == 0) {
		rc = JAG_C_COL_TYPE_FLOAT;
	} else if (strcasecmp(gettok, "longdouble") == 0) {
		rc = JAG_C_COL_TYPE_LONGDOUBLE;
	} else if ( strcasecmp(gettok, "double") == 0 || strcasecmp(gettok, "numeric") == 0 || 
				strcasecmp(gettok, "decimal") == 0) {
		rc = JAG_C_COL_TYPE_DOUBLE;
	} else if (strcasecmp(gettok, "real") == 0) {
		rc = JAG_C_COL_TEMPTYPE_REAL;
	} else if (strcasecmp(gettok, "datetimenano") == 0) {
		rc = JAG_C_COL_TYPE_DATETIMENANO;
	} else if (strcasecmp(gettok, "datetimesec") == 0) {
		rc = JAG_C_COL_TYPE_DATETIMESEC;
	} else if (strcasecmp(gettok, "datetimemill") == 0) {
		rc = JAG_C_COL_TYPE_DATETIMEMILL;
	} else if (strcasecmp(gettok, "datetime") == 0) {
		rc = JAG_C_COL_TYPE_DATETIME;
	} else if (strcasecmp(gettok, "timestampnano") == 0) {
		rc = JAG_C_COL_TYPE_TIMESTAMPNANO;
	} else if (strcasecmp(gettok, "timestampmill") == 0) {
		rc = JAG_C_COL_TYPE_TIMESTAMPMILL;
	} else if (strcasecmp(gettok, "timestampsec") == 0) {
		rc = JAG_C_COL_TYPE_TIMESTAMPSEC;
	} else if (strcasecmp(gettok, "timestamp") == 0) {
		rc = JAG_C_COL_TYPE_TIMESTAMP;
	} else if (strcasecmp(gettok, "timenano") == 0) {
		rc = JAG_C_COL_TYPE_TIMENANO;
	} else if (strcasecmp(gettok, "time") == 0) {
		rc = JAG_C_COL_TYPE_TIME;
	} else if (strcasecmp(gettok, "date") == 0) {
		rc = JAG_C_COL_TYPE_DATE;
	} else if (strcasecmp(gettok, "uuid") == 0) {
		rc = JAG_C_COL_TYPE_UUID;
	} else if (strcasecmp(gettok, "file") == 0) {
		rc = JAG_C_COL_TYPE_FILE;
	} else if (strcasecmp(gettok, "enum") == 0) {
		rc = JAG_C_COL_TYPE_ENUM;
	} else if (strcasecmp(gettok, "point3d") == 0) {
		rc = JAG_C_COL_TYPE_POINT3D;
	} else if (strcasecmp(gettok, "point") == 0) {
		rc = JAG_C_COL_TYPE_POINT;
	} else if (strcasecmp(gettok, "circle") == 0) {
		rc = JAG_C_COL_TYPE_CIRCLE;
	} else if (strcasecmp(gettok, "circle3d") == 0) {
		rc = JAG_C_COL_TYPE_CIRCLE3D;
	} else if (strcasecmp(gettok, "sphere") == 0) {
		rc = JAG_C_COL_TYPE_SPHERE;
	} else if (strcasecmp(gettok, "square3d") == 0) {
		rc = JAG_C_COL_TYPE_SQUARE3D;
	} else if (strcasecmp(gettok, "square") == 0) {
		rc = JAG_C_COL_TYPE_SQUARE;
	} else if (strcasecmp(gettok, "cube") == 0) {
		rc = JAG_C_COL_TYPE_CUBE;
	} else if (strcasecmp(gettok, "rectangle3d") == 0) {
		rc = JAG_C_COL_TYPE_RECTANGLE3D;
	} else if (strcasecmp(gettok, "rectangle") == 0) {
		rc = JAG_C_COL_TYPE_RECTANGLE;
	} else if (strcasecmp(gettok, "box") == 0) {
		rc = JAG_C_COL_TYPE_BOX;
	} else if (strcasecmp(gettok, "triangle3d") == 0) {
		rc = JAG_C_COL_TYPE_TRIANGLE3D;
	} else if (strcasecmp(gettok, "triangle") == 0) {
		rc = JAG_C_COL_TYPE_TRIANGLE;
	} else if (strcasecmp(gettok, "cylinder") == 0) {
		rc = JAG_C_COL_TYPE_CYLINDER;
	} else if (strcasecmp(gettok, "cone") == 0) {
		rc = JAG_C_COL_TYPE_CONE;
	} else if (strcasecmp(gettok, "ellipsoid") == 0) {
		rc = JAG_C_COL_TYPE_ELLIPSOID;
	} else if (strcasecmp(gettok, "ellipse") == 0) {
		rc = JAG_C_COL_TYPE_ELLIPSE;
	} else if (strcasecmp(gettok, "ellipse3d") == 0) {
		rc = JAG_C_COL_TYPE_ELLIPSE3D;
	} else if (strcasecmp(gettok, "line") == 0) {
		rc = JAG_C_COL_TYPE_LINE;
	} else if (strcasecmp(gettok, "line3d") == 0) {
		rc = JAG_C_COL_TYPE_LINE3D;
	} else if (strcasecmp(gettok, "linestring") == 0) {
		rc = JAG_C_COL_TYPE_LINESTRING;
	} else if (strcasecmp(gettok, "linestring3d") == 0) {
		rc = JAG_C_COL_TYPE_LINESTRING3D;
	} else if (strcasecmp(gettok, "multipoint") == 0) {
		rc = JAG_C_COL_TYPE_MULTIPOINT;
	} else if (strcasecmp(gettok, "multipoint3d") == 0) {
		rc = JAG_C_COL_TYPE_MULTIPOINT3D;
	} else if (strcasecmp(gettok, "polygon") == 0) {
		rc = JAG_C_COL_TYPE_POLYGON;
	} else if (strcasecmp(gettok, "polygon3d") == 0) {
		rc = JAG_C_COL_TYPE_POLYGON3D;
	} else if (strcasecmp(gettok, "multilinestring") == 0) {
		rc = JAG_C_COL_TYPE_MULTILINESTRING;
	} else if (strcasecmp(gettok, "multilinestring3d") == 0) {
		rc = JAG_C_COL_TYPE_MULTILINESTRING3D;
	} else if (strcasecmp(gettok, "multipolygon") == 0) {
		rc = JAG_C_COL_TYPE_MULTIPOLYGON;
	} else if (strcasecmp(gettok, "multipolygon3d") == 0) {
		rc = JAG_C_COL_TYPE_MULTIPOLYGON3D;
	} else if (strcasecmp(gettok, "range") == 0) {
		rc = JAG_C_COL_TYPE_RANGE;
	} else {
		prt(("s2201 unknown gettok=[%s] rc=0\n", gettok ));
		rc = "";
	}

	return rc;
}

int JagParser::getColumnLength( const Jstr &colType )
{
	int onelen;
	if ( colType == JAG_C_COL_TYPE_DATETIME ||  colType == JAG_C_COL_TYPE_TIMESTAMP ) {
		onelen = JAG_DATETIME_FIELD_LEN;
	} else if ( colType == JAG_C_COL_TYPE_DATETIMESEC || colType == JAG_C_COL_TYPE_TIMESTAMPSEC ) {
		onelen = JAG_DATETIMESEC_FIELD_LEN;
	} else if ( colType == JAG_C_COL_TYPE_DATETIMENANO || colType == JAG_C_COL_TYPE_TIMESTAMPNANO ) {
		onelen = JAG_DATETIMENANO_FIELD_LEN;
	} else if ( colType == JAG_C_COL_TYPE_DATETIMEMILL || colType == JAG_C_COL_TYPE_TIMESTAMPMILL ) {
		onelen = JAG_DATETIMEMILL_FIELD_LEN;
	} else if ( colType == JAG_C_COL_TYPE_TIME ) {
		onelen = JAG_TIME_FIELD_LEN;
	} else if ( colType == JAG_C_COL_TYPE_TIMENANO ) {
		onelen = JAG_TIMENANO_FIELD_LEN;
	} else if ( colType == JAG_C_COL_TYPE_DATE ) {
		onelen = JAG_DATE_FIELD_LEN;
	} else if ( colType == JAG_C_COL_TYPE_UUID ) {
		onelen = JAG_UUID_FIELD_LEN;
	} else if ( colType == JAG_C_COL_TYPE_FILE ) {
		onelen = JAG_FILE_FIELD_LEN;
	} else if ( colType == JAG_C_COL_TYPE_DBOOLEAN ) {
		onelen = JAG_DBOOLEAN_FIELD_LEN;
	} else if ( colType == JAG_C_COL_TYPE_DBIT ) {
		onelen = JAG_DBIT_FIELD_LEN;
	} else if ( colType == JAG_C_COL_TYPE_DINT ) {
		onelen = JAG_DINT_FIELD_LEN;
	} else if ( colType == JAG_C_COL_TYPE_DTINYINT ) {
		onelen = JAG_DTINYINT_FIELD_LEN;
	} else if ( colType == JAG_C_COL_TYPE_DSMALLINT ) {
		onelen = JAG_DSMALLINT_FIELD_LEN;
	} else if ( colType == JAG_C_COL_TYPE_DMEDINT ) {
		onelen = JAG_DMEDINT_FIELD_LEN;
	} else if ( colType == JAG_C_COL_TYPE_DBIGINT ) {
		onelen = JAG_DBIGINT_FIELD_LEN;
	} else if ( colType == JAG_C_COL_TEMPTYPE_REAL ) {
		onelen = JAG_REAL_FIELD_LEN; // 40
	} else if ( colType == JAG_C_COL_TEMPTYPE_TEXT ) {
		onelen = JAG_TEXT_FIELD_LEN; // 1024
	} else if ( colType == JAG_C_COL_TEMPTYPE_MEDIUMTEXT ) {
		onelen = JAG_MEDIUMTEXT_FIELD_LEN; // 2048
	} else if ( colType == JAG_C_COL_TEMPTYPE_LONGTEXT ) {
		onelen = JAG_LONGTEXT_FIELD_LEN; // 10240
	} else if ( colType == JAG_C_COL_TEMPTYPE_TINYTEXT ) {
		onelen = JAG_TINYTEXT_FIELD_LEN; // 256
	} else if ( colType == JAG_C_COL_TEMPTYPE_STRING ) {
		onelen = JAG_STRING_FIELD_LEN; // 64
	} else {
		onelen = -1;
	}

	return onelen;
}

bool JagParser::isValidGrantPerm( Jstr &perm )
{
	Jstr newperm, str, lperm;
	JagStrSplit sp( perm, ',', true );
	for ( int i = 0; i < sp.length(); ++i ) {
		str = trimChar(sp[i], ' ');
    	lperm = makeLowerString(str);
    	if ( lperm == "all" ) {
    		perm = JAG_ROLE_ALL;
			return true;
		}

    	if ( lperm == "select" ) {
    		newperm += Jstr(JAG_ROLE_SELECT) + ",";
    	} else if ( lperm == "insert" ) {
    		newperm += Jstr(JAG_ROLE_INSERT) + ",";
    	} else if ( lperm == "update" ) {
    		newperm += Jstr(JAG_ROLE_UPDATE) + ",";
    	} else if ( lperm == "delete" ) {
    		newperm += Jstr(JAG_ROLE_DELETE) + ",";
    	} else if ( lperm == "create" ) {
    		newperm += Jstr(JAG_ROLE_CREATE) + ",";
    	} else if ( lperm == "drop" ) {
    		newperm += Jstr(JAG_ROLE_DROP) + ",";
    	} else if ( lperm == "alter" ) {
    		newperm += Jstr(JAG_ROLE_ALTER) + ",";
    	} else if ( lperm == "truncate" ) {
    		newperm += Jstr(JAG_ROLE_TRUNCATE) + ",";
    	} else {
			// errmsg = Jstr(lperm) + " is not a valid permission";
			return false;
    	}
	}

	perm = trimTailChar(newperm.c_str(), ',');
	// prt(("s9283 isValidGrantPerm perm=[%s]\n", perm.c_str() ));
	return true;
}

bool JagParser::isValidGrantObj( Jstr &obj )
{
	bool rc;
	JagStrSplit sp(obj, '.' );
	if ( sp.length() >= 1 && sp.length() <=3 ) rc = true;
	else rc = false;

	if ( sp.length() == 1 ) {
		if ( 0==strcasecmp( obj.c_str(), "all" ) ) {
			obj = "*.*.*";
		} else if ( 0==strcasecmp( obj.c_str(), "*" )  ) {
			obj = "*.*.*";
		} else {
			obj = obj + ".*.*";
		}
	} else if ( sp.length() == 2 ) {
		obj = obj + ".*";
	} 

	return rc;
}

// get JaColumn object reference give a colName
const JagColumn* JagParser::getColumn( const JagParseParam *pparam, const Jstr &colName  ) const
{
	if ( ! pparam ) return NULL;
	if (  pparam->objectVec.size() < 1 ) {
	 	return NULL;
	}

	Jstr db = pparam->objectVec[0].dbName;
	Jstr objcol = pparam->objectVec[0].colName;
	if ( db != objcol && objcol.size()>0 ) db = objcol;
	if ( db.size() < 1 ) {
	 	return NULL;
	}

	Jstr objname = pparam->objectVec[0].tableName;
	if ( pparam->objectVec[0].indexName.size() > 0 ) {
		objname = pparam->objectVec[0].indexName;
	}

	return getColumn( db, objname, colName );
}


bool JagParser::isPolyType( const Jstr &rcs )
{
	if ( rcs.size() < 1 ) return false;
	if ( rcs == JAG_C_COL_TYPE_LINESTRING || rcs == JAG_C_COL_TYPE_LINESTRING3D
		 || rcs == JAG_C_COL_TYPE_MULTIPOINT || rcs == JAG_C_COL_TYPE_MULTIPOINT3D
		 || rcs == JAG_C_COL_TYPE_MULTILINESTRING || rcs == JAG_C_COL_TYPE_MULTILINESTRING3D
		 || rcs == JAG_C_COL_TYPE_MULTIPOLYGON || rcs == JAG_C_COL_TYPE_MULTIPOLYGON3D
		 || rcs == JAG_C_COL_TYPE_POLYGON || rcs == JAG_C_COL_TYPE_POLYGON3D ) {
		 return true;
	}
	return false;
}

bool JagParser::isVectorGeoType( const Jstr &rcs )
{
	if ( rcs.size() < 1 ) return false;

	if ( 
		    rcs == JAG_C_COL_TYPE_CIRCLE || rcs == JAG_C_COL_TYPE_SPHERE 
		 || rcs == JAG_C_COL_TYPE_SQUARE || rcs == JAG_C_COL_TYPE_CUBE 
		 || rcs == JAG_C_COL_TYPE_SQUARE3D 
		 || rcs == JAG_C_COL_TYPE_CIRCLE3D 
		 || rcs == JAG_C_COL_TYPE_TRIANGLE 
		 || rcs == JAG_C_COL_TYPE_TRIANGLE3D 
		 || rcs == JAG_C_COL_TYPE_CYLINDER 
		 || rcs == JAG_C_COL_TYPE_CONE 
		 || rcs == JAG_C_COL_TYPE_BOX 
		 || rcs == JAG_C_COL_TYPE_ELLIPSE
		 || rcs == JAG_C_COL_TYPE_ELLIPSE3D
		 || rcs == JAG_C_COL_TYPE_ELLIPSOID
		 || rcs == JAG_C_COL_TYPE_RECTANGLE3D
		 || rcs == JAG_C_COL_TYPE_RECTANGLE 
	   ) 
	 {
		 return true;
	 } 
 	 return false;
}

int JagParser::vectorShapeCoordinates( const Jstr &rcs )
{
	if ( rcs == JAG_C_COL_TYPE_POINT ) return 2; // x y
	if ( rcs == JAG_C_COL_TYPE_POINT3D ) return 3; // x y z
	if ( rcs == JAG_C_COL_TYPE_LINE ) return 4; // x1 y1 x2 y2
	if ( rcs == JAG_C_COL_TYPE_LINE3D ) return 6; // x1 y1 z1 x2 y2 z2
	if ( rcs == JAG_C_COL_TYPE_TRIANGLE ) return 6;  // x1 y1 x2 y2 x3 y3
	if ( rcs == JAG_C_COL_TYPE_TRIANGLE3D ) return 9;
	if ( rcs == JAG_C_COL_TYPE_SQUARE ) return 4;  // x y a nx
	if ( rcs == JAG_C_COL_TYPE_SQUARE3D ) return 6; // x y z a nx ny
	if ( rcs == JAG_C_COL_TYPE_RECTANGLE ) return 5; // x y a b nx
	if ( rcs == JAG_C_COL_TYPE_RECTANGLE3D ) return 7;  // x y z a b nx ny
	if ( rcs == JAG_C_COL_TYPE_CIRCLE ) return 3;  // x y a
	if ( rcs == JAG_C_COL_TYPE_CIRCLE3D ) return 6; // x y z a nx ny
	if ( rcs == JAG_C_COL_TYPE_ELLIPSE ) return 5; // x y a b nx
	if ( rcs == JAG_C_COL_TYPE_ELLIPSE3D ) return 7; // x y z a b nx ny
	if ( rcs == JAG_C_COL_TYPE_CYLINDER ) return 7; // x y z a h nx ny
	if ( rcs == JAG_C_COL_TYPE_CONE ) return 7;  // x y z a h nx ny
	if ( rcs == JAG_C_COL_TYPE_SPHERE ) return 4; // x y z a
	if ( rcs == JAG_C_COL_TYPE_ELLIPSOID ) return 8; // x y z a b c nx ny
	if ( rcs == JAG_C_COL_TYPE_CUBE ) return 6;  // x y z a nx ny
	if ( rcs == JAG_C_COL_TYPE_BOX ) return 8; // x y z a b c nx ny

	return 0;
}

bool JagParser::isGeoType( const Jstr &rcs ) 
{
	if ( rcs.size() < 1 ) return false;

	if ( rcs == JAG_C_COL_TYPE_POINT || rcs == JAG_C_COL_TYPE_POINT3D
		     || rcs == JAG_C_COL_TYPE_LINE || rcs == JAG_C_COL_TYPE_LINE3D
		     || rcs == JAG_C_COL_TYPE_LINESTRING || rcs == JAG_C_COL_TYPE_LINESTRING3D
		     || rcs == JAG_C_COL_TYPE_MULTILINESTRING || rcs == JAG_C_COL_TYPE_MULTILINESTRING3D
		     || rcs == JAG_C_COL_TYPE_MULTIPOINT || rcs == JAG_C_COL_TYPE_MULTIPOINT3D
			 || rcs == JAG_C_COL_TYPE_POLYGON || rcs == JAG_C_COL_TYPE_POLYGON3D 
			 || rcs == JAG_C_COL_TYPE_MULTIPOLYGON || rcs == JAG_C_COL_TYPE_MULTIPOLYGON3D 
			 || rcs == JAG_C_COL_TYPE_CIRCLE || rcs == JAG_C_COL_TYPE_SPHERE 
			 || rcs == JAG_C_COL_TYPE_SQUARE || rcs == JAG_C_COL_TYPE_CUBE 
			 || rcs == JAG_C_COL_TYPE_SQUARE3D 
			 || rcs == JAG_C_COL_TYPE_CIRCLE3D 
			 || rcs == JAG_C_COL_TYPE_TRIANGLE 
			 || rcs == JAG_C_COL_TYPE_TRIANGLE3D 
			 || rcs == JAG_C_COL_TYPE_CYLINDER 
			 || rcs == JAG_C_COL_TYPE_CONE 
			 || rcs == JAG_C_COL_TYPE_ELLIPSE
			 || rcs == JAG_C_COL_TYPE_ELLIPSE3D
			 || rcs == JAG_C_COL_TYPE_ELLIPSOID
			 || rcs == JAG_C_COL_TYPE_RECTANGLE3D
			 || rcs == JAG_C_COL_TYPE_RECTANGLE || rcs == JAG_C_COL_TYPE_BOX
		   ) 
	 {
		 return true;
	 } 

 	 return false;
}

// complex types:  geo type, other
bool JagParser::isComplexType( const Jstr &rcs ) 
{
	if ( rcs.size() < 1 ) return false;
	bool rc = isGeoType( rcs );
	if ( rc ) return rc;

	return false;
}

void JagParser::replaceChar( char *start, char oldc, char newc, char stopchar )
{
	if ( NULL == start || *start == '\0' ) return;
	while ( *start != stopchar && *start != '\0' ) {
		if ( *start == oldc ) {
			*start = newc;
		}
		++start;
	}

	if ( *start == stopchar ) {
		*start = newc;
	}
}


// _ptrParam is modified
void JagParser::addCreateAttrAndColumn(bool isValue, CreateAttribute &cattr, int &coloffset  )
{
	int ncol;

	//prt(("s3539 done createAttrVec.append cattr.type=[%s]\n", cattr.type.c_str() ));
	if ( cattr.type == JAG_C_COL_TYPE_POINT ) {
		//prt(("s5093 cattr.offset=%d\n", cattr.offset ));
		 cattr.begincol = _ptrParam->createAttrVec.size() +1;
		 cattr.endcol = _ptrParam->createAttrVec.size() +JAG_POINT_DIM + cattr.metrics;
		 *(cattr.spare+2) = JAG_ASC;
		 _ptrParam->createAttrVec.append( cattr );
		 _ptrParam->addPointColumns( cattr );
		 coloffset += JAG_POINT_DIM*JAG_GEOM_TOTLEN + cattr.metrics*JAG_METRIC_LEN;
    } else if ( cattr.type == JAG_C_COL_TYPE_POINT3D ) {
		//prt(("s5094 cattr.offset=%d\n", cattr.offset ));
		 cattr.begincol = _ptrParam->createAttrVec.size() +1;
		 cattr.endcol = _ptrParam->createAttrVec.size() +JAG_POINT3D_DIM + cattr.metrics;
		 *(cattr.spare+2) = JAG_ASC;
		 _ptrParam->createAttrVec.append( cattr );
		 _ptrParam->addPoint3DColumns( cattr );
		 coloffset += JAG_POINT3D_DIM*JAG_GEOM_TOTLEN + cattr.metrics*JAG_METRIC_LEN;
    } else if ( cattr.type == JAG_C_COL_TYPE_CIRCLE ) {
		 cattr.begincol = _ptrParam->createAttrVec.size() +1;
		 cattr.endcol = _ptrParam->createAttrVec.size() +JAG_CIRCLE_DIM + cattr.metrics;
		 *(cattr.spare+2) = JAG_ASC;
		 _ptrParam->createAttrVec.append( cattr );
		 _ptrParam->addCircleColumns( cattr );
		 coloffset += JAG_CIRCLE_DIM*JAG_GEOM_TOTLEN + cattr.metrics*JAG_METRIC_LEN;
    } else if ( cattr.type == JAG_C_COL_TYPE_SPHERE ) {
		 cattr.begincol = _ptrParam->createAttrVec.size() +1;
		 cattr.endcol = _ptrParam->createAttrVec.size() +JAG_SPHERE_DIM + cattr.metrics;
		 *(cattr.spare+2) = JAG_ASC;
		 _ptrParam->createAttrVec.append( cattr );
		 _ptrParam->addColumns( cattr, 1,1,1, 1,0,0, 0,0 );
		 coloffset += JAG_SPHERE_DIM*JAG_GEOM_TOTLEN + + cattr.metrics*JAG_METRIC_LEN;
		 //prt(("s5003 JAG_C_COL_TYPE_SPHERE added 4 columns\n" ));
    } else if (  cattr.type == JAG_C_COL_TYPE_SQUARE3D
				|| cattr.type == JAG_C_COL_TYPE_CIRCLE3D ) {
		 cattr.begincol = _ptrParam->createAttrVec.size() +1;
		 ncol = JAG_SQUARE3D_DIM;
		 cattr.endcol = _ptrParam->createAttrVec.size() + ncol + cattr.metrics;
		 *(cattr.spare+2) = JAG_ASC;
		 _ptrParam->createAttrVec.append( cattr );
		 _ptrParam->addColumns( cattr, 1,1,1, 1,0,0, 1,1 );
		 coloffset += ncol*JAG_GEOM_TOTLEN + + cattr.metrics*JAG_METRIC_LEN; 
    } else if ( cattr.type == JAG_C_COL_TYPE_SQUARE ) {
		 cattr.begincol = _ptrParam->createAttrVec.size() +1;
		 cattr.endcol = _ptrParam->createAttrVec.size() +JAG_SQUARE_DIM + cattr.metrics;
		 *(cattr.spare+2) = JAG_ASC;
		 _ptrParam->createAttrVec.append( cattr );
		 _ptrParam->addSquareColumns( cattr );
		 coloffset += JAG_SQUARE_DIM*JAG_GEOM_TOTLEN + + cattr.metrics*JAG_METRIC_LEN;
    } else if ( cattr.type == JAG_C_COL_TYPE_CUBE ) {
		 cattr.begincol = _ptrParam->createAttrVec.size() +1;
		 cattr.endcol = _ptrParam->createAttrVec.size() +JAG_CUBE_DIM + cattr.metrics;
		 *(cattr.spare+2) = JAG_ASC;
		 _ptrParam->createAttrVec.append( cattr );
		 _ptrParam->addColumns( cattr, 1,1,1, 1,0,0, 1,1 );
		 coloffset += JAG_CUBE_DIM*JAG_GEOM_TOTLEN + cattr.metrics*JAG_METRIC_LEN;  
    } else if ( cattr.type == JAG_C_COL_TYPE_RECTANGLE || cattr.type == JAG_C_COL_TYPE_ELLIPSE ) {
		 cattr.begincol = _ptrParam->createAttrVec.size() +1;
		 cattr.endcol = _ptrParam->createAttrVec.size() +JAG_RECTANGLE_DIM + cattr.metrics;
		 *(cattr.spare+2) = JAG_ASC;
		 _ptrParam->createAttrVec.append( cattr );
		 _ptrParam->addColumns( cattr, 1,1,0, 1,1,0, 1,0 );
		 coloffset += JAG_RECTANGLE_DIM*JAG_GEOM_TOTLEN + cattr.metrics*JAG_METRIC_LEN;  // 5 columns x,y,width, height nx
		 //prt(("s5013 JAG_C_COL_TYPE_ELLIPSE added 6 columns\n" ));
    } else if ( cattr.type == JAG_C_COL_TYPE_RECTANGLE3D || cattr.type == JAG_C_COL_TYPE_ELLIPSE3D ) { 
		 cattr.begincol = _ptrParam->createAttrVec.size() +1;
		 ncol = JAG_RECTANGLE3D_DIM;
		 cattr.endcol = _ptrParam->createAttrVec.size() +ncol + cattr.metrics;
		 *(cattr.spare+2) = JAG_ASC;
		 _ptrParam->createAttrVec.append( cattr );
		 _ptrParam->addColumns( cattr, 1,1,1, 1,1,0, 1,1 );
		 coloffset += ncol*JAG_GEOM_TOTLEN + cattr.metrics*JAG_METRIC_LEN;  // 7 columns x y z width height nx ny
    } else if ( cattr.type == JAG_C_COL_TYPE_BOX
				|| cattr.type == JAG_C_COL_TYPE_ELLIPSOID ) {
		 cattr.begincol = _ptrParam->createAttrVec.size() +1;
		 ncol = JAG_ELLIPSOID_DIM;
		 cattr.endcol = _ptrParam->createAttrVec.size() + ncol + cattr.metrics;
		 *(cattr.spare+2) = JAG_ASC;
		 _ptrParam->createAttrVec.append( cattr );
		 _ptrParam->addBoxColumns( cattr );
		 coloffset += ncol*JAG_GEOM_TOTLEN + cattr.metrics*JAG_METRIC_LEN;
    } else if ( cattr.type == JAG_C_COL_TYPE_CYLINDER || cattr.type == JAG_C_COL_TYPE_CONE ) {
		 ncol = JAG_CONE_DIM;
		 cattr.begincol = _ptrParam->createAttrVec.size() +1;
		 cattr.endcol = _ptrParam->createAttrVec.size() +ncol + cattr.metrics;
		 *(cattr.spare+2) = JAG_ASC;
		 _ptrParam->createAttrVec.append( cattr );
		 _ptrParam->addCylinderColumns( cattr );
		 coloffset += ncol*JAG_GEOM_TOTLEN + cattr.metrics*JAG_METRIC_LEN; 
    } else if ( cattr.type == JAG_C_COL_TYPE_LINE ) {
		 ncol=JAG_LINE_DIM;
		 cattr.begincol = _ptrParam->createAttrVec.size() +1;
		 cattr.endcol = _ptrParam->createAttrVec.size() +ncol + cattr.metrics;
		 *(cattr.spare+2) = JAG_ASC;
		 _ptrParam->createAttrVec.append( cattr );
		 _ptrParam->addLineColumns( cattr, 0 );
		 coloffset += ncol*JAG_GEOM_TOTLEN + cattr.metrics*JAG_METRIC_LEN; 
    } else if ( cattr.type == JAG_C_COL_TYPE_LINE3D ) {
		 cattr.begincol = _ptrParam->createAttrVec.size() +1;
		 cattr.endcol = _ptrParam->createAttrVec.size() +JAG_LINE3D_DIM + cattr.metrics;
		 *(cattr.spare+2) = JAG_ASC;
		 _ptrParam->createAttrVec.append( cattr );
		 _ptrParam->addLineColumns( cattr, 1 );
		 coloffset += JAG_LINE3D_DIM*JAG_GEOM_TOTLEN + cattr.metrics*JAG_METRIC_LEN;  // 6 columns x1 y1 z1 x2 y2 z2
    } else if ( cattr.type == JAG_C_COL_TYPE_LINESTRING ) {
		 ncol=JAG_LINESTRING_DIM;
		 cattr.begincol = _ptrParam->createAttrVec.size() +1;
		 cattr.endcol = _ptrParam->createAttrVec.size() +ncol + cattr.metrics;
		 *(cattr.spare+2) = JAG_ASC;
		 _ptrParam->createAttrVec.append( cattr );
		 _ptrParam->addLineStringColumns( cattr, 0 );
		 coloffset += ncol*JAG_GEOM_TOTLEN + cattr.metrics*JAG_METRIC_LEN;  // 3 columns seg x y 
    } else if ( cattr.type == JAG_C_COL_TYPE_LINESTRING3D ) {
		 ncol=JAG_LINESTRING3D_DIM;
		 cattr.begincol = _ptrParam->createAttrVec.size() +1;
		 cattr.endcol = _ptrParam->createAttrVec.size() +ncol + cattr.metrics;
		 *(cattr.spare+2) = JAG_ASC;
		 _ptrParam->createAttrVec.append( cattr );
		 _ptrParam->addLineStringColumns( cattr, 1 );
		 coloffset += ncol*JAG_GEOM_TOTLEN + cattr.metrics*JAG_METRIC_LEN;  // 3 columns seg x y 
    } else if ( cattr.type == JAG_C_COL_TYPE_MULTIPOINT ) {
		 ncol=JAG_MULTIPOINT_DIM;
		 cattr.begincol = _ptrParam->createAttrVec.size() +1;
		 cattr.endcol = _ptrParam->createAttrVec.size() +ncol + cattr.metrics;
		 *(cattr.spare+2) = JAG_ASC;
		 _ptrParam->createAttrVec.append( cattr );
		 _ptrParam->addLineStringColumns( cattr, 0 );
		 coloffset += ncol*JAG_GEOM_TOTLEN + cattr.metrics*JAG_METRIC_LEN;  // 3 columns seg x y 
    } else if ( cattr.type == JAG_C_COL_TYPE_MULTIPOINT3D ) {
		 ncol=JAG_MULTIPOINT3D_DIM;
		 cattr.begincol = _ptrParam->createAttrVec.size() +1;
		 cattr.endcol = _ptrParam->createAttrVec.size() +ncol + cattr.metrics;
		 *(cattr.spare+2) = JAG_ASC;
		 _ptrParam->createAttrVec.append( cattr );
		 _ptrParam->addLineStringColumns( cattr, 1 );
		 coloffset += ncol*JAG_GEOM_TOTLEN + cattr.metrics*JAG_METRIC_LEN;  // 3 columns seg x y 
    } else if ( cattr.type == JAG_C_COL_TYPE_POLYGON ) {
		 ncol=JAG_POLYGON_DIM;
		 cattr.begincol = _ptrParam->createAttrVec.size() +1;
		 cattr.endcol = _ptrParam->createAttrVec.size() +ncol + cattr.metrics;
		 *(cattr.spare+2) = JAG_ASC;
		 _ptrParam->createAttrVec.append( cattr );
		 _ptrParam->addPolygonColumns( cattr, 0 );
		 coloffset += ncol*JAG_GEOM_TOTLEN + cattr.metrics*JAG_METRIC_LEN;  // 3 columns seg x y 
    } else if ( cattr.type == JAG_C_COL_TYPE_POLYGON3D ) {
		 ncol=JAG_POLYGON3D_DIM;
		 cattr.begincol = _ptrParam->createAttrVec.size() +1;
		 cattr.endcol = _ptrParam->createAttrVec.size() +ncol + cattr.metrics;
		 *(cattr.spare+2) = JAG_ASC;
		 _ptrParam->createAttrVec.append( cattr );
		 _ptrParam->addPolygonColumns( cattr, 1 );
		 coloffset += ncol*JAG_GEOM_TOTLEN + cattr.metrics*JAG_METRIC_LEN;  // 3 columns seg x y 
    } else if ( cattr.type == JAG_C_COL_TYPE_MULTILINESTRING ) {
		 ncol=JAG_MULTILINESTRING_DIM;
		 cattr.begincol = _ptrParam->createAttrVec.size() +1;
		 cattr.endcol = _ptrParam->createAttrVec.size() +ncol + cattr.metrics;
		 *(cattr.spare+2) = JAG_ASC;
		 _ptrParam->createAttrVec.append( cattr );
		 _ptrParam->addPolygonColumns( cattr, 0 );
		 coloffset += ncol*JAG_GEOM_TOTLEN + cattr.metrics*JAG_METRIC_LEN;  // 3 columns seg x y 
    } else if ( cattr.type == JAG_C_COL_TYPE_MULTILINESTRING3D ) {
		 ncol=JAG_MULTILINESTRING3D_DIM;
		 cattr.begincol = _ptrParam->createAttrVec.size() +1;
		 cattr.endcol = _ptrParam->createAttrVec.size() +ncol + cattr.metrics;
		 *(cattr.spare+2) = JAG_ASC;
		 _ptrParam->createAttrVec.append( cattr );
		 _ptrParam->addPolygonColumns( cattr, 1 );
		 coloffset += ncol*JAG_GEOM_TOTLEN + cattr.metrics*JAG_METRIC_LEN;  // 3 columns seg x y 
    } else if ( cattr.type == JAG_C_COL_TYPE_MULTIPOLYGON ) {
		 ncol=JAG_MULTIPOLYGON_DIM;
		 cattr.begincol = _ptrParam->createAttrVec.size() +1;
		 cattr.endcol = _ptrParam->createAttrVec.size() +ncol + cattr.metrics;
		 *(cattr.spare+2) = JAG_ASC;
		 _ptrParam->createAttrVec.append( cattr );
		 _ptrParam->addPolygonColumns( cattr, 0 );
		 coloffset += ncol*JAG_GEOM_TOTLEN + cattr.metrics*JAG_METRIC_LEN;  // 3 columns seg x y 
    } else if ( cattr.type == JAG_C_COL_TYPE_MULTIPOLYGON3D ) {
		 ncol=JAG_MULTIPOLYGON3D_DIM;
		 cattr.begincol = _ptrParam->createAttrVec.size() +1;
		 cattr.endcol = _ptrParam->createAttrVec.size() +ncol + cattr.metrics;
		 *(cattr.spare+2) = JAG_ASC;
		 _ptrParam->createAttrVec.append( cattr );
		 _ptrParam->addPolygonColumns( cattr, 1 );
		 coloffset += ncol*JAG_GEOM_TOTLEN + cattr.metrics*JAG_METRIC_LEN;  // 3 columns seg x y 
    } else if ( cattr.type == JAG_C_COL_TYPE_TRIANGLE ) {
		 cattr.begincol = _ptrParam->createAttrVec.size() +1;
		 cattr.endcol = _ptrParam->createAttrVec.size() +6 + cattr.metrics;
		 *(cattr.spare+2) = JAG_ASC;
		 _ptrParam->createAttrVec.append( cattr );
		 _ptrParam->addTriangleColumns( cattr, 0 );
		 coloffset += 6*JAG_GEOM_TOTLEN + cattr.metrics*JAG_METRIC_LEN;  // 6 columns x1 y1 x2 y2 x3 y3
    } else if ( cattr.type == JAG_C_COL_TYPE_TRIANGLE3D ) {
		 cattr.begincol = _ptrParam->createAttrVec.size() +1;
		 cattr.endcol = _ptrParam->createAttrVec.size() +JAG_TRIANGLE3D_DIM + cattr.metrics;
		 *(cattr.spare+2) = JAG_ASC;
		 _ptrParam->createAttrVec.append( cattr );
		 _ptrParam->addTriangleColumns( cattr, 1 );
		 coloffset += JAG_TRIANGLE3D_DIM*JAG_GEOM_TOTLEN + cattr.metrics*JAG_METRIC_LEN;  // 9 columns x1 y1 z1 x2 y2 z2 x3 y3 z3
    } else if ( cattr.type == JAG_C_COL_TYPE_RANGE ) {
		 cattr.begincol = _ptrParam->createAttrVec.size() +1;
		 cattr.endcol = _ptrParam->createAttrVec.size() + JAG_RANGE_DIM;
		 *(cattr.spare+2) = JAG_ASC;
		 _ptrParam->createAttrVec.append( cattr );
		 int colLen = getEachRangeFieldLength( cattr.srid );
		 _ptrParam->addRangeColumns(colLen, cattr );
		 coloffset += JAG_RANGE_DIM* colLen;
	} else {
		_ptrParam->createAttrVec.append( cattr );
		// prt(("s7273  _ptrParam=%0x createAttrVec=%0x\n", _ptrParam, _ptrParam->createAttrVec ));
		//prt(("s7273  _ptrParam=%0x \n", _ptrParam ));
		//prt(("s7273 isValue=%d cattr.length=%d\n", isValue, cattr.length ));
		prt(("s7273 colName=[%s] cattr.defValues=[%s]\n", cattr.objName.colName.c_str(), cattr.defValues.c_str() ));
		if ( !isValue ) {
			_ptrParam->keyLength += cattr.length;
		} else {
			_ptrParam->valueLength += cattr.length;
		}
	}

}

void JagParser::addExtraOtherCols( const JagColumn *pcol, OtherAttribute &other, int &numCols )
{
	if ( pcol->type.size() < 1 ) return;

	Jstr on;

	if ( pcol->type == JAG_C_COL_TYPE_POINT || pcol->type == JAG_C_COL_TYPE_POINT3D  ) {
		on = other.objName.colName;
		other.objName.colName = on + ":x";
		_ptrParam->otherVec.append(other);
		++numCols;
		other.objName.colName = on + ":y";
		_ptrParam->otherVec.append(other);
		++numCols;
		if ( pcol->type == JAG_C_COL_TYPE_POINT3D ) {
			other.objName.colName = on + ":z";
			_ptrParam->otherVec.append(other);
			++numCols;
		}
	} else if ( pcol->type == JAG_C_COL_TYPE_SPHERE || pcol->type == JAG_C_COL_TYPE_CIRCLE ) {
		on = other.objName.colName;
		other.objName.colName = on + ":x";
		_ptrParam->otherVec.append(other);
		++numCols;

		other.objName.colName = on + ":y";
		_ptrParam->otherVec.append(other);
		++numCols;

		if ( pcol->type == JAG_C_COL_TYPE_SPHERE ) {
			other.objName.colName = on + ":z";
			_ptrParam->otherVec.append(other);
			++numCols;
		}

		other.objName.colName = on + ":a";
		_ptrParam->otherVec.append(other);
		++numCols;

	} else if ( pcol->type == JAG_C_COL_TYPE_SQUARE3D
				|| pcol->type == JAG_C_COL_TYPE_CIRCLE3D ) {
		on = other.objName.colName;
		other.objName.colName = on + ":x";
		_ptrParam->otherVec.append(other);
		++numCols;

		other.objName.colName = on + ":y";
		_ptrParam->otherVec.append(other);
		++numCols;

		other.objName.colName = on + ":z";
		_ptrParam->otherVec.append(other);
		++numCols;

		other.objName.colName = on + ":a";
		_ptrParam->otherVec.append(other);
		++numCols;

		other.objName.colName = on + ":nx";
		_ptrParam->otherVec.append(other);
		++numCols;
		other.objName.colName = on + ":ny";
		_ptrParam->otherVec.append(other);
		++numCols;
	} else if ( pcol->type == JAG_C_COL_TYPE_CUBE ||  pcol->type == JAG_C_COL_TYPE_SQUARE ) {
		on = other.objName.colName;
		other.objName.colName = on + ":x";
		_ptrParam->otherVec.append(other);
		++numCols;

		other.objName.colName = on + ":y";
		_ptrParam->otherVec.append(other);
		++numCols;

		if ( pcol->type == JAG_C_COL_TYPE_CUBE ) {
			other.objName.colName = on + ":z";
			_ptrParam->otherVec.append(other);
			++numCols;
		}

		other.objName.colName = on + ":a";
		_ptrParam->otherVec.append(other);
		++numCols;

		other.objName.colName = on + ":nx";
		_ptrParam->otherVec.append(other);
		++numCols;

		if ( pcol->type == JAG_C_COL_TYPE_CUBE ) {
			other.objName.colName = on + ":ny";
			_ptrParam->otherVec.append(other);
			++numCols;
		}

	} else if ( pcol->type == JAG_C_COL_TYPE_RECTANGLE
				|| pcol->type == JAG_C_COL_TYPE_ELLIPSE ) {
		on = other.objName.colName;
		other.objName.colName = on + ":x";
		_ptrParam->otherVec.append(other);
		++numCols;

		other.objName.colName = on + ":y";
		_ptrParam->otherVec.append(other);
		++numCols;

		other.objName.colName = on + ":a";
		_ptrParam->otherVec.append(other);
		++numCols;

		other.objName.colName = on + ":b";
		_ptrParam->otherVec.append(other);
		++numCols;

		other.objName.colName = on + ":nx";
		_ptrParam->otherVec.append(other);
		++numCols;

	} else if ( pcol->type == JAG_C_COL_TYPE_BOX || pcol->type == JAG_C_COL_TYPE_ELLIPSOID ) {
		on = other.objName.colName;
		other.objName.colName = on + ":x";
		_ptrParam->otherVec.append(other);
		++numCols;

		other.objName.colName = on + ":y";
		_ptrParam->otherVec.append(other);
		++numCols;

		other.objName.colName = on + ":z";
		_ptrParam->otherVec.append(other);
		++numCols;

		other.objName.colName = on + ":a";
		_ptrParam->otherVec.append(other);
		++numCols;

		other.objName.colName = on + ":b";
		_ptrParam->otherVec.append(other);
		++numCols;

		other.objName.colName = on + ":c";
		_ptrParam->otherVec.append(other);
		++numCols;

		other.objName.colName = on + ":nx";
		_ptrParam->otherVec.append(other);
		++numCols;

		other.objName.colName = on + ":ny";
		_ptrParam->otherVec.append(other);
		++numCols;

	} else if ( pcol->type == JAG_C_COL_TYPE_CYLINDER || pcol->type == JAG_C_COL_TYPE_CONE  ) {
		on = other.objName.colName;
		other.objName.colName = on + ":x";
		_ptrParam->otherVec.append(other);
		++numCols;

		other.objName.colName = on + ":y";
		_ptrParam->otherVec.append(other);
		++numCols;

		other.objName.colName = on + ":z";
		_ptrParam->otherVec.append(other);
		++numCols;

		other.objName.colName = on + ":a";
		_ptrParam->otherVec.append(other);
		++numCols;

		other.objName.colName = on + ":c";
		_ptrParam->otherVec.append(other);
		++numCols;

		other.objName.colName = on + ":nx";
		_ptrParam->otherVec.append(other);
		++numCols;
		other.objName.colName = on + ":ny";
		_ptrParam->otherVec.append(other);
		++numCols;

	} else if ( pcol->type == JAG_C_COL_TYPE_LINE3D || pcol->type == JAG_C_COL_TYPE_LINE ) {
		on = other.objName.colName;
		other.objName.colName = on + ":x1";
		_ptrParam->otherVec.append(other);
		++numCols;

		other.objName.colName = on + ":y1";
		_ptrParam->otherVec.append(other);
		++numCols;

		if ( pcol->type == JAG_C_COL_TYPE_LINE3D ) {
			other.objName.colName = on + ":z1";
			_ptrParam->otherVec.append(other);
			++numCols;
		}

		other.objName.colName = on + ":x2";
		_ptrParam->otherVec.append(other);
		++numCols;

		other.objName.colName = on + ":y2";
		_ptrParam->otherVec.append(other);
		++numCols;

		if ( pcol->type == JAG_C_COL_TYPE_LINE3D ) {
			other.objName.colName = on + ":z2";
			_ptrParam->otherVec.append(other);
			++numCols;
		}
	} else if ( pcol->type == JAG_C_COL_TYPE_LINESTRING3D || pcol->type == JAG_C_COL_TYPE_LINESTRING ) {
		on = other.objName.colName;

		other.objName.colName = on + ":x";
		_ptrParam->otherVec.append(other);
		++numCols;

		other.objName.colName = on + ":y";
		_ptrParam->otherVec.append(other);
		++numCols;

		if ( pcol->type == JAG_C_COL_TYPE_LINESTRING3D ) {
			other.objName.colName = on + ":z";
			_ptrParam->otherVec.append(other);
			++numCols;
		}
	} else if ( pcol->type == JAG_C_COL_TYPE_POLYGON3D || pcol->type == JAG_C_COL_TYPE_POLYGON
			    || pcol->type == JAG_C_COL_TYPE_MULTIPOINT || pcol->type == JAG_C_COL_TYPE_MULTIPOINT3D
			    || pcol->type == JAG_C_COL_TYPE_MULTILINESTRING || pcol->type == JAG_C_COL_TYPE_MULTILINESTRING3D
				|| pcol->type == JAG_C_COL_TYPE_MULTIPOLYGON || pcol->type == JAG_C_COL_TYPE_MULTIPOLYGON3D
	            ) {
		on = other.objName.colName;

		other.objName.colName = on + ":x";
		_ptrParam->otherVec.append(other);
		++numCols;

		other.objName.colName = on + ":y";
		_ptrParam->otherVec.append(other);
		++numCols;

		if ( pcol->type == JAG_C_COL_TYPE_POLYGON3D
		     || pcol->type == JAG_C_COL_TYPE_MULTILINESTRING3D
		     || pcol->type == JAG_C_COL_TYPE_MULTIPOINT3D
			 || pcol->type == JAG_C_COL_TYPE_MULTIPOLYGON3D ) {
			other.objName.colName = on + ":z";
			_ptrParam->otherVec.append(other);
			++numCols;
		}
	} else if ( pcol->type == JAG_C_COL_TYPE_TRIANGLE3D || pcol->type == JAG_C_COL_TYPE_TRIANGLE ) {
		on = other.objName.colName;
		other.objName.colName = on + ":x1";
		_ptrParam->otherVec.append(other);
		++numCols;

		other.objName.colName = on + ":y1";
		_ptrParam->otherVec.append(other);
		++numCols;

		if ( pcol->type == JAG_C_COL_TYPE_TRIANGLE3D ) {
			other.objName.colName = on + ":z1";
			_ptrParam->otherVec.append(other);
			++numCols;
		}

		other.objName.colName = on + ":x2";
		_ptrParam->otherVec.append(other);
		++numCols;

		other.objName.colName = on + ":y2";
		_ptrParam->otherVec.append(other);
		++numCols;

		if ( pcol->type == JAG_C_COL_TYPE_TRIANGLE3D ) {
			other.objName.colName = on + ":z2";
			_ptrParam->otherVec.append(other);
			++numCols;
		}

		other.objName.colName = on + ":x3";
		_ptrParam->otherVec.append(other);
		++numCols;

		other.objName.colName = on + ":y3";
		_ptrParam->otherVec.append(other);
		++numCols;

		if ( pcol->type == JAG_C_COL_TYPE_TRIANGLE3D ) {
			other.objName.colName = on + ":z3";
			_ptrParam->otherVec.append(other);
			++numCols;
		}
	} else if ( pcol->type == JAG_C_COL_TYPE_RECTANGLE3D ) {
		on = other.objName.colName;

		other.objName.colName = on + ":x";
		_ptrParam->otherVec.append(other);
		++numCols;

		other.objName.colName = on + ":y";
		_ptrParam->otherVec.append(other);
		++numCols;

		other.objName.colName = on + ":z";
		_ptrParam->otherVec.append(other);
		++numCols;

		other.objName.colName = on + ":a";
		_ptrParam->otherVec.append(other);
		++numCols;

		other.objName.colName = on + ":b";
		_ptrParam->otherVec.append(other);
		++numCols;

		other.objName.colName = on + ":nx";
		_ptrParam->otherVec.append(other);
		++numCols;

		other.objName.colName = on + ":ny";
		_ptrParam->otherVec.append(other);
		++numCols;
	}

}

void JagParser::setToRealType( const Jstr &rcs, CreateAttribute &cattr )
{
	if ( rcs == JAG_C_COL_TEMPTYPE_TEXT  ) cattr.type = JAG_C_COL_TYPE_STR;
	else if ( rcs == JAG_C_COL_TEMPTYPE_LONGTEXT  ) cattr.type = JAG_C_COL_TYPE_STR;
	else if ( rcs == JAG_C_COL_TEMPTYPE_MEDIUMTEXT  ) cattr.type = JAG_C_COL_TYPE_STR;
	else if ( rcs == JAG_C_COL_TEMPTYPE_TINYTEXT  ) cattr.type = JAG_C_COL_TYPE_STR;
	else if ( rcs == JAG_C_COL_TEMPTYPE_REAL  ) cattr.type = JAG_C_COL_TYPE_DOUBLE;
	else if ( rcs == JAG_C_COL_TEMPTYPE_STRING  ) cattr.type = JAG_C_COL_TYPE_STR;
	else if ( rcs == JAG_C_COL_TYPE_UUID  ) {
		cattr.type = JAG_C_COL_TYPE_STR;
		*(cattr.spare+1) = JAG_C_COL_TYPE_UUID[0];
	} else if ( rcs == JAG_C_COL_TYPE_FILE )  {
		cattr.type = JAG_C_COL_TYPE_STR;
		*(cattr.spare+1) = JAG_C_COL_TYPE_FILE[0];
	} else {
		cattr.type = rcs;
	}
}

int JagParser::getTypeNameArg( const char *gettok, Jstr &tname, Jstr &targ, int &collen, int &sig, int &metrics )
{
	prt(("s5503 getTypeNameArg gettok=[%s]\n", gettok ));
	metrics = 0;
	char save;
	targ = "";
	char *p1, *p2;
	p1 = (char*)strchr( gettok, '(' );
	//if ( ! p1 ) { tname=gettok; return 0; }
	if ( ! p1 ) { 
		//prt(("s3728 gettok=[%s]\n", gettok ));
		p1 = (char*)strchr( gettok, ' ' );
		if ( ! p1 ) { tname=gettok; return 0; }
	} 
	//prt(("s9370 p1=[%s]\n", p1 ));
	while ( isspace(*p1) ) ++p1;
	if ( 0 == strncasecmp( p1, "precision", 9 ) ) {
		JagStrSplit sp(gettok, ' ', true );
		tname  = sp[0];
		return 0;
	}

	save = *p1;
	*p1 = '\0';
	tname = gettok;
	//*p1 = '(';
	*p1 = save;
	prt(("s54031 tname=[%s]\n", tname.s() ));

	++p1;
	p2 = strchr( p1, ')' );
	if ( ! p2 ) {
		p2 = p1; while (*p2) ++p2;
	} 
	targ = Jstr( p1, p2-p1, p2-p1);
	prt(("s10938 targ=[%s]\n", targ.s() ));
	if ( strchr(targ.c_str(), ',' ) && ! strstr(targ.c_str(), "srid") && ! strstr(targ.c_str(), "metrics") ) {
		// "srid:4326, metrics:5"
		// 13,4
		JagStrSplit sp( targ, ',', true );
		// prt(("s5123 sp.length=%d\n", sp.length() ));
		if ( sp.length() >=1 ) {
			collen = jagatoi( sp[0].c_str() );
		} 
		if ( sp.length() >=2 ) {
			sig = jagatoi( sp[1].c_str() );
		}
	} else if ( strchr(targ.c_str(), '.' ) ) {
		JagStrSplit sp( targ, '.', true );
		// prt(("s5124 sp.length=%d\n", sp.length() ));
		if ( sp.length() >=1 ) {
			collen = jagatoi( sp[0].c_str() );
		} 
		if ( sp.length() >=2 ) {
			sig = jagatoi( sp[1].c_str() );
		}
	} else {
		if ( 0==strcasecmp(targ.c_str(), "wgs84" ) ) {
			collen = JAG_GEO_WGS84;
		} else if ( 0==strcasecmp(targ.c_str(), "time" ) ) {
			collen = JAG_RANGE_TIME;
		} else if ( 0==strcasecmp(targ.c_str(), "date" ) ) {
			collen = JAG_RANGE_DATE;
		} else if ( 0==strcasecmp(targ.c_str(), "datetimesec" ) ) {
			collen = JAG_RANGE_DATETIMESEC;
		} else if ( 0==strcasecmp(targ.c_str(), "datetimenano" ) ) {
			collen = JAG_RANGE_DATETIMENANO;
		} else if ( 0==strcasecmp(targ.c_str(), "datetimemill" ) ) {
			collen = JAG_RANGE_DATETIMEMILL;
		} else if ( 0==strcasecmp(targ.c_str(), "datetime" ) ) {
			collen = JAG_RANGE_DATETIME;
		} else if ( 0==strcasecmp(targ.c_str(), "bigint" ) ) {
			collen = JAG_RANGE_BIGINT;
		} else if ( 0==strcasecmp(targ.c_str(), "int" ) ) {
			collen = JAG_RANGE_INT;
		} else if ( 0==strcasecmp(targ.c_str(), "smallint" ) ) {
			collen = JAG_RANGE_SMALLINT;
		} else if ( 0==strcasecmp(targ.c_str(), "longdouble" ) ) {
			collen = JAG_RANGE_LONGDOUBLE;
		} else if ( 0==strcasecmp(targ.c_str(), "double" ) ) {
			collen = JAG_RANGE_DOUBLE;
		} else if ( 0==strcasecmp(targ.c_str(), "float" ) ) {
			collen = JAG_RANGE_FLOAT;
		} else {
			if ( 0==strncasecmp(targ.c_str(), "srid:", 5 ) ) {
				prt(("s53310 see srid:\n"));
				JagStrSplit sp(targ, ':');
				if ( 0==strcasecmp(sp[1].c_str(), "wgs84" ) ) {
					collen = JAG_GEO_WGS84;
				} else {
					collen = jagatoi( sp[1].c_str() );
				}
				prt(("s2838 collen=%d\n", collen ));

				const char *pm = strstr( targ.c_str(), "metrics:" );
				if ( pm ) {
					JagStrSplit sp2(pm, ':');
					metrics = jagatoi( sp2[1].c_str() );
				}
				prt(("s2838 pm=[%s] metrics=%d\n", pm, metrics ));
			} else if ( 0==strncasecmp(targ.c_str(), "metrics:", 8 ) ) {
				JagStrSplit sp(targ, ':');
				metrics = jagatoi( sp[1].c_str() );
			} else {
				collen = jagatoi( targ.c_str() );
			}
		}
	}

	//prt(("s6040 collen=%d sig=%d\n", collen, sig ));
	return 0;
}

bool JagParser::hasPolyGeoType( const char *createSQL, int &dim )
{
	dim = 0;
	const char *p = strchr(createSQL, '(');
	if ( ! p ) return false;
	++p; if ( *p == '\0' ) return false;

	JagStrSplitWithQuote sp(p, ',');
	//JagStrSplit sp(p, ',');
	if ( sp.length() < 1 ) return false;
	for ( int i=0; i < sp.length(); ++i ) {
		if ( sp[i].size() < 1 ) continue;
		// sp[i]: "key: a int" "value: b int"   "c linestring(43)" "e enum('a', 'b', 'c')"  "f point3d"
		p = strchr( sp[i].c_str(), ':' );
		if ( p ) { ++p; }
		else p = sp[i].c_str();
		//prt(("s5041 i=%d p=[%s]\n", i, p ));
		JagStrSplit nt(p, ' ', true);
		if ( nt.length() < 2 ) continue;
		//prt(("s5043 i=%d p=[%s]\n", i, p ));

		if ( 0==strncasecmp(nt[1].c_str(), "linestring3d", 12) ) {
			if ( dim < 3 ) dim = 3;
		} else if ( 0==strncasecmp(nt[1].c_str(), "multipoint3d", 12) ) {
			if ( dim < 3 ) dim = 3;
		} else if ( 0==strncasecmp(nt[1].c_str(), "polygon3d", 9) ) {
			if ( dim < 3 ) dim = 3;
		} else if ( 0==strncasecmp(nt[1].c_str(), "multipolygon3d", 14) ) {
			if ( dim < 3 ) dim = 3;
		} else if ( 0==strncasecmp(nt[1].c_str(), "multilinestring3d", 17) ) {
			if ( dim < 3 ) dim = 3;
		} else if ( 0==strncasecmp(nt[1].c_str(), "linestring", 10) ) {
			if ( dim < 2 ) dim = 2;
		} else if ( 0==strncasecmp(nt[1].c_str(), "multipoint", 10) ) {
			if ( dim < 2 ) dim = 2;
		} else if ( 0==strncasecmp(nt[1].c_str(), "polygon", 7) ) {
			if ( dim < 2 ) dim = 2;
		} else if ( 0==strncasecmp(nt[1].c_str(), "multipolygon", 12) ) {
			if ( dim < 2 ) dim = 2;
		} else if ( 0==strncasecmp(nt[1].c_str(), "multilinestring", 15) ) {
			if ( dim < 2 ) dim = 2;
		}
	}

	if ( dim >=2 ) return true;
	return false;
}

void JagParser::addBBoxGeomKeyColumns( CreateAttribute &cattr, int polyDim, bool lead, int &offset )
{
	int isMute;
	if ( lead ) {
		isMute = 0;  // bbx columns will be real-keys
	} else {
		isMute = 1;  // all bbox cols are mute
	}
	//prt(("s2239 addBBoxGeomKeyColumns polyDim=%d lead=%d isMute=%d\n", polyDim, lead, isMute ));

	offset = _ptrParam->keyLength;

	cattr.objName.colName = "geo:xmin";
	_ptrParam->fillDoubleSubData( cattr, offset, 1, isMute, 0, 0 );
	// cattr was initialized, but offset is increated

	cattr.objName.colName = "geo:ymin";
	_ptrParam->fillDoubleSubData( cattr, offset, 1, isMute, 0, 0 );

	// set boundbox all to xmin:ymin:zmin:xmax:ymax:zmax 
	// in 2D zmin=zmax=0
	if ( 1 || polyDim >=3 ) {
		cattr.objName.colName = "geo:zmin";
		_ptrParam->fillDoubleSubData( cattr, offset, 1, isMute, 0, 0 );
	}

	cattr.objName.colName = "geo:xmax";
	_ptrParam->fillDoubleSubData( cattr, offset, 1, isMute, 0, 0 );

	cattr.objName.colName = "geo:ymax";
	_ptrParam->fillDoubleSubData( cattr, offset, 1, isMute, 0, 0 );

	if ( 1 || polyDim >=3 ) {
		cattr.objName.colName = "geo:zmax";
		_ptrParam->fillDoubleSubData( cattr, offset, 1, isMute, 0, 0 );
	}

	cattr.objName.colName = "geo:id";
	_ptrParam->fillStringSubData( cattr, offset, 1, JAG_UUID_FIELD_LEN, 1, 0, 0 );

	cattr.objName.colName = "geo:col";
	_ptrParam->fillSmallIntSubData( cattr, offset, 1, 1, 0, 0 );  // must be part of key and mute

	cattr.objName.colName = "geo:m";  // m-th polygon in multipolygon  starts from 1
	_ptrParam->fillIntSubData( cattr, offset, 1, 1, 0, 0 );  // must be part of key and mute

	cattr.objName.colName = "geo:n";  // n-th ring in a polygon or n-th linestring in multilinestring
	_ptrParam->fillIntSubData( cattr, offset, 1, 1, 0, 0 );  // must be part of key and mute

	cattr.objName.colName = "geo:i";  // i-th point in a linestring
	_ptrParam->fillIntSubData( cattr, offset, 1, 1, 0, 0 );  // must be part of key and mute

	//prt(("s5034 end of addbboundbox cols _ptrParam->keyLength=%d cattr.offset=%d offset=%d\n", _ptrParam->keyLength, cattr.offset, offset ));
}


Jstr JagParser::getColumns( const char *str )
{
	//prt(("s2728 JagParser::getColumns str=[%s]\n", str ));
	Jstr s, res;
	JagStrSplitWithQuote spq(str, ' ' );
	char *p, *q;
	for ( int i=0; i < spq.length(); ++i ) {
		// intersect( ls1, linestring(1 2 , 3 4 ))
		// "&&"  "and"
		// intersect( ls2, linestring(1 2 , 3 4 ))
		//prt(("s6727 spq=[%s]\n", spq[i].c_str() ));
		p = (char*)strchr( spq[i].c_str(), '(' );
		//prt(("s67277 p=[%s]\n", p ));
		if ( ! p ) continue;
		++p;
		if ( *p == '\0' ) continue;
		//prt(("s6728 p=[%s]\n", p ));
		//JagStrSplit sp(p, ',', true );
		JagStrSplitWithQuote sp(p, ',' );
		for ( int j=0; j < sp.length() && j < 2; ++j ) {
			//prt(("s6632 sp[%d]=[%s]\n", j, sp[j].c_str() ));
			p = (char*)strchr( sp[j].c_str(), '(' );
			// if ( p ) continue;
			if ( p ) {
				p = (char*)strrchr( sp[j].c_str(), '(' );  // convexhull(ss(col))
				++p; if ( *p == 0 ) continue;
				while ( isspace(*p) ) ++p; if ( *p == 0 ) continue;
				q = strchr(p, ')');
				if ( q ) {
					--q; while ( isspace(*q) ) --q;
					if ( p == q) continue;
					s = Jstr(p, q-p+1, q-p+1);  // (p)col1(q) 
					if ( res.size() < 1 ) {
						res = s;
					} else {
						res += Jstr("|") + s;
					}
					//prt(("s2094 res=[%s]\n", res.c_str() ));
				} 
				continue;
			}
			p = (char*)strchr( sp[j].c_str(), ')' );
			if ( p ) { 
				*p = '\0'; 
				s = sp[j].c_str();
				*p = ')';
			} else {
				s = sp[j];
			}
			//prt(("s6724 sp[%d]=[%s] should be column name\n", j, sp[j].c_str() ));
			s.trimEndChar(')');
			if ( s.size() < 1 ) continue;
			if ( res.size() < 1 ) {
				res = s;
			} else {
				res += Jstr("|") + s;
			}
		}
	}

	//prt(("s2220 columns res=[%s]\n", res.c_str() ));
	if ( strchr( res.c_str(), ' ') || strchr( res.c_str(), ',') ) {
		return "";
	} else {
		return res;
	}
}

int JagParser::checkLineStringData( const char *p )
{
	if ( *p == 0 ) return -3427;
	//prt(("s2438 linestring( p=[%s]\n", p ));
	JagStrSplit sp( p, ',', true );
	int len = sp.length();
	char c;
	for ( int i = 0; i < len; ++i ) {
		if ( strchr(sp[i].c_str(), ':') ) { c = ':'; } else { c = ' '; }
		JagStrSplit ss( sp[i], c, true );
		if ( ss.length() < 2 ) {  continue; }
		if ( ss[0].length() >= JAG_POINT_LEN ) { return -4416; }
		if ( ss[1].length() >= JAG_POINT_LEN ) { return -4417; }
	}
	return 0;
}

int JagParser::getLineStringMinMax( char sep, const char *p, double &xmin, double &ymin, double &xmax, double &ymax )
{
	//prt(("s2921 getLineStringMinMax p=[%s]\n", p ));
	if ( *p == 0 ) return -4410;

	double xv, yv;
	char c;
	JagStrSplit sp( p, sep, true );
	int len = sp.length();
	for ( int i = 0; i < len; ++i ) {
		if ( strchr(sp[i].c_str(), ':') ) { c = ':'; } else { c = ' '; }
    	JagStrSplit ss( sp[i], c, true );
		//prt(("ss.length()=%d sp[%d]=[%s]\n", ss.length(), i, sp[i].c_str() ));
    	if ( ss.length() < 2 ) continue;
    	if ( ss[0].length() >= JAG_POINT_LEN ) { return -4616; }
    	if ( ss[1].length() >= JAG_POINT_LEN ) { return -4617; }
    	xv = jagatof( ss[0].c_str() );
    	yv = jagatof( ss[1].c_str() );
		//prt(("s2229 xv=%f yv=%f\n", xv, yv ));

		if ( xv > xmax ) xmax = xv;
		if ( yv > ymax ) ymax = yv;
		if ( xv < xmin ) xmin = xv;
		if ( yv < ymin ) ymin = yv;
	}
	//prt(("s1038 getLineStringMinMax() xmin=%f xmax=%f ymin=%f ymax=%f\n", xmin, xmax, ymin, ymax ));
	return 0;
}

int JagParser::addLineStringData( JagLineString &linestr, const char *p )
{
	if ( *p == 0 ) return -4410;
	//prt(("s2838 linestring( p=[%s]\n", p ));

	JagStrSplit sp( p, ',', true );
	int len = sp.length();
	JagLineString3D linestr3d;
	char c;
	for ( int i = 0; i < len; ++i ) {
		if ( strchr(sp[i].c_str(), ':') ) { c = ':'; } else { c = ' '; }
		//JagStrSplit ss( sp[i], c, true );
		JagStrSplitWithQuote ss( sp[i].s(), c, true, true );
		if ( ss.length() < 2 ) {  continue; }
		if ( ss[0].length() >= JAG_POINT_LEN ) { return -4416; }
		if ( ss[1].length() >= JAG_POINT_LEN ) { return -4417; }
		JagPoint2D p( ss[0].c_str(), ss[1].c_str() );
		if ( ! getMetrics( ss, p.metrics ) ) return -4939;
		linestr3d.add(p);
	}
	linestr = linestr3d;
	//prt(("s0394 appended linestring other.linestr.size=%d\n", linestr.size() ));
	return 0;
}

// return 0: OK,  < 0 error
int JagParser::checkLineString3DData( const char *p )
{
	//prt(("s2838 linestring( p=[%s]\n", p ));
	if ( *p == 0 ) return -4322;
	JagStrSplit sp( p, ',', true );
	int len = sp.length();
	char c;
	for ( int i = 0; i < len; ++i ) {
		if ( strchr(sp[i].c_str(), ':') ) { c = ':'; } else { c = ' '; }
		JagStrSplit ss( sp[i], c, true );
		//if ( ss.length() < 3 ) { return -3404; }
		if ( ss.length() < 3 ) continue;
		if ( ss[0].length() >= JAG_POINT_LEN ) { return -3426; }
		if ( ss[1].length() >= JAG_POINT_LEN ) { return -3427; }
		if ( ss[2].length() >= JAG_POINT_LEN ) { return -3428; }
	}
	return 0;
}

int JagParser::getLineString3DMinMax( char sep, const char *p, double &xmin, double &ymin, double &zmin, double &xmax, double &ymax, double &zmax )
{
	//prt(("s2838 linestring( p=[%s]\n", p ));
	if ( *p == 0 ) return -4422;
	double xv, yv, zv;
	//JagStrSplit sp( p, ',', true );
	JagStrSplit sp( p, sep, true );
	int len = sp.length();
	char c;
	for ( int i = 0; i < len; ++i ) {
		//prt(("s7712 sp[%d]=[%s]\n", i, sp[i].c_str() ));
		if ( strchr( sp[i].c_str(), '(' ) )  { sp[i].remove('('); }
		if ( strchr(sp[i].c_str(), ':') ) { c = ':'; } else { c = ' '; }
		JagStrSplit ss( sp[i], c, true );
		// if ( ss.length() < 3 ) { return -4404; }
		if ( ss.length() < 3 ) continue;
		if ( ss[0].length() >= JAG_POINT_LEN ) { return -4426; }
		if ( ss[1].length() >= JAG_POINT_LEN ) { return -4427; }
		if ( ss[2].length() >= JAG_POINT_LEN ) { return -4428; }
		xv = jagatof( ss[0].c_str() );
		yv = jagatof( ss[1].c_str() );
		zv = jagatof( ss[2].c_str() );
		if ( xv > xmax ) xmax = xv;
		if ( yv > ymax ) ymax = yv;
		if ( zv > zmax ) zmax = zv;
		if ( xv < xmin ) xmin = xv;
		if ( yv < ymin ) ymin = yv;
		if ( zv < zmin ) zmin = zv;

	}
	return 0;
}

int JagParser::addLineString3DData( JagLineString &linestr, const char *p )
{
	prt(("s2838 linestring( p=[%s]\n", p ));
	if ( *p == 0 ) return -4422;
	JagStrSplit sp( p, ',', true );
	int len = sp.length();
	JagLineString3D linestr3d;
	char c;
	for ( int i = 0; i < len; ++i ) {
		//prt(("s7712 sp[%d]=[%s]\n", i, sp[i].c_str() ));
		if ( strchr( sp[i].c_str(), '(' ) )  { sp[i].remove('('); }
		if ( strchr( sp[i].c_str(), ':') ) { c = ':'; } else { c = ' '; }
		//JagStrSplit ss( sp[i], c, true );
		JagStrSplitWithQuote ss( sp[i].s(), c, true, true );
		// if ( ss.length() < 3 ) { return -4404; }
		if ( ss.length() < 3 ) continue;
		if ( ss[0].length() >= JAG_POINT_LEN ) { return -4436; }
		if ( ss[1].length() >= JAG_POINT_LEN ) { return -4437; }
		if ( ss[2].length() >= JAG_POINT_LEN ) { return -4438; }
		JagPoint3D p( ss[0].c_str(), ss[1].c_str(), ss[2].c_str() );
		if ( ! getMetrics( ss, p.metrics ) ) return -5010;
		linestr3d.add(p);
	}
	linestr = linestr3d;
	//prt(("s0395 appended linestring other.linestr.size=%d\n", linestr.size() ));
	return 0;
}


int JagParser::checkPolygonData( const char *p, bool mustClose )
{
	//prt(("s3438 polygon( p=[%s]\n", p ));
	int len;
	const char *q;
	double x1, y1, xn, yn;
	if ( *p == 0 ) return -4521;
	// (p  (...), (...) )

	while ( *p != '\0' ) {
		while ( *p != '(' && *p != '\0' ) ++p; 
		if ( *p == '\0' ) break;
		++p;
		if ( *p == '\0' ) break;
		while ( isspace(*p) ) ++p;
		while ( *p == '(' ) ++p;
		// ( (p ...), ( ... )
		q = p;
		while ( *q != ')' && *q != '\0' ) ++q; 
		if ( *q == '\0' ) break;
		//  // (  (p...q), (...) )
		if ( q == p ) return -4518;
		while ( *p == '(' ) ++p;
		Jstr polygon(p, q-p);
		//prt(("s7062 one polygon=[%s]\n", polygon.c_str() ));
		JagStrSplit sp( polygon, ',', true );
		len = sp.length();
		for ( int i = 0; i < len; ++i ) {
			JagStrSplit ss( sp[i], ' ', true );
			//if ( ss.length() < 2 ) { return -4523; }
			if ( ss.length() < 2 ) continue;
			if ( ss[0].length() >= JAG_POINT_LEN ) { return -4526; }
			if ( ss[1].length() >= JAG_POINT_LEN ) { return -4527; }

			if ( mustClose ) {
    			if ( 0==i) {
    				x1 = jagatof(ss[0].c_str() );
    				y1 = jagatof(ss[1].c_str() );
    			}
    
    			if ( len-1==i) {
    				xn = jagatof(ss[0].c_str() );
    				yn = jagatof(ss[1].c_str() );
    			}
			}
		}

		if ( mustClose ) {
			if ( ! jagEQ(x1, xn) || ! jagEQ(y1,yn) ) {
				return -4630;
			}
		}
		
		p=q+1;
		while ( *p != ',' && *p != '\0' ) ++p; 
		if ( *p == '\0' ) break;
		++p;
		//prt(("s5410 p=[%s]\n", p ));
	}

	return 0;
}

int JagParser::checkMultiPolygonData( const char *p, bool mustClose, bool is3D )
{
	//prt(("s3588 multipolygon( p=[%s]\n", p ));
	int rc;
	const char *q;
	if ( *p == 0 ) return -4535;
	int bracketCount = 0; // count ( ( ) )

	++p; // skip first '('
	++ bracketCount;

	while ( *p != '\0' ) {
		if ( 0 == bracketCount ) break;
		while ( isspace(*p) ) ++p;
		if ( *p == '\0' ) break;
		if ( *p == '(' ) {
			++ bracketCount;
			// search ending ')'
			q = p+1;
			if ( *q == '\0' ) break;
			while ( *q != '\0' ) {
				if ( *q == '(' ) {
					++ bracketCount;
				} else if ( *q == ')' ) {
					-- bracketCount;
					if ( 1 == bracketCount ) {
						Jstr s(p+1, q-p-1); // p: "p ( ) ( ) ( ) q
						//prt(("s3701 checking polygon=[%s] ...\n", s.c_str() ));
						if ( is3D ) {
							rc = checkPolygon3DData( s.c_str(), mustClose );
						} else {
							rc = checkPolygonData( s.c_str(), mustClose );
						}
						if ( rc < 0 ) { return rc; }
						p = q;
						break;
					}
				}
				++q;
			}
		} else if ( *p == ')' ) {
			-- bracketCount;
		}
		++p;
	}

	return 0;
}

int JagParser::addMultiPolygonData( Jstr &pgvec, const char *p, 
								    bool eachFirstOnly, bool mustClose, bool is3D )
{
	prt(("s3524 multipolygon p=[%s]\n", p ));
	int rc;
	const char *q;
	if ( *p == 0 ) return 0;
	if ( 0==strcmp(p, "()") ) return 0;
	int bracketCount = 0; // count ( ( ) )

	++p; // skip first '('
	++ bracketCount;

	while ( *p != '\0' ) {
		if ( 0 == bracketCount ) break;
		while ( isspace(*p) ) ++p;
		if ( *p == '\0' ) break;
		if ( *p == '(' ) {
			++ bracketCount;
			// search ending ')'
			q = p+1;
			if ( *q == '\0' ) break;
			while ( *q != '\0' ) {
				if ( *q == '(' ) {
					++ bracketCount;
				} else if ( *q == ')' ) {
					-- bracketCount;
					if ( 1 == bracketCount ) {
						Jstr s(p+1, q-p-1); // p: "p ( ) ( ) ( ) q
						//prt(("s3701 add polygon=[%s] ...\n", s.c_str() ));
						// JagPolygon pgon;
						Jstr pgon;
						if ( is3D ) {
							rc = addPolygon3DData(pgon, s.c_str(), eachFirstOnly, mustClose );
						} else {
							rc = addPolygonData(pgon, s.c_str(), eachFirstOnly, mustClose );
						}
						if ( rc < 0 ) { return rc; }
						//pgvec.append( pgon );
						if ( pgvec.size() < 1 ) {
							pgvec = pgon;
						} else {
							pgvec += Jstr("! ") + pgon;
						}
						p = q;
						break;
					}
				}
				++q;
			}
		} else if ( *p == ')' ) {
			-- bracketCount;
		}
		++p;
	}

	return 1;
}

int JagParser::addMultiPolygonData( JagVector<JagPolygon> &pgvec, const char *p, 
								    bool eachFirstOnly, bool mustClose, bool is3D )
{
	prt(("s3524 multipolygon( p=[%s]\n", p ));
	int rc;
	const char *q;
	if ( *p == 0 ) return 0;
	if ( 0==strcmp(p, "()") ) return 0;
	int bracketCount = 0; // count ( ( ) )

	++p; // skip first '('
	++ bracketCount;

	while ( *p != '\0' ) {
		if ( 0 == bracketCount ) break;
		while ( isspace(*p) ) ++p;
		if ( *p == '\0' ) break;
		if ( *p == '(' ) {
			++ bracketCount;
			// search ending ')'
			q = p+1;
			if ( *q == '\0' ) break;
			while ( *q != '\0' ) {
				if ( *q == '(' ) {
					++ bracketCount;
				} else if ( *q == ')' ) {
					-- bracketCount;
					if ( 1 == bracketCount ) {
						Jstr s(p+1, q-p-1); // p: "p ( ) ( ) ( ) q
						//prt(("s3701 add polygon=[%s] ...\n", s.c_str() ));
						JagPolygon pgon;
						if ( is3D ) {
							rc = addPolygon3DData(pgon, s.c_str(), eachFirstOnly, mustClose );
						} else {
							rc = addPolygonData(pgon, s.c_str(), eachFirstOnly, mustClose );
						}
						if ( rc < 0 ) { return rc; }
						pgvec.append( pgon );
						p = q;
						break;
					}
				}
				++q;
			}
		} else if ( *p == ')' ) {
			-- bracketCount;
		}
		++p;
	}

	return 1;
}

int JagParser::addMultiPolygonData( JagVector<JagPolygon> &pgvec, const JagStrSplit &sp, bool firstOnly, bool is3D )
{
	prt(("s5608 addMultiPolygonData firstOnly=%d is3D=%d sp.print():\n", firstOnly, is3D ));
	//sp.print();

	const char *str;
	char *p;
	JagLineString3D linestr;
	double dx,dy,dz;
	JagPolygon pgon;
	dz = 0.0;
	// "x:y x:y x:y x:y x:y"
	// "x:y x:y ! x:y x:y x:y"
	// "x:y x:y | x:y x:y x:y ! x:y x:y x:y"
	// "x:y x:y | x:y x:y x:y ! x:y x:y x:y | x:y x:y x:y"

	bool skip = false;
	for ( int i=JAG_SP_START; i < sp.length(); ++i ) {
		str = sp[i].c_str();
		prt(("s4089 sp[i=%d]=[%s]\n", i, str ));

		JagPoint3D pt(dx,dy,dz);
		if ( sp[i] == "!" ) {
			skip = false;
			// start a new polygon
			// if ( firstOnly ) break;
			pgon.add(linestr);
			pgvec.append(pgon);
			pgon.init();
            linestr.init();
		} else if ( sp[i] == "|" ) {
			if ( firstOnly ) {
				skip = true;
			} else {
				pgon.add( linestr );
				linestr.init();
			}
		} else {
			// coordinates
			if ( ! skip) {
        		if ( is3D ) {
        			if ( strchrnum( str, ':') < 2 ) continue;
        			get3double(str, p, ':', dx, dy, dz );
        		} else {
        			if ( strchrnum( str, ':') < 1 ) continue;
        			get2double(str, p, ':', dx, dy );
        		}
    			JagPoint3D pt(dx,dy,dz);
    			linestr.add( pt );
			}
		}
	}

	pgon.add( linestr );
	pgvec.append( pgon );
	return 1;
}

int JagParser::getPolygonMinMax( const char *p,  double &xmin, double &ymin, double &xmax, double &ymax )
{
	double xv, yv;
	
	//prt(("s5838 getPolygonMinMax( p=[%s]\n", p ));
	int len;
	const char *q;
	//double x1, y1, xn, yn;
	if ( *p == 0 ) return -4519;
	// (p  (...), (...) )

	while ( *p != '\0' ) {
		while ( *p != '(' && *p != '\0' ) ++p; 
		if ( *p == '\0' ) break;
		++p;
		if ( *p == '\0' ) break;
		// ( (p ...), ( ... )
		q = p;
		while ( *q != ')' && *q != '\0' ) ++q; 
		if ( *q == '\0' ) break;
		//  // (  (p...q), (...) )
		while ( *p == '(' ) ++p;
		Jstr polygon(p, q-p);
		if ( q == p ) { return -4524; }
		//prt(("s5262 one polygon=[%s]\n", polygon.c_str() ));
		JagStrSplit sp( polygon, ',', true );
		len = sp.length();
		for ( int i = 0; i < len; ++i ) {
			JagStrSplit ss( sp[i], ' ', true );
			// if ( ss.length() < 2 ) { return -4523; }
			if ( ss.length() < 2 ) continue;
			if ( ss[0].length() >= JAG_POINT_LEN ) { return -4526; }
			if ( ss[1].length() >= JAG_POINT_LEN ) { return -4527; }
			xv = jagatof( ss[0].c_str() );
			yv = jagatof( ss[1].c_str() );
			if ( xv > xmax ) xmax = xv;
			if ( yv > ymax ) ymax = yv;
			if ( xv < xmin ) xmin = xv;
			if ( yv < ymin ) ymin = yv;
			//prt(("s7835 xv=%f yv=%f xmin=%f ymin=%f\n", xv, yv, xmin, ymin ));
		}
		break; // outer polygon only
		
		p=q+1;
		while ( *p != ',' && *p != '\0' ) ++p; 
		if ( *p == '\0' ) break;
		++p;
	}

	return 0;
}

int JagParser::addPolygonData( Jstr &pgon, const char *p, bool firstOnly, bool mustClose )
{
	//prt(("s3538 addPolygonData( p=[%s]\n", p ));
	int len;
	const char *q;
	double x1, y1, xn, yn;
	if ( *p == 0 ) return 0;
	if ( 0==strcmp(p, "()") ) return -1;
	// (p  (...), (...) )
	//while ( *p == '(' ) ++p; 

	while ( *p != '\0' ) {
		while ( *p != '(' && *p != '\0' ) ++p; 
		if ( *p == '\0' ) break;
		++p;
		if ( *p == '\0' ) break;
		// ( (p ...), ( ... )
		while ( isspace(*p) ) ++p;
		//prt(("s3093 p=[%s]\n", p ));
		q = p;
		while ( *q != ')' && *q != '\0' ) ++q; 
		if ( *q == '\0' ) break;
		//  // (  (p...q), (...) )
		while ( *p == '(' ) ++p;
		Jstr polygon(p, q-p);
		//prt(("s7252 one polygon=[%s] q=[%s] p=[%s]\n", polygon.c_str(), q, p ));
		JagStrSplit sp( polygon, ',', true );
		len = sp.length();
		//JagLineString3D linestr3d;
		Jstr linestr3d;
		for ( int i = 0; i < len; ++i ) {
			JagStrSplit ss( sp[i], ' ', true );
			//prt(("s2038 sp[i=%d]=[%s]\n", i, sp[i].c_str() ));
			// if ( ss.length() < 2 ) { return -4523; }
			if ( ss.length() < 2 ) continue;
			if ( ss[0].length() >= JAG_POINT_LEN ) { return -4526; }
			if ( ss[1].length() >= JAG_POINT_LEN ) { return -4527; }
			/**
			JagPoint2D p2d( ss[0].c_str(), ss[1].c_str() );
			linestr3d.add(p2d);
			**/
			linestr3d += ss[0] + ":" + ss[1] + " ";
			if ( mustClose ) {
    			if ( 0==i) {
    				x1 = jagatof(ss[0].c_str() );
    				y1 = jagatof(ss[1].c_str() );
    			}
    
    			if ( len-1==i) {
    				xn = jagatof(ss[0].c_str() );
    				yn = jagatof(ss[1].c_str() );
    			}
			}
		}

		if ( mustClose ) {
			if ( ! jagEQ(x1, xn) || ! jagEQ(y1,yn) ) {
				//prt(("s4530 x1=%f xn=%f   y1=%f  yn=%f\n", x1, xn, y1, yn ));
				return -4530;
			}
		}
		//pgon.add( linestr3d ); // one linestring

		if ( pgon.size() < 1 ) {
			pgon = linestr3d; // first ring
		} else {
			pgon += Jstr("| ") + linestr3d; // more rings
		}

		if ( firstOnly ) break;
		
		p=q+1;
		while ( *p != ',' && *p != '\0' ) ++p; 
		if ( *p == '\0' ) break;
		++p;
	}

	return 1;
}

int JagParser::addPolygonData( JagPolygon &pgon, const char *p, bool firstOnly, bool mustClose )
{
	//prt(("s3538 addPolygonData( p=[%s]\n", p ));
	int len;
	const char *q;
	double x1, y1, xn, yn;
	if ( *p == 0 ) return 0;
	if ( 0==strcmp(p, "()") ) return -1;
	// (p  (...), (...) )
	//while ( *p == '(' ) ++p; 

	while ( *p != '\0' ) {
		while ( *p != '(' && *p != '\0' ) ++p; 
		if ( *p == '\0' ) break;
		++p;
		if ( *p == '\0' ) break;
		// ( (p ...), ( ... )
		while ( isspace(*p) ) ++p;
		//prt(("s3093 p=[%s]\n", p ));
		q = p;
		while ( *q != ')' && *q != '\0' ) ++q; 
		if ( *q == '\0' ) break;
		//  // (  (p...q), (...) )
		while ( *p == '(' ) ++p;
		Jstr polygon(p, q-p);
		//prt(("s7252 one polygon=[%s] q=[%s] p=[%s]\n", polygon.c_str(), q, p ));
		JagStrSplit sp( polygon, ',', true );

		len = sp.length();
		JagLineString3D linestr3d;
		for ( int i = 0; i < len; ++i ) {
			//JagStrSplit ss( sp[i], ' ', true );
			JagStrSplitWithQuote ss( sp[i].s(), ' ', true, true );
			//prt(("s2038 sp[i=%d]=[%s]\n", i, sp[i].c_str() ));
			// if ( ss.length() < 2 ) { return -4523; }
			if ( ss.length() < 2 ) continue;
			if ( ss[0].length() >= JAG_POINT_LEN ) { return -4526; }
			if ( ss[1].length() >= JAG_POINT_LEN ) { return -4527; }
			JagPoint2D p2d( ss[0].c_str(), ss[1].c_str() );
			if ( ! getMetrics( ss, p2d.metrics ) ) return -4620;
			linestr3d.add(p2d);
			if ( mustClose ) {
    			if ( 0==i) {
    				x1 = jagatof(ss[0].c_str() );
    				y1 = jagatof(ss[1].c_str() );
    			}
    
    			if ( len-1==i) {
    				xn = jagatof(ss[0].c_str() );
    				yn = jagatof(ss[1].c_str() );
    			}
			}
		}

		if ( mustClose ) {
			if ( ! jagEQ(x1, xn) || ! jagEQ(y1,yn) ) {
				//prt(("s4530 x1=%f xn=%f   y1=%f  yn=%f\n", x1, xn, y1, yn ));
				return -4530;
			}
		}
		pgon.add( linestr3d ); // one linestring
		if ( firstOnly ) break;
		
		p=q+1;
		while ( *p != ',' && *p != '\0' ) ++p; 
		if ( *p == '\0' ) break;
		++p;
	}

	return 1;
}

int JagParser::checkPolygon3DData( const char *p, bool mustClose )
{
	if ( *p == 0 ) return -4519;
	//prt(("s3818 polygon3d( p=[%s]\n", p ));
	int len;
	const char *q;
	// (p  (...), (...) )

	double x1, y1, z1, xn, yn, zn;
	while ( *p != '\0' ) {
		while ( *p != '(' && *p != '\0' ) ++p; 
		if ( *p == '\0' ) break;
		++p;
		while ( isspace(*p) ) ++p;
		while ( *p == '(' ) ++p;
		if ( *p == '\0' ) break;
		while ( isspace(*p) ) ++p;
		// ( (p ...), ( ... )
		q = p;
		while ( *q != ')' && *q != '\0' ) ++q; 
		if ( *q == '\0' ) break;
		//  // (  (p...q), (...) )
		if ( q == p ) return -4623;

		while ( *p == '(' ) ++p;
		Jstr polygon(p, q-p);
		//prt(("s7362 one polygon=[%s]\n", polygon.c_str() ));
		JagStrSplit sp( polygon, ',', true );
		len = sp.length();
		for ( int i = 0; i < len; ++i ) {
			JagStrSplit ss( sp[i], ' ', true );
			// if ( ss.length() < 3 ) { return -4633; }
			if ( ss.length() < 3 ) continue;
			if ( ss[0].length() >= JAG_POINT_LEN ) { return -4636; }
			if ( ss[1].length() >= JAG_POINT_LEN ) { return -4637; }
			if ( ss[2].length() >= JAG_POINT_LEN ) { return -4638; }

			if ( mustClose ) {
    			if ( 0==i) {
    				x1 = jagatof(ss[0].c_str() );
    				y1 = jagatof(ss[1].c_str() );
    				z1 = jagatof(ss[2].c_str() );
    			}
    			if ( (len-1)==i) {
    				xn = jagatof(ss[0].c_str() );
    				yn = jagatof(ss[1].c_str() );
    				zn = jagatof(ss[2].c_str() );
    			}
			}
		}

		if ( mustClose ) {
			if ( ! jagEQ(x1, xn) || ! jagEQ(y1,yn) || ! jagEQ(z1,zn) ) {
				//prt(("E4643 x1=%.3f xn=%.3f   y1=%.3f yn=%.3f z1=%.3f zn=%.3f \n", x1, xn, y1, yn, z1, zn ));
				return -4643;
			}
		}
		
		p=q+1;
		while ( *p != ',' && *p != '\0' ) ++p; 
		if ( *p == '\0' ) break;
		++p;
		//prt(("s4621 p=[%s]\n", p ));
	}

	return 0;
}

int JagParser::getPolygon3DMinMax( const char *p , double &xmin, double &ymin, double &zmin,
							   double &xmax, double &ymax, double &zmax )
{
	if ( *p == 0 ) return -4519;

	double xv, yv, zv;

	//prt(("s3834 getPolygon3DMinMax( p=[%s]\n", p ));
	int len;
	const char *q;
	// (p  (...), (...) )

	//double x1, y1, z1, xn, yn, zn;
	while ( *p != '\0' ) {
		while ( *p != '(' && *p != '\0' ) ++p; 
		if ( *p == '\0' ) break;
		++p;
		if ( *p == '\0' ) break;
		// ( (p ...), ( ... )
		while ( isspace(*p) ) ++p;
		q = p;
		while ( *q != ')' && *q != '\0' ) ++q; 
		if ( *q == '\0' ) break;
		//  // (  (p...q), (...) )
		Jstr polygon(p, q-p);
		//prt(("s7462 one polygon=[%s]\n", polygon.c_str() ));
		JagStrSplit sp( polygon, ',', true );
		len = sp.length();
		for ( int i = 0; i < len; ++i ) {
			JagStrSplit ss( sp[i], ' ', true );
			// if ( ss.length() < 3 ) { return -4533; }
			if ( ss.length() < 3 ) continue;
			if ( ss[0].length() >= JAG_POINT_LEN ) { return -4536; }
			if ( ss[1].length() >= JAG_POINT_LEN ) { return -4537; }
			if ( ss[2].length() >= JAG_POINT_LEN ) { return -4538; }
			JagPoint3D p( ss[0].c_str(), ss[1].c_str(), ss[2].c_str() );
    		xv = jagatof( ss[0].c_str() );
    		yv = jagatof( ss[1].c_str() );
    		zv = jagatof( ss[2].c_str() );
    		if ( xv > xmax ) xmax = xv;
    		if ( yv > ymax ) ymax = yv;
    		if ( zv > zmax ) zmax = zv;
    
    		if ( xv < xmin ) xmin = xv;
    		if ( yv < ymin ) ymin = yv;
    		if ( zv < zmin ) zmin = zv;
		}
		break; // check outer polygon only

		p=q+1;
		while ( *p != ',' && *p != '\0' ) ++p; 
		if ( *p == '\0' ) break;
		++p;
	}

	return 0;
}

int JagParser::addPolygon3DData( Jstr &pgon, const char *p, bool firstOnly, bool mustClose )
{
	if ( *p == 0 ) { return 0; }
	if ( 0==strcmp(p, "()") ) return -1;

	int len;
	const char *q;
	double x1, y1, z1, xn, yn, zn;
	while ( *p != '\0' ) {
		while ( *p != '(' && *p != '\0' ) ++p; 
		if ( *p == '\0' ) break;
		++p;
		if ( *p == '\0' ) break;
		while ( isspace(*p) ) ++p;
		q = p;
		while ( *q != ')' && *q != '\0' ) ++q; 
		if ( *q == '\0' ) break;
		//  // (  (p...q), (...) )
		Jstr polygon(p, q-p);
		//prt(("s8262 one polygon=[%s]\n", polygon.c_str() ));
		JagStrSplit sp( polygon, ',', true );
		len = sp.length();
		//JagLineString3D linestr3d;
		Jstr linestr3d;
		for ( int i = 0; i < len; ++i ) {
			JagStrSplit ss( sp[i], ' ', true );
			// if ( ss.length() < 3 ) { return -4533; }
			if ( ss.length() < 3 ) continue;
			if ( ss[0].length() >= JAG_POINT_LEN ) { return -4536; }
			if ( ss[1].length() >= JAG_POINT_LEN ) { return -4537; }
			if ( ss[2].length() >= JAG_POINT_LEN ) { return -4538; }
			/***
			JagPoint3D p3d( ss[0].c_str(), ss[1].c_str(), ss[2].c_str() );
			linestr3d.add(p3d);
			***/
			linestr3d += ss[0] + ":" + ss[1] + ":" + ss[2] + " ";
			if ( mustClose ) {
    			if ( 0==i) {
    				x1 = jagatof(ss[0].c_str() );
    				y1 = jagatof(ss[1].c_str() );
    				z1 = jagatof(ss[2].c_str() );
    			}
    
    			if ( len-1==i) {
    				xn = jagatof(ss[0].c_str() );
    				yn = jagatof(ss[1].c_str() );
    				zn = jagatof(ss[2].c_str() );
    			}
			}
		}

		if ( mustClose ) {
			if ( ! jagEQ(x1, xn) || ! jagEQ(y1,yn) || ! jagEQ(z1,zn) ) {
				return -4543;
			}
		}
		// pgon.add( linestr3d ); // one polygon
		if ( pgon.size() < 1 ) {
			pgon = linestr3d;
		} else {
			pgon += Jstr(" | ") + linestr3d;
		}
		if ( firstOnly ) break;
		
		p=q+1;
		//prt(("s3246 p=[%s]\n", p ));
		while ( *p != ',' && *p != '\0' ) ++p; 
		//prt(("s3248 p=[%s]\n", p ));
		if ( *p == '\0' ) break;
		++p;
		//prt(("s3249 p=[%s]\n", p ));
	}

	return 1;
}

int JagParser::addPolygon3DData( JagPolygon &pgon, const char *p, bool firstOnly, bool mustClose )
{
	if ( *p == 0 ) {
		return 0;
	}
	if ( 0==strcmp(p, "()") ) return 0;

	//prt(("s3238 polygon( p=[%s]\n", p ));
	int len;
	const char *q;
	// (p  (...), (...) )
	// while ( *p == '(' ) ++p; 
	//prt(("s3234 p=[%s]\n", p ));

	double x1, y1, z1, xn, yn, zn;
	while ( *p != '\0' ) {
		while ( *p != '(' && *p != '\0' ) ++p; 
		if ( *p == '\0' ) break;
		//prt(("s3244 p=[%s]\n", p ));
		++p;
		//prt(("s3245 p=[%s]\n", p ));
		if ( *p == '\0' ) break;
		// ( (p ...), ( ... )
		while ( isspace(*p) ) ++p;
		q = p;
		while ( *q != ')' && *q != '\0' ) ++q; 
		if ( *q == '\0' ) break;
		//  // (  (p...q), (...) )
		Jstr polygon(p, q-p);
		//prt(("s8262 one polygon=[%s]\n", polygon.c_str() ));
		JagStrSplit sp( polygon, ',', true );
		len = sp.length();
		JagLineString3D linestr3d;
		for ( int i = 0; i < len; ++i ) {
			//JagStrSplit ss( sp[i], ' ', true );
			JagStrSplitWithQuote ss( sp[i].s(), ' ', true, true );
			if ( ss.length() < 3 ) continue;
			if ( ss[0].length() >= JAG_POINT_LEN ) { return -4536; }
			if ( ss[1].length() >= JAG_POINT_LEN ) { return -4537; }
			if ( ss[2].length() >= JAG_POINT_LEN ) { return -4538; }
			JagPoint3D p3d( ss[0].c_str(), ss[1].c_str(), ss[2].c_str() );
			if ( ! getMetrics( ss, p3d.metrics ) ) return -4801;
			linestr3d.add(p3d);
			if ( mustClose ) {
    			if ( 0==i) {
    				x1 = jagatof(ss[0].c_str() );
    				y1 = jagatof(ss[1].c_str() );
    				z1 = jagatof(ss[2].c_str() );
    			}
    
    			if ( len-1==i) {
    				xn = jagatof(ss[0].c_str() );
    				yn = jagatof(ss[1].c_str() );
    				zn = jagatof(ss[2].c_str() );
    			}
			}
		}

		if ( mustClose ) {
			if ( ! jagEQ(x1, xn) || ! jagEQ(y1,yn) || ! jagEQ(z1,zn) ) {
				return -4543;
			}
		}
		pgon.add( linestr3d ); // one polygon
		if ( firstOnly ) break;
		
		p=q+1;
		//prt(("s3246 p=[%s]\n", p ));
		while ( *p != ',' && *p != '\0' ) ++p; 
		//prt(("s3248 p=[%s]\n", p ));
		if ( *p == '\0' ) break;
		++p;
		//prt(("s3249 p=[%s]\n", p ));
	}

	return 1;
}

void JagParser::removeEndUnevenBracket( char *str )
{
	if ( NULL == str || *str == 0 ) return;
	char *p= (char*)str;
	///prt(("s1029 removeEndUnevenBracket str=[%s]\n", str ));
	int brackets = 0;
	while ( *p != 0 ) { 
		//prt(("s6128 *p=[%c] brackets=%d\n", *p, brackets ));
		if ( *p == '\'' || *p == '"' ) { *p = ' '; }
		else if ( *p == '(' ) { --brackets;}
		else if ( *p == ')' ) { ++brackets;}
		++p; 
	}
	if ( 0 == brackets ) return;
	//prt(("s6108 brackets=%d\n", brackets ));

	--p;
	while ( p != str ) {
		if ( brackets  <= 0) {
			return;
		}
		if ( *p == ')' ) { *p = 0; --brackets; }
		else if ( isspace(*p) ) *p = 0; 
		--p;
		//prt(("s6103 brackets=%d\n", brackets ));
	}
}

void JagParser::removeEndUnevenBracketAll( char *str )
{
	if ( NULL == str || *str == 0 ) return;
	char *p= (char*)str;
	//prt(("s1029 removeEndUnevenBracket str=[%s]\n", str ));
	int brackets = 0;
	while ( *p != 0 ) { 
		//prt(("s6128 *p=[%c] brackets=%d\n", *p, brackets ));
		if ( *p == '(' ) { --brackets;}
		else if ( *p == ')' ) { ++brackets;}
		++p; 
	}
	if ( 0 == brackets ) return;
	//prt(("s6108 brackets=%d\n", brackets ));

	--p;
	while ( p != str ) {
		//prt(("s2918 p=[%s]\n", p ));
		if ( *p == ')' ) { 
			--brackets; 
			if ( brackets < 0 ) return;
			*p = '\0'; 
		}
		else *p = '\0'; 
		--p;
		//prt(("s6103 brackets=%d\n", brackets ));
	}
}

int JagParser::addPolygonData( JagPolygon &pgon, const JagStrSplit &sp, bool firstOnly )
{
	const char *str;
	char *p;
	JagLineString3D linestr;
	double dx,dy;
	int nc;
	int cnt = 0;
	for ( int i=JAG_SP_START; i < sp.length(); ++i ) {
		str = sp[i].c_str();
		if ( sp[i] == "|" || sp[i] == "!" ) {
			// start a new polygon
			if ( firstOnly ) break; 
			pgon.add( linestr );
			linestr.init();
		} else {
			nc = strchrnum( str, ':');
			if ( nc < 1 ) continue; // skip bbox
			get2double(str, p, ':', dx, dy );
			JagPoint2D pt(dx,dy);
			linestr.add( pt );
			++cnt;
		}
	}

	if ( cnt < 1 ) return 0;
	pgon.add( linestr );
	return 1;
}

int JagParser::addPolygon3DData( JagPolygon &pgon, const JagStrSplit &sp, bool firstOnly )
{
	const char *str;
	char *p;
	JagLineString3D linestr;
	double dx,dy,dz;
	int nc;
	int cnt = 0;
	for ( int i=JAG_SP_START; i < sp.length(); ++i ) {
		str = sp[i].c_str();
		if ( sp[i] == "|" || sp[i] == "!" ) {
			if ( firstOnly ) break; 
			pgon.add( linestr );
			linestr.init();
		} else {
			nc = strchrnum( str, ':');
			if ( nc < 2 ) continue; // skip bbox
			get3double(str, p, ':', dx, dy, dz );
			JagPoint3D pt(dx,dy,dz);
			linestr.add( pt );
			++cnt;
		}
	}

	if ( cnt < 1 ) return 0;
	pgon.add( linestr );
	return 1;
}


void JagParser::addLineStringData( JagLineString &linestr, const JagStrSplit &sp )
{
	const char *str;
	char *p;
	double dx,dy;
	int nc;
	for ( int i=JAG_SP_START; i < sp.length(); ++i ) {
		str = sp[i].c_str();
		nc = strchrnum( str, ':');
		if ( nc < 1 ) continue;
		get2double(str, p, ':', dx, dy );
		linestr.add( dx, dy );
		//prt(("s4028 addLineStringData add(%.2f %.2f)\n", dx, dy ));
	}
}

// from OJAG type
void JagParser::addLineStringData( JagLineString3D &linestr, const JagStrSplit &sp )
{
	const char *str;
	char *p;
	double dx,dy;
	int nc;
	for ( int i=JAG_SP_START; i < sp.length(); ++i ) {
		str = sp[i].c_str();
		nc = strchrnum( str, ':');
		if ( nc < 1 ) continue;
		get2double(str, p, ':', dx, dy );
		linestr.add( dx, dy );
		//prt(("s4028 addLineStringData add(%.2f %.2f)\n", dx, dy ));
	}
}

// from OJAG type
void JagParser::addLineString3DData( JagLineString3D &linestr, const JagStrSplit &sp )
{
	const char *str;
	char *p;
	double dx,dy, dz;
	for ( int i=JAG_SP_START; i < sp.length(); ++i ) {
		str = sp[i].c_str();
		if ( strchrnum( str, ':') < 2 ) continue;
		get3double(str, p, ':', dx, dy, dz );
		linestr.add( dx, dy, dz );
		prt(("s4048 addLineStringData add(%.2f %.2f %.2f)\n", dx, dy, dz ));
	}
}

int JagParser::getMultiPolygonMinMax( const char *p, double &xmin, double &ymin, double &xmax, double &ymax )
{
	//prt(("s1538 multipolygon( p=[%s]\n", p ));
	int rc;
	const char *q;
	if ( *p == 0 ) return -4532;
	int bracketCount = 0; // count ( ( ) )

	++p; // skip first '('
	++ bracketCount;

	while ( *p != '\0' ) {
		if ( 0 == bracketCount ) break;
		while ( isspace(*p) ) ++p;
		if ( *p == '\0' ) break;
		if ( *p == '(' ) {
			++ bracketCount;
			// search ending ')'
			q = p+1;
			if ( *q == '\0' ) break;
			while ( *q != '\0' ) {
				if ( *q == '(' ) {
					++ bracketCount;
				} else if ( *q == ')' ) {
					-- bracketCount;
					if ( 1 == bracketCount ) {
						Jstr s(p+1, q-p-1); // p: "p ( ) ( ) ( ) q
						rc = getPolygonMinMax( s.c_str(), xmin, ymin, xmax, ymax );
						if ( rc < 0 ) { return rc; }
						p = q;
						break;
					}
				}
				++q;
			}
		} else if ( *p == ')' ) {
			-- bracketCount;
		}
		++p;
	}

	return 0;
}

int JagParser::getMultiPolygon3DMinMax( const char *p, double &xmin, double &ymin, double &zmin,
									    double &xmax, double &ymax, double &zmax )
{
	//prt(("s3338 getMultiPolygon3DMinMax( p=[%s]\n", p ));
	int rc;
	const char *q;
	if ( *p == 0 ) return -4541;
	int bracketCount = 0; // count ( ( ) )

	++p; // skip first '('
	++ bracketCount;

	while ( *p != '\0' ) {
		if ( 0 == bracketCount ) break;
		while ( isspace(*p) ) ++p;
		if ( *p == '\0' ) break;
		if ( *p == '(' ) {
			++ bracketCount;
			// search ending ')'
			q = p+1;
			if ( *q == '\0' ) break;
			while ( *q != '\0' ) {
				if ( *q == '(' ) {
					++ bracketCount;
				} else if ( *q == ')' ) {
					-- bracketCount;
					if ( 1 == bracketCount ) {
						Jstr s(p+1, q-p-1); // p: "p ( ) ( ) ( ) q
						rc = getPolygon3DMinMax( s.c_str(), xmin, ymin, zmin, xmax, ymax, zmax );
						if ( rc < 0 ) { return rc; }
						p = q;
						break;
					}
				}
				++q;
			}
		} else if ( *p == ')' ) {
			-- bracketCount;
		}
		++p;
	}

	return 0;
}

int JagParser::convertJsonToOther( OtherAttribute &other, const char *json, int jsonlen )
{
	//prt(("s3911 convertJsonToOther json=[%s]\n", json ));

	const char *q = json+jsonlen-1;
	while ( *q != '}' && q != json ) --q;
	if ( q == json ) return -3024;
	// q points to last '}'
	++q; // '}'q
	//prt(("s3912 convertJsonToOther json=[%s]\n", json ));

	rapidjson::Document dom;
	dom.Parse( json, q-json );
	if ( dom.HasParseError() ) {
		return -3004;
	}

	const rapidjson::Value& tp = dom["type"];
	const char *type = tp.GetString();
	//prt(("s7580 convertJsonToOther type=[%s]\n", type ));
	int rc = 0;
	if ( 0==strcasecmp( type, "point" ) ) {
		rc = convertJsonToPoint( dom, other );
	} else if ( 0==strcasecmp( type, "linestring" ) ) {
		//prt(("s8322 convertJsonToLineString...\n" ));
		rc = convertJsonToLineString( dom, other );
		//prt(("s8322 convertJsonToLineString rc=%d\n", rc ));
	} else if ( 0==strcasecmp( type, "multipoint" ) ) {
		rc = convertJsonToLineString( dom, other );
		if ( rc < 0 ) return rc;
		if ( other.is3D ) {
			other.type = JAG_C_COL_TYPE_MULTIPOINT3D;
		} else {
			other.type = JAG_C_COL_TYPE_MULTIPOINT;
		}
	} else if ( 0==strcasecmp( type, "polygon" ) ) {
		rc = convertJsonToPolygon( dom, other );
	} else if ( 0==strcasecmp( type, "multilinestring" ) ) {
		rc = convertJsonToPolygon( dom, other );
		if ( rc < 0 ) return rc;
		if ( other.is3D ) {
			other.type = JAG_C_COL_TYPE_MULTILINESTRING3D;
		} else {
			other.type = JAG_C_COL_TYPE_MULTILINESTRING;
		}
	} else if ( 0==strcasecmp( type, "multipolygon" ) ) {
		rc = convertJsonToMultiPolygon( dom, other );
	} else {
		rc = -3410;
	}

	return rc;
}

int JagParser::convertJsonToPoint( const rapidjson::Document &dom, OtherAttribute &other )
{
	bool is3D = false;
	Jstr vs;

	const rapidjson::Value& cd = dom["coordinates"];
	if ( ! cd.IsArray() ) { return -3200; }
	if ( cd.Size() < 2 ) return -3201;
	if ( cd.Size() > 2 ) {
		is3D = true;
		other.type =  JAG_C_COL_TYPE_POINT;
	} else {
		other.type =  JAG_C_COL_TYPE_POINT3D;
	}

	//for (SizeType i = 0; i < cd.Size(); i++) { }
	if ( cd[0].IsString() ) {
		if ( cd[0].GetStringLength() > JAG_POINT_LEN ) return -3202;
		strcpy(other.point.x, cd[0].GetString() );
	} else if ( cd[0].IsNumber() ) {
		vs = doubleToStr(cd[0].GetDouble() ).trimEndZeros();
		if ( vs.size() > JAG_POINT_LEN ) return -3210;
		strcpy(other.point.x, vs.c_str() );
	} else return -3204;

	if ( cd[1].IsString() ) {
		if ( cd[1].GetStringLength() > JAG_POINT_LEN ) return -3205;
		strcpy(other.point.y, cd[1].GetString() );
	} else if ( cd[1].IsNumber() ) {
		vs = doubleToStr(cd[1].GetDouble() ).trimEndZeros();
		if ( vs.size() > JAG_POINT_LEN ) return -3206;
		strcpy(other.point.y, vs.c_str() );
	} else return -3208;

	if ( is3D ) {
    	if ( cd[2].IsString() ) {
			if ( cd[2].GetStringLength() > JAG_POINT_LEN ) return -3209;
    		strcpy(other.point.z, cd[2].GetString() );
    	} else if ( cd[2].IsNumber() ) {
    		vs = doubleToStr(cd[2].GetDouble() ).trimEndZeros();
    		if ( vs.size() > JAG_POINT_LEN ) return -3210;
    		strcpy(other.point.z, vs.c_str() );
    	} else return -3213;
	} 

	other.is3D = is3D;

	return 0;
}

int JagParser::convertJsonToLineString( const rapidjson::Document &dom, OtherAttribute &other )
{
	bool is3D = false;
	Jstr vs;

	const rapidjson::Value& cdc = dom["coordinates"];
	if ( ! cdc.IsArray() ) { return -3230; }

	Jstr valueData = "(";
	bool first = true;

	for ( rapidjson::SizeType i = 0; i < cdc.Size(); i++) { 
		if ( ! cdc[i].IsArray() ) { return -3231; }
		const rapidjson::Value &cd = cdc[i];
		//prt(("s7421 cd.Size()=%d\n",  cd.Size() ));
		if ( cd.Size() < 2 ) return -3233;
		if ( cd.Size() > 2 ) { is3D = true; }
		//prt(("s7421 cd.Size()=%d is3D=%d\n",  cd.Size(), is3D ));
    	if ( cd[0].IsString() ) {
    		if ( cd[0].GetStringLength() > JAG_POINT_LEN ) return -3236;
			vs = Jstr(cd[0].GetString());
    	} else if ( cd[0].IsNumber() ) {
    		vs = doubleToStr(cd[0].GetDouble() ).trimEndZeros();
    		if ( vs.size() > JAG_POINT_LEN ) return -3237;
    	} else return -3239;

		if ( first ) {
			valueData += vs;
			first = false;
		} else {
			valueData += Jstr(",") + vs;
		}
    
    	if ( cd[1].IsString() ) {
    		if ( cd[1].GetStringLength() > JAG_POINT_LEN ) return -3241;
			vs = Jstr(cd[1].GetString());
    	} else if ( cd[1].IsNumber() ) {
    		vs = doubleToStr(cd[1].GetDouble() ).trimEndZeros();
    		if ( vs.size() > JAG_POINT_LEN ) return -3243;
    	} else return -3245;
		valueData += Jstr(" ") + vs;
    
    	if ( is3D ) {
        	if ( cd[2].IsString() ) {
    			if ( cd[2].GetStringLength() > JAG_POINT_LEN ) return -3247;
				vs = Jstr(cd[2].GetString());
        	} else if ( cd[2].IsNumber() ) {
        		vs = doubleToStr(cd[2].GetDouble() ).trimEndZeros();
        		if ( vs.size() > JAG_POINT_LEN ) return -3250;
        	} else return -3252;
			valueData += Jstr(" ") + vs;
    	}
	}
	valueData += Jstr(")");

	if ( is3D ) {
		other.type =  JAG_C_COL_TYPE_LINESTRING3D;
	} else {
		other.type =  JAG_C_COL_TYPE_LINESTRING;
	}
	other.valueData = valueData;
	other.is3D = is3D;
	//prt(("s8630 got valueData=[%s]\n", valueData.c_str() ));

	return 0;
}


int JagParser::convertJsonToPolygon( const rapidjson::Document &dom, OtherAttribute &other )
{
	bool is3D = false;
	Jstr vs;

	const rapidjson::Value& cdc = dom["coordinates"];
	if ( ! cdc.IsArray() ) { return -3240; }

	Jstr valueData = "(";
	bool first1 = true;
	bool first2 = true;

	for ( rapidjson::SizeType i = 0; i < cdc.Size(); i++) { 
		if ( ! cdc[i].IsArray() ) { return -3241; }

		if ( first1 ) {
			valueData += "(";
			first1 = false;
		} else {
			valueData += ",(";
		}

		const rapidjson::Value &cd1 = cdc[i];
    	first2 = true;
		for ( rapidjson::SizeType j = 0; j < cd1.Size(); j++) { 
			const rapidjson::Value &cd = cd1[j];

			if ( cd.Size() < 2 ) return -3243;
			if ( cd.Size() > 2 ) { is3D = true; }

        	if ( cd[0].IsString() ) {
        		if ( cd[0].GetStringLength() > JAG_POINT_LEN ) return -3246;
    			vs = Jstr(cd[0].GetString());
        	} else if ( cd[0].IsNumber() ) {
        		vs = doubleToStr(cd[0].GetDouble() ).trimEndZeros();
        		if ( vs.size() > JAG_POINT_LEN ) return -3247;
    
        	} else return -3249;
    
    		if ( first2 ) {
    			valueData += vs;
    			first2 = false;
    		} else {
    			valueData += Jstr(",") + vs;
    		}
        
        	if ( cd[1].IsString() ) {
        		if ( cd[1].GetStringLength() > JAG_POINT_LEN ) return -3251;
    			vs = Jstr(cd[1].GetString());
        	} else if ( cd[1].IsNumber() ) {
        		vs = doubleToStr(cd[1].GetDouble() ).trimEndZeros();
        		if ( vs.size() > JAG_POINT_LEN ) return -3253;
        	} else return -3255;
    		valueData += Jstr(" ") + vs;
        
        	if ( is3D ) {
            	if ( cd[2].IsString() ) {
        			if ( cd[2].GetStringLength() > JAG_POINT_LEN ) return -3257;
    				vs = Jstr(cd[2].GetString());
            	} else if ( cd[2].IsNumber() ) {
            		vs = doubleToStr(cd[2].GetDouble() ).trimEndZeros();
            		if ( vs.size() > JAG_POINT_LEN ) return -3260;
            	} else return -3262;
    			valueData += Jstr(" ") + vs;
        	}
		}

		valueData += ")";
	}
	valueData += Jstr(")");

	if ( is3D ) {
		other.type =  JAG_C_COL_TYPE_POLYGON3D;
	} else {
		other.type =  JAG_C_COL_TYPE_POLYGON;
	}
	other.valueData = valueData;
	other.is3D = is3D;
	//prt(("s8631 got valueData=[%s]\n", valueData.c_str() ));

	return 0;
}

int JagParser::convertJsonToMultiPolygon( const rapidjson::Document &dom, OtherAttribute &other )
{
	bool is3D = false;
	Jstr vs;

	const rapidjson::Value& cdc = dom["coordinates"];
	if ( ! cdc.IsArray() ) { return -3250; }

	Jstr valueData = "(";
	bool first1 = true;
	bool first2 = true;
	bool first3 = true;

	for ( rapidjson::SizeType i = 0; i < cdc.Size(); i++) { 
		const rapidjson::Value &cd1 = cdc[i];
		if ( ! cd1.IsArray() ) { return -3251; }

		if ( first1 ) {
			valueData += "(";
			first1 = false;
		} else {
			valueData += ",(";
		}

    	first2 = true;
		for ( rapidjson::SizeType j = 0; j < cd1.Size(); j++) { 
			const rapidjson::Value &cd2 = cd1[j];
			if ( ! cd2.IsArray() ) { return -3251; }

			if ( first2 ) {
				valueData += "(";
				first2 = false;
			} else {
				valueData += ",(";
			}

    		first3 = true;
			for ( rapidjson::SizeType k = 0; k < cd2.Size(); k++) { 
				const rapidjson::Value &cd = cd2[k];
				if ( ! cd.IsArray() ) { return -3252; }
			
    			if ( cd.Size() < 2 ) return -3253;
    			if ( cd.Size() > 2 ) { is3D = true; }
    
            	if ( cd[0].IsString() ) {
            		if ( cd[0].GetStringLength() > JAG_POINT_LEN ) return -3256;
        			vs = Jstr(cd[0].GetString());
            	} else if ( cd[0].IsNumber() ) {
            		vs = doubleToStr(cd[0].GetDouble() ).trimEndZeros();
            		if ( vs.size() > JAG_POINT_LEN ) return -3257;
        
            	} else return -3259;
        
        		if ( first3 ) {
        			valueData += vs;
        			first3 = false;
        		} else {
        			valueData += Jstr(",") + vs;
        		}
            
            	if ( cd[1].IsString() ) {
            		if ( cd[1].GetStringLength() > JAG_POINT_LEN ) return -3261;
        			vs = Jstr(cd[1].GetString());
            	} else if ( cd[1].IsNumber() ) {
            		vs = doubleToStr(cd[1].GetDouble() ).trimEndZeros();
            		if ( vs.size() > JAG_POINT_LEN ) return -3262;
            	} else return -3264;
        		valueData += Jstr(" ") + vs;
            
            	if ( is3D ) {
                	if ( cd[2].IsString() ) {
            			if ( cd[2].GetStringLength() > JAG_POINT_LEN ) return -3265;
        				vs = Jstr(cd[2].GetString());
                	} else if ( cd[2].IsNumber() ) {
                		vs = doubleToStr(cd[2].GetDouble() ).trimEndZeros();
                		if ( vs.size() > JAG_POINT_LEN ) return -3266;
                	} else return -3267;
        			valueData += Jstr(" ") + vs;
            	}

			}
			valueData += ")";
		}

		valueData += ")";
	}
	valueData += Jstr(")");

	if ( is3D ) {
		other.type =  JAG_C_COL_TYPE_MULTIPOLYGON3D;
	} else {
		other.type =  JAG_C_COL_TYPE_MULTIPOLYGON;
	}
	other.valueData = valueData;
	other.is3D = is3D;
	//prt(("s8633 got valueData=[%s]\n", valueData.c_str() ));

	return 0;
}

int JagParser::getEachRangeFieldLength( int srid ) const
{
	int len = JAG_DATETIME_FIELD_LEN;
	if ( JAG_RANGE_DATE == srid )  {
		len = JAG_DATE_FIELD_LEN;
	} else if ( JAG_RANGE_TIME == srid ) {
		len = JAG_TIME_FIELD_LEN;
	} else if ( JAG_RANGE_DATETIME == srid ) {
		len = JAG_DATETIME_FIELD_LEN;
	} else if ( JAG_RANGE_DATETIMESEC == srid ) {
		len = JAG_DATETIMESEC_FIELD_LEN;
	} else if ( JAG_RANGE_DATETIMENANO == srid ) {
		len = JAG_DATETIMENANO_FIELD_LEN;
	} else if ( JAG_RANGE_DATETIMEMILL == srid ) {
		len = JAG_DATETIMEMILL_FIELD_LEN;
	} else if ( JAG_RANGE_BIGINT == srid ) {
		len = JAG_DBIGINT_FIELD_LEN;
	} else if ( JAG_RANGE_INT == srid ) {
		len = JAG_DINT_FIELD_LEN;
	} else if ( JAG_RANGE_SMALLINT == srid ) {
		len = JAG_DSMALLINT_FIELD_LEN;
	} else if ( JAG_RANGE_LONGDOUBLE == srid ) {
		len = JAG_LONGDOUBLE_FIELD_LEN;
	} else if ( JAG_RANGE_DOUBLE == srid ) {
		len = JAG_DOUBLE_FIELD_LEN;
	} else if ( JAG_RANGE_FLOAT == srid ) {
		len = JAG_FLOAT_FIELD_LEN;
	} 

	return len;
}

Jstr JagParser::getFieldType( int srid ) 
{
	Jstr ctype = JAG_C_COL_TYPE_DATETIME;
	if ( JAG_RANGE_DATE == srid )  {
		ctype = JAG_C_COL_TYPE_DATE;
	} else if ( JAG_RANGE_TIME == srid ) {
		ctype = JAG_C_COL_TYPE_TIME;
	} else if ( JAG_RANGE_DATETIME == srid ) {
		ctype = JAG_C_COL_TYPE_DATETIME;
	} else if ( JAG_RANGE_DATETIMESEC == srid ) {
		ctype = JAG_C_COL_TYPE_DATETIMESEC;
	} else if ( JAG_RANGE_DATETIMENANO == srid ) {
		ctype = JAG_C_COL_TYPE_DATETIMENANO;
	} else if ( JAG_RANGE_DATETIMEMILL == srid ) {
		ctype = JAG_C_COL_TYPE_DATETIMEMILL;
	} else if ( JAG_RANGE_BIGINT == srid ) {
		ctype = JAG_C_COL_TYPE_DBIGINT;
	} else if ( JAG_RANGE_INT == srid ) {
		ctype = JAG_C_COL_TYPE_DINT;
	} else if ( JAG_RANGE_SMALLINT == srid ) {
		ctype = JAG_C_COL_TYPE_DSMALLINT;
	} else if ( JAG_RANGE_LONGDOUBLE == srid ) {
		ctype = JAG_C_COL_TYPE_LONGDOUBLE;
	} else if ( JAG_RANGE_DOUBLE == srid ) {
		ctype = JAG_C_COL_TYPE_DOUBLE;
	} else if ( JAG_RANGE_FLOAT == srid ) {
		ctype = JAG_C_COL_TYPE_FLOAT;
	} 

	return ctype;
}

Jstr JagParser::getFieldTypeString( int srid ) 
{
	Jstr ctype = "datetime";
	if ( JAG_RANGE_DATE == srid )  {
		ctype = "date";
	} else if ( JAG_RANGE_TIME == srid ) {
		ctype = "time";
	} else if ( JAG_RANGE_DATETIME == srid ) {
		ctype = "datetime";
	} else if ( JAG_RANGE_DATETIMESEC == srid ) {
		ctype = "datetimesec";
	} else if ( JAG_RANGE_DATETIMENANO == srid ) {
		ctype = "datetimenano";
	} else if ( JAG_RANGE_DATETIMEMILL == srid ) {
		ctype = "datetimemill";
	} else if ( JAG_RANGE_BIGINT == srid ) {
		ctype = "bigint";
	} else if ( JAG_RANGE_INT == srid ) {
		ctype = "int";
	} else if ( JAG_RANGE_SMALLINT == srid ) {
		ctype = "smallint";
	} else if ( JAG_RANGE_LONGDOUBLE == srid ) {
		ctype = "longdouble";
	} else if ( JAG_RANGE_DOUBLE == srid ) {
		ctype = "double";
	} else if ( JAG_RANGE_FLOAT == srid ) {
		ctype = "float";
	} 

	return ctype;
}

bool JagParser::getMetrics( const JagStrSplitWithQuote &sp, JagVector<Jstr> &metrics )
{
	prt(("s422398 getMetrics \n"));
	Jstr t;
	int cnt = 0;
	for ( int i = 0; i < sp.size(); ++i ) {
		t = sp[i];
		if ( '\'' == *(t.c_str()) ) {
			t.trimChar( '\'');
			metrics.append( t );
			++ cnt;
		} else if ( '"' == *(t.c_str()) ) {
			t.trimChar( '"');
			metrics.append( t );
			++ cnt;
		}
		if ( strchr(t.s(), '#') ) return false;
	}

	//metrics.print(); 

	return true;
}

int JagParser::convertConstantObjToJAG( const JagFixString &instr, Jstr &outstr )
{
	int cnt = 0;
	Jstr othertype;
	int rc = 0;
	char *p = (char*)instr.c_str();
	if ( ! p || *p == '\0' ) return 0;
	if ( strncasecmp( p, "point(", 6 ) == 0 || strncasecmp( p, "point3d(", 8 ) == 0 ) {
		while ( *p != '(' ) ++p; ++p;  // (p
		if ( *p == 0 ) return -10;
		JagParser::replaceChar(p, ',', ' ', ')' );
		JagStrSplit sp(p, ' ', true );
		for ( int k=0; k < sp.length(); ++k ) { if ( sp[k].length() >= JAG_POINT_LEN ) { return -381; } }
		if ( sp.length() == 3 ) {
			outstr = Jstr("CJAG=0=0=PT3=d 0:0:0:0:0:0 ") + sp[0] + " " + sp[1] + " " + sp[2];
		} else if ( sp.length() == 2 ) {
			outstr = Jstr("CJAG=0=0=PT=d 0:0:0:0 ") + sp[0] + " " + sp[1];
		} else {
			outstr = "";
			return -9;
		}
		++cnt;
	} else if ( strncasecmp( p, "circle(", 7 ) == 0 ) {
		while ( *p != '(' ) ++p; ++p;  // (p
		if ( *p == 0 ) return -20;
		JagParser::replaceChar(p, ',', ' ', ')' );
		JagStrSplit sp(p, ' ', true );
		if (  sp.length() < 3 ) { return -30; }
		for ( int k=0; k < sp.length(); ++k ) { if ( sp[k].length() >= JAG_POINT_LEN ) { return -382; } }
		outstr = Jstr("CJAG=0=0=CR=d 0:0:0:0 ") + sp[0] + " " + sp[1] + " " + sp[2];
		++cnt;
	} else if ( strncasecmp( p, "square(", 7 )==0 ) {
		while ( *p != '(' ) ++p; ++p;  // (p
		if ( *p == 0 ) return -20;
		JagParser::replaceChar(p, ',', ' ', ')' );
		JagStrSplit sp(p, ' ', true );
		if (  sp.length() < 3 ) { return -30; }
		for ( int k=0; k < sp.length(); ++k ) { if ( sp[k].length() >= JAG_POINT_LEN ) { return -383; } }
		if ( sp.length() <= 3 ) {
			outstr = Jstr("CJAG=0=0=SQ=d 0:0:0:0 ") + sp[0] + " " + sp[1] + " " + sp[2] + " 0.0";
		} else {
			outstr = Jstr("CJAG=0=0=SQ=d 0:0:0:0 ") + sp[0] + " " + sp[1] + " " + sp[2] + " " + sp[3];
		}
		++cnt;
	} else if (  strncasecmp( p, "cube(", 5 )==0 || strncasecmp( p, "sphere(", 7 )==0 ) {
		// cube( x y z radius )   inner circle radius
		if ( strncasecmp( p, "cube", 4 )==0 ) {
			othertype =  JAG_C_COL_TYPE_CUBE;
		} else {
			othertype =  JAG_C_COL_TYPE_SPHERE;
		}

		while ( *p != '(' ) ++p; ++p;  // (p
		if ( *p == 0 ) return -30;
		JagParser::replaceChar(p, ',', ' ', ')' );
		JagStrSplit sp(p, ' ', true );
		if (  sp.length() < 4 ) { return -32; }
		for ( int k=0; k < sp.length(); ++k ) { if ( sp[k].length() >= JAG_POINT_LEN ) { return -384; } }
		outstr = Jstr("CJAG=0=0=") + othertype + "=d 0:0:0:0:0:0 " + sp[0] + " " + sp[1] + " " + sp[2] + " " + sp[3] + " ";
		++cnt;
		if ( othertype == JAG_C_COL_TYPE_CUBE ) {
			Jstr nx, ny;
			if ( sp.length() >= 5 ) { nx = sp[4]; } else { nx="0.0"; }
			if ( sp.length() >= 6 ) { ny = sp[5]; } else { ny="0.0"; }
			outstr += nx + " " + ny;
		}
	} else if ( strncasecmp( p, "circle3d(", 9 )==0
				|| strncasecmp( p, "square3d(", 9 )==0 ) {
		// circle3d( x y z radius nx ny )   inner circle radius
		if ( strncasecmp( p, "circ", 4 )==0 ) {
			othertype =  JAG_C_COL_TYPE_CIRCLE3D;
		} else {
			othertype =  JAG_C_COL_TYPE_SQUARE3D;
		} 

		while ( *p != '(' ) ++p; ++p;  // (p
		if ( *p == 0 ) return -40;
		JagParser::replaceChar(p, ',', ' ', ')' );
		JagStrSplit sp(p, ' ', true );
		if (  sp.length() < 4 ) { return -42; }
		for ( int k=0; k < sp.length(); ++k ) { if ( sp[k].length() >= JAG_POINT_LEN ) { return -385; } }
		outstr = Jstr("CJAG=0=0=") + othertype + "=d 0:0:0:0:0:0 " + sp[0] + " " + sp[1] + " " + sp[2] + " " + sp[3] + " ";
		Jstr nx, ny;
		if ( sp.length() >= 5 ) { nx = sp[4]; } else { nx="0.0"; }
		if ( sp.length() >= 6 ) { ny = sp[5]; } else { ny="0.0"; }
		outstr += nx + " " + ny;
		++cnt;
	} else if (  strncasecmp( p, "rectangle(", 10 )==0 || strncasecmp( p, "ellipse(", 8 )==0 ) {
		// rectangle( x y width height nx ny) 
		if ( strncasecmp( p, "rect", 4 )==0 ) {
			othertype =  JAG_C_COL_TYPE_RECTANGLE;
		} else {
			othertype =  JAG_C_COL_TYPE_ELLIPSE;
		}
		while ( *p != '(' ) ++p; ++p;  // (p
		if ( *p == 0 ) return -45;
		JagParser::replaceChar(p, ',', ' ', ')' );
		JagStrSplit sp(p, ' ', true );
		if (  sp.length() < 4 ) { return -46; }
		for ( int k=0; k < sp.length(); ++k ) { if ( sp[k].length() >= JAG_POINT_LEN ) { return -386; } }
		outstr = Jstr("CJAG=0=0=") + othertype + "=d 0:0:0:0 " + sp[0] + " " + sp[1] + " " + sp[2] + " " + sp[3] + " ";
		Jstr nx;
		if ( sp.length() >= 5 ) { nx = sp[4]; } else { nx="0.0"; }
		outstr += nx;
		++cnt;
	} else if ( strncasecmp( p, "rectangle3d(", 12 )==0 || strncasecmp( p, "ellipse3d(", 10 )==0 ) {
		// rectangle( x y z width height nx ny ) 
		if ( strncasecmp( p, "rect", 4 )==0 ) {
			othertype =  JAG_C_COL_TYPE_RECTANGLE3D;
		} else {
			othertype =  JAG_C_COL_TYPE_ELLIPSE3D;
		}
		while ( *p != '(' ) ++p; ++p;  // (p
		if ( *p == 0 ) return -55;
		JagParser::replaceChar(p, ',', ' ', ')' );
		JagStrSplit sp(p, ' ', true );
		if (  sp.length() < 5 ) { return -48; }
		for ( int k=0; k < sp.length(); ++k ) { if ( sp[k].length() >= JAG_POINT_LEN ) { return -387; } }
		outstr = Jstr("CJAG=0=0=") + othertype + "=d 0:0:0:0:0:0 " + sp[0] + " " + sp[1] + " " + sp[2] + " " + sp[3] + " ";
		outstr += sp[4] + " ";
		Jstr nx, ny;
		if ( sp.length() >= 6 ) { nx = sp[5]; } else { nx="0.0"; }
		if ( sp.length() >= 7 ) { ny = sp[6]; } else { ny="0.0"; }
		outstr += nx + " " + ny;
		++cnt;
	} else if (  strncasecmp( p, "box(", 4 )==0 || strncasecmp( p, "ellipsoid(", 10 )==0 ) {
		// box( x y z width depth height nx ny ) 
		if ( strncasecmp( p, "box", 3 )==0 ) {
			othertype =  JAG_C_COL_TYPE_BOX;
		} else {
			othertype =  JAG_C_COL_TYPE_ELLIPSOID;
		}
		while ( *p != '(' ) ++p;
		++p;  // (p
		if ( *p == 0 ) return -56;
		JagParser::replaceChar(p, ',', ' ', ')' );
		JagStrSplit sp(p, ' ', true );
		if (  sp.length() < 6 ) { return -49; }
		for ( int k=0; k < sp.length(); ++k ) { if ( sp[k].length() >= JAG_POINT_LEN ) { return -388; } }
		outstr = Jstr("CJAG=0=0=") + othertype + "=d 0:0:0:0:0:0 " + sp[0] + " " + sp[1] + " " + sp[2] + " " + sp[3] + " ";
		outstr += sp[4] + " " + sp[5];
		Jstr nx, ny;
		if ( sp.length() >= 7 ) { nx = sp[6]; } else { nx="0.0"; }
		if ( sp.length() >= 8 ) { ny = sp[7]; } else { ny="0.0"; }
		outstr += nx + " " + ny;
		++cnt;
	} else if ( strncasecmp( p, "cylinder(", 9 )==0 || strncasecmp( p, "cone(", 5 )==0 ) {
		// cylinder( x y z r height  nx ny 
		if ( strncasecmp( p, "cone", 4 )==0 ) {
			othertype =  JAG_C_COL_TYPE_CONE;
		} else {
			othertype =  JAG_C_COL_TYPE_CYLINDER;
		}
		while ( *p != '(' ) ++p;
		++p;  // (p
		if ( *p == 0 ) return -58;
		JagParser::replaceChar(p, ',', ' ', ')' );
		JagStrSplit sp(p, ' ', true );
		if (  sp.length() < 5 ) { return -52; }
		for ( int k=0; k < sp.length(); ++k ) { if ( sp[k].length() >= JAG_POINT_LEN ) { return -390; } }
		outstr = Jstr("CJAG=0=0=") + othertype + "=d 0:0:0:0:0:0 " + sp[0] + " " + sp[1] + " " + sp[2] + " " + sp[3] + " ";
		outstr += sp[4];
		Jstr nx, ny;
		if ( sp.length() >= 6 ) { nx = sp[5]; } else { nx="0.0"; }
		if ( sp.length() >= 7 ) { ny = sp[6]; } else { ny="0.0"; }
		outstr += nx + " " + ny;
		++cnt;
	} else if ( strncasecmp( p, "line(", 5 )==0 ) {
		// line( x1 y1 x2 y2)
		while ( *p != '(' ) ++p; ++p;  // (p
		if ( *p == 0 ) return -61;
		JagParser::replaceChar(p, ',', ' ', ')' );
		JagStrSplit sp(p, ' ', true );
		if (  sp.length() != 4 ) { return -55; }
		for ( int k=0; k < sp.length(); ++k ) { if ( sp[k].length() >= JAG_POINT_LEN ) { return -391; } }
		othertype =  JAG_C_COL_TYPE_LINE;
		outstr = Jstr("CJAG=0=0=") + othertype + "=d 0:0:0:0 " + sp[0] + " " + sp[1] + " " + sp[2] + " " + sp[3];
		++cnt;
	} else if ( strncasecmp( p, "line3d(", 7 )==0 ) {
		// line3d(x1 y1 z1 x2 y2 z2)
		while ( *p != '(' ) ++p; ++p;  // (p
		if ( *p == 0 ) return -64;
		JagParser::replaceChar(p, ',', ' ', ')' );
		JagStrSplit sp(p, ' ', true );
		if (  sp.length() != 6 ) { return -57; }
		for ( int k=0; k < sp.length(); ++k ) { if ( sp[k].length() >= JAG_POINT_LEN ) { return -440; } }
		othertype =  JAG_C_COL_TYPE_LINE3D;
		outstr = Jstr("CJAG=0=0=") + othertype + "=d 0:0:0:0:0:0 " + sp[0] + " " + sp[1] + " " + sp[2] + " " + sp[3] + " ";
		outstr += sp[4] + " " + sp[5];
		++cnt;
	} else if ( strncasecmp( p, "linestring(", 11 )==0 ) {
		while ( *p != '(' ) ++p;  ++p;
		if ( *p == 0 ) return -64;
		othertype =  JAG_C_COL_TYPE_LINESTRING;
		outstr = Jstr("CJAG=0=0=") + othertype + "=d 0:0:0:0 ";
		JagStrSplit sp(p, ',', true );
		int len = sp.length();
		for ( int i = 0; i < len; ++i ) {
			JagStrSplit ss( sp[i], ' ', true );
			if ( ss.length() < 2 ) {  continue; }
			outstr += Jstr(" ") + ss[0] + ":" + ss[1];
		}
		++cnt;
	} else if ( strncasecmp( p, "linestring3d(", 13 )==0 ) {
		// linestring( x1 y1 z1, x2 y2 z2, x3 y3 z3, x4 y4 z4)
		//prt(("s2836 linestring3d( p=[%s]\n", p ));
		while ( *p != '(' ) ++p; ++p;
		if ( *p == 0 ) return -65;
		othertype =  JAG_C_COL_TYPE_LINESTRING3D;
		outstr = Jstr("CJAG=0=0=") + othertype + "=d 0:0:0:0:0:0 ";
		JagStrSplit sp(p, ',', true );
		int len = sp.length();
		for ( int i = 0; i < len; ++i ) {
			JagStrSplit ss( sp[i], ' ', true );
			if ( ss.length() < 3 ) {  continue; }
			outstr += Jstr(" ") + ss[0] + ":" + ss[1]  + ":" + ss[2];
			++cnt;
		}
	} else if ( strncasecmp( p, "multipoint(", 11 )==0 ) {
		// multipoint( x1 y1, x2 y2, x3 y3, x4 y4)
		//prt(("s2834 multipoint( p=[%s]\n", p ));
		while ( *p != '(' ) ++p;  ++p;
		if ( *p == 0 ) return -67;
		othertype =  JAG_C_COL_TYPE_MULTIPOINT;
		outstr = Jstr("CJAG=0=0=") + othertype + "=d 0:0:0:0 ";
		JagStrSplit sp(p, ',', true );
		int len = sp.length();
		for ( int i = 0; i < len; ++i ) {
			JagStrSplit ss( sp[i], ' ', true );
			if ( ss.length() < 2 ) {  continue; }
			outstr += Jstr(" ") + ss[0] + ":" + ss[1];
			++cnt;
		}
	} else if ( strncasecmp( p, "multipoint3d(", 13 )==0 ) {
		// multipoint3d( x1 y1 z1, x2 y2 z2, x3 y3 z3, x4 y4 z4)
		//prt(("s2836 multipoint3d( p=[%s]\n", p ));
		while ( *p != '(' ) ++p; ++p;
		if ( *p == 0 ) return -68;
		othertype =  JAG_C_COL_TYPE_MULTIPOINT3D;
		outstr = Jstr("CJAG=0=0=") + othertype + "=d 0:0:0:0:0:0 ";
		JagStrSplit sp(p, ',', true );
		int len = sp.length();
		for ( int i = 0; i < len; ++i ) {
			JagStrSplit ss( sp[i], ' ', true );
			if ( ss.length() < 3 ) {  continue; }
			outstr += Jstr(" ") + ss[0] + ":" + ss[1]  + ":" + ss[2];
			++cnt;
		}
	} else if ( strncasecmp( p, "polygon(", 8 )==0 ) {
		// polygon( ( x1 y1, x2 y2, x3 y3, x4 y4), ( 2 3, 3 4, 9 8, 2 3 ), ( ...) )
		//prt(("s3834 polygon( p=[%s]\n", p ));
		while ( *p != '(' ) ++p; ++p;
		if ( *p == 0 ) return -72;
		othertype =  JAG_C_COL_TYPE_POLYGON;
		outstr = Jstr("CJAG=0=0=") + othertype + "=d 0:0:0:0 ";
		Jstr pgonstr;
		rc = JagParser::addPolygonData( pgonstr, p, false, true );
		if ( rc <= 0 ) return rc; 
		outstr += pgonstr;
		++cnt;
	} else if ( strncasecmp( p, "polygon3d(", 10 )==0 ) {
		// polygon( ( x1 y1 z1, x2 y2 z2, x3 y3 z3, x4 y4 z4), ( 2 3 8, 3 4 0, 9 8 2, 2 3 8 ), ( ...) )
		//prt(("s3835 polygon3d( p=[%s] )\n", p ));
		while ( *p != '(' ) ++p; ++p;
		if ( *p == 0 ) return -73;
		othertype =  JAG_C_COL_TYPE_POLYGON3D;
		outstr = Jstr("CJAG=0=0=") + othertype + "=d 0:0:0:0:0:0 ";
		Jstr pgonstr;
		rc = JagParser::addPolygon3DData( pgonstr, p, false, true );
		if ( rc <= 0 ) return rc; 
		outstr += pgonstr;
		++cnt;
	} else if ( strncasecmp( p, "multipolygon(", 13 )==0 ) {
		// multipolygon( (( x1 y1, x2 y2, x3 y3, x4 y4), ( 2 3, 3 4, 9 8, 2 3 ), ( ...)), ( (..), (..) ) )
		prt(("s3834 multipolygon( p=[%s]\n", p ));
		while ( *p != '(' ) ++p;  // p: "( ((...), (...), (...)), (...), ... )
		othertype =  JAG_C_COL_TYPE_MULTIPOLYGON;
		outstr = Jstr("CJAG=0=0=") + othertype + "=d 0:0:0:0 ";
		Jstr mgon;
		rc = JagParser::addMultiPolygonData( mgon, p, false, false, false );
		prt(("s3238 addMultiPolygonData mgon=[%s] rc=%d\n", mgon.c_str(), rc ));
		if ( rc <= 0 ) return rc; 
		outstr += mgon;
		++cnt;
	} else if ( strncasecmp( p, "multipolygon3d(", 10 )==0 ) {
		// polygon( ( x1 y1 z1, x2 y2 z2, x3 y3 z3, x4 y4 z4), ( 2 3 8, 3 4 0, 9 8 2, 2 3 8 ), ( ...) )
		//prt(("s3835 polygon3d( p=[%s] )\n", p ));
		while ( *p != '(' ) ++p;  // "(p ((...), (...), (...)), (...), ... ) 
		othertype =  JAG_C_COL_TYPE_MULTIPOLYGON3D;
		outstr = Jstr("CJAG=0=0=") + othertype + "=d 0:0:0:0:0:0 ";
		Jstr mgon;
		rc = JagParser::addMultiPolygonData( mgon, p, false, true, true );
		if ( rc <= 0 ) return rc; 
		outstr += mgon;
		++cnt;
	} else if ( strncasecmp( p, "multilinestring(", 16 )==0 ) {
		// multilinestring( ( x1 y1, x2 y2, x3 y3, x4 y4), ( 2 3, 3 4, 9 8, 2 3 ), ( ...) )
		//prt(("s3834 polygon( p=[%s]\n", p ));
		while ( *p != '(' ) ++p; ++p;
		if ( *p == 0 ) return -74;
		othertype =  JAG_C_COL_TYPE_MULTILINESTRING;
		outstr = Jstr("CJAG=0=0=") + othertype + "=d 0:0:0:0 ";
		Jstr pgonstr;
		rc = JagParser::addPolygonData( pgonstr, p, false, false );
		if ( rc <= 0 ) return rc; 
		outstr += pgonstr;
		++cnt;
	} else if ( strncasecmp( p, "multilinestring3d(", 10 )==0 ) {
		// multilinestring3d( ( x1 y1 z1, x2 y2 z2, x3 y3 z3, x4 y4 z4), ( 2 3 8, 3 4 0, 9 8 2, 2 3 8 ), ( ...) )
		//prt(("s3835 multilinestring3d( p=[%s] )\n", p ));
		while ( *p != '(' ) ++p; ++p;
		if ( *p == 0 ) return -78;
		othertype =  JAG_C_COL_TYPE_MULTILINESTRING3D;
		outstr = Jstr("CJAG=0=0=") + othertype + "=d 0:0:0:0:0:0 ";
		Jstr pgonstr;
		rc = JagParser::addPolygon3DData( pgonstr, p, false, false );
		if ( rc <= 0 ) return rc; 
		outstr += pgonstr;
		++cnt;
	} else if ( strncasecmp( p, "triangle(", 9 )==0 ) {
		// triangle(x1 y1 x2 y2 x3 y3 )
		while ( *p != '(' ) ++p; ++p;  // (p
		if ( *p == 0 ) return -82;
		JagParser::replaceChar(p, ',', ' ', ')' );
		JagStrSplit sp(p, ' ', true );
		if (  sp.length() != 6 ) { return -387; }
		for ( int k=0; k < sp.length(); ++k ) { if ( sp[k].length() >= JAG_POINT_LEN ) { return -318; } }
		othertype =  JAG_C_COL_TYPE_TRIANGLE;
		outstr = Jstr("CJAG=0=0=") + othertype + "=d 0:0:0:0 " + sp[0] + " " + sp[1] + " " + sp[2] + " " + sp[3] + " ";
		outstr += sp[4] + " " + sp[5];
		++cnt;
	} else if ( strncasecmp( p, "triangle3d(", 11 )==0 ) {
		// triangle3d(x1 y1 z1 x2 y2 z2 x3 y3 z3 )
		while ( *p != '(' ) ++p; ++p;  // (p
		if ( *p == 0 ) return -88;
		JagParser::replaceChar(p, ',', ' ', ')' );
		JagStrSplit sp(p, ' ', true );
		if (  sp.length() != 9 ) { return -390; }
		for ( int k=0; k < sp.length(); ++k ) { if ( sp[k].length() >= JAG_POINT_LEN ) { return -3592; } }
		othertype =  JAG_C_COL_TYPE_TRIANGLE3D;
		outstr = Jstr("CJAG=0=0=") + othertype + "=d 0:0:0:0:0:0 " + sp[0] + " " + sp[1] + " " + sp[2] + " " + sp[3] + " ";
		outstr += sp[4] + " " + sp[5] + " " + sp[6] + " " + sp[7] + " " + sp[8];
		++cnt;
	}

	return cnt;
}

