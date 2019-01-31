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
#include <sys/types.h>
#include <sys/time.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <iostream>
#include <signal.h>
#include <stdarg.h>

#include <abax.h>
#include <JagCfg.h>
#include <JagGapVector.h>
#include <JagBlock.h>
#include <JagArray.h>
#include <JagClock.h>
#include <JaguarCPPClient.h>
#include <JagUtil.h>
#include <JagStrSplit.h>

class PassParam {
	public:
		Jstr username;
		Jstr passwd;
		Jstr host;
		Jstr port;
		Jstr dbname;
		Jstr session;
};
PassParam g_param;
int  _debug = 0;

// void 	sig_int(int sig);
int  	parseArgs( int argc, char *argv[], 
				Jstr &username, Jstr &passwd, Jstr &host, Jstr &port,
				Jstr& dbname, Jstr &sqlcmd, int &echo, Jstr &exclusive, 
				Jstr &fullConnect, Jstr &mcmd, Jstr &clearex, 
				Jstr &vvdebug, Jstr &quiet, Jstr &keepNewline, Jstr &sqlFile );

int queryAndReply( JaguarCPPClient& jcli, const char *query, bool ishello=false );
void usage( const char *prog );
void* sendStopSignal(void *p);
void  printResult( const JaguarCPPClient &jcli, const JagClock &clock, abaxint cnt );
int executeCommands( JagClock &clock, const Jstr &sqlFile, JaguarCPPClient &jcli, 
					 const Jstr &quiet, int echo, int saveNewline, const Jstr &username );

void printit( FILE *outf, const char *fmt, ... );

int expectCorrectRows;
int expectErrorRows;
bool lastCmdIsExpect;


//  jcli -u test -p test -h 128.22.22.3:2345 -d mydb
int main(int argc, char *argv[])
{
	char *pcmd;
	int rc;
	JaguarCPPClient jcli;
	Jstr username, passwd, host, port, dbname, exclusive, fullconnect; 
	Jstr sqlcmd, mcmd, clearex, vvdebug, quiet, keepNewline, sqlFile;
	int  echo;
	int  isEx = 0;
	int  saveNewline = 1;

	host = "127.0.0.1";
	port = "8888";
	username = "";
	passwd = "";
	dbname = "test";
	echo = 0;
	fullconnect = "yes";
	clearex = "no";
	vvdebug = "no";
	quiet = "no";
	// keepNewline = "yes";
	keepNewline = "no";

	JagNet::socketStartup();

	Jstr argPort, unixSocket;
	JagClock clock;
	parseArgs( argc, argv, username, passwd, host, argPort, dbname, sqlcmd, echo, exclusive, 
			   fullconnect,  mcmd, clearex, vvdebug, quiet, keepNewline, sqlFile );
	// printf("u=%s p=%s h=%s port=%s dbname=%s\n", username.c_str(), passwd.c_str(), host.c_str(), port.c_str(), dbname.c_str() );
	// printf("sqlcmd=[%s]\n", sqlcmd.c_str() );
	if ( argPort.size()>0 ) {
		port = argPort;
	}

	char *p = NULL;
	if ( username.size() < 1 ) {
		p = getenv("USERNAME");
		if ( p ) { username = p; }
		if ( username.size() < 1 ) {
			username = "test";
		}
	}

	if ( passwd.size() < 1 ) {
		p = getenv("PASSWORD");
		if ( p ) { passwd = getenv("PASSWORD"); }
	}
	if ( passwd.length() < 1 ) {
		prt(("Password: "));
		getPassword( passwd );
	}

	if ( passwd.length() < 1 ) {
        printit( jcli._outf, "Please enter the correct password.\n");
		usage( argv[0] );
        exit(1);
	}

	unixSocket = "src=jql";
	if ( exclusive == "yes" ) {
		unixSocket += "/exclusive=yes";
	}

	if ( clearex == "yes" ) {
		unixSocket += "/clearex=yes";
	}

	if ( fullconnect == "yes" ) {
		jcli.setFullConnection( true );
	} else {
		jcli.setFullConnection( false );
	}

	if ( vvdebug == "yes" ) {
		jcli.setDebug( true );
		_debug = 1;
	} else {
		jcli.setDebug( false );
		_debug = 0;
	}

	// put back new line in stdin
	if ( keepNewline == "yes" ) {
		saveNewline = 1;
	} else {
		saveNewline = 0;
	}

	//prt(("c8293 connect host=[%s] port=[%s] user=[%s] pass=[%s]\n", host.c_str(), port.c_str(), username.c_str(), passwd.c_str() ));

    if ( ! jcli.connect( host.c_str(), atoi(port.c_str()), username.c_str(), passwd.c_str(), dbname.c_str(), unixSocket.c_str(), 0 ) ) {
        printit( jcli._outf, "Error connect to [%s:%s/%s] [%s]\n", host.c_str(), port.c_str(), dbname.c_str(), jcli.error() );
        // printf( "%s\n", jcli.getMessage() );
        printit( jcli._outf, "Please make sure you have provided the correct username, password, database, and host:port\n");
        jcli.close();
		usage( argv[0] );
        exit(1);
    }

	// jcli.recoverAllLogs();

	g_param.username = username;
	g_param.passwd = passwd;
	g_param.dbname = dbname;
	g_param.host = host;
	g_param.port = port;
	g_param.session = jcli.getSession();

	// setup sig handler SIGINT  ctrl-C entered by user
	#ifndef _WINDOWS64_
	signal( SIGHUP, SIG_IGN );
	signal( SIGCHLD, SIG_IGN);
	#endif

	if ( sqlcmd.length() > 0 ) {
    	rc = queryAndReply( jcli, sqlcmd.c_str() );
		// jcli.close( );
		// printf("c4481 sqlcmd=[%s]\n", sqlcmd.c_str() );
		// fflush( stdout );
		if ( ! rc ) {
			printit( jcli._outf, "Command [%s] failed. %s\n", sqlcmd.c_str(), jcli.error() );
		}
		return 1;
	}

	if ( mcmd.length() > 1 ) {
		JagStrSplit sp( mcmd, ';', true );
		for ( int i = 0; i < sp.length(); ++i ) {
			// prt(("%s ...\n", sp[i].c_str() ));
    		queryAndReply( jcli, sp[i].c_str() );
		}
		return 1;
	}

	//////// send hello
    rc = queryAndReply( jcli, "hello", true );
    if ( ! rc ) {
        jcli.close( );
        // exit(1);
		return 1;
    }

	//////////////////
	executeCommands( clock, sqlFile, jcli, quiet, echo, saveNewline, username );
	jcli.close();
	JagNet::socketCleanup();
    return 0;
}

int  parseArgs( int argc, char *argv[], Jstr &username, Jstr &passwd, 
				Jstr &host, Jstr &port, Jstr &dbname, Jstr &sqlcmd,
				int &echo, Jstr &exclusive, Jstr &fullconnect, Jstr &mcmd, Jstr &clearex,
				Jstr & vvdebug, Jstr &quiet, Jstr &keepNewline, Jstr &sqlFile )
{
	int i = 0;
	char *p;

	for ( i = 1; i < argc; ++i )
	{
		if ( 0 == strcmp( argv[i], "-u" ) ) {
			if ( (i+1) <= (argc-1) ) {
				username = argv[i+1];
			} 
		} else if ( 0 == strcmp( argv[i], "-p"  )  ) {
			if ( (i+1) <= (argc-1) && *argv[i+1] != '-' ) {
				passwd = argv[i+1];
			} 
		} else if ( 0 == strcmp( argv[i], "-d"  )  ) {
			if ( (i+1) <= (argc-1) ) {
				dbname = argv[i+1];
			} 
		} else if ( 0 == strcmp( argv[i], "-h"  )  ) {
			if ( (i+1) <= (argc-1) ) {
				if ( NULL != (p=strchr( argv[i+1], ':')) ) {
					if ( argv[i+1][0] == ':' ) {
						++p;
						if ( *p != '\0' ) {
							port = p;
						}
					} else {
    					p = strtok( argv[i+1], ":");
    					host = p;
    					p = strtok( NULL,  ":");
    					if ( p ) {
    						port = p;
    					}
					}
				} else {
					host = argv[i+1];
				}
			}
		} else if ( 0 == strcmp( argv[i], "-e"  )  ) {
			if ( (i+1) <= (argc-1) ) {
				sqlcmd = argv[i+1];
			} 
		} else if ( 0 == strcmp( argv[i], "-m"  )  ) {
			if ( (i+1) <= (argc-1) ) {
				mcmd = argv[i+1];
			} 
		} else if ( 0 == strcmp( argv[i], "-x"  )  ) {
			if ( (i+1) <= (argc-1) ) {
				exclusive = argv[i+1];
			} 
		} else if ( 0 == strcmp( argv[i], "-cx"  )  ) {
			if ( (i+1) <= (argc-1) ) {
				clearex = argv[i+1];
			} 
		} else if ( 0 == strcmp( argv[i], "-a"  )  ) {
			if ( (i+1) <= (argc-1) ) {
				fullconnect = argv[i+1];
			} 
		} else if ( 0 == strcmp( argv[i], "-f"  )  ) {
			if ( (i+1) <= (argc-1) ) {
				sqlFile = argv[i+1];
			} 
		} else if ( 0 == strcmp( argv[i], "-v"  )  ) {
			echo = 1;
		} else if ( 0 == strcmp( argv[i], "-vv"  )  ) {
			vvdebug = "yes";
		} else if ( 0 == strcmp( argv[i], "-q"  )  ) {
			quiet = "yes";
		} else if ( 0 == strcmp( argv[i], "-n"  )  ) {
			// keepNewline = "no";
			keepNewline = "yes";
		}
	}
}

// 1: OK
// 0: error
int queryAndReply( JaguarCPPClient &jcli, const char *query, bool ishello ) 
{
	const char *p;
    int rc = jcli.query( query );
    if ( ! rc ) {
        printit(jcli._outf, "E3020 Error query [%s]\n", query );
		return 0;
    }
    
    while ( jcli.reply() ) {
		p = jcli.getMessage();
		if ( ! p ) continue;
		if ( ishello ) {
       		printit(jcli._outf, "%s / Client %s\n\n", p, jcli._version.c_str() );
			JagStrSplit sp(p, ' ', true );
			if ( sp.length() < 3 || sp[2] != jcli._version ) {
				printit( jcli._outf, "Jaguar server version and client version do not match, exit\n");
				jcli.close();
				exit ( 1 );
			} 
		} else {
       		printit(jcli._outf, "%s\n", p );
		}
    } 
	if ( jcli.hasError() ) {
		if ( jcli.error() ) {
			printit( jcli._outf, "%s\n", jcli.error() );
		}
	}
    // jcli.freeResult();
	// prt(("c3848 done queryAndReply [%s]\n", query ));

	return 1;
}

void usage( const char *prog )
{
	printf("\n");
	printf("%s [-h HOST:PORT] [-u USERNAME] [-p PASSWORD] [-d DATABASE] [-e COMMAND] [-f sqlFile] [-v FLAG] [-x FLAG] [-a FLAG] [-q] [-n]\n", prog  );
	printf("    [-h HOST:PORT]   ( IP address and port number of the server. Default: 127.0.0.1:8888 )\n");
	printf("    [-u USERNAME]    ( User name in Jaguar. If not given, uses USERNAME environment variable. Finally: test )\n");
	printf("    [-p PASSWORD]    ( Password of USERNAME. If not given, uses PASSWORD environment variable. Finally, prompted to provide )\n");
	printf("    [-d DATABASE]    ( Database name. Default: test )\n");
	printf("    [-e \"command\"]   ( Execute some command. Default: none )\n");
	printf("    [-f INPUTFILE]   ( Execute command from INPUTFILE. Default: standard input)\n");
	printf("    [-v yes/no]      ( Echo input command. Default: no )\n");
	printf("    [-x yes/no]      ( Exclusive login mode for admin. Default: no )\n");
	printf("    [-a yes/no]      ( Require successful connection to all servers. Default: yes )\n");
	printf("    [-q]             ( Quiet mode. Default: no  )\n");
	// printf("    [-n]             ( Ignore newline character in command. Default: no )\n");
	printf("    [-n]             ( Keep newline character in command. Default: no )\n");
	printf("\n");
}

void printResult( const JaguarCPPClient &jcli, const JagClock &clock, abaxint cnt )
{	
	const char *tstr;
	const char *rstr;
	abaxint rc = clock.elapsed(); // millisecs
	if ( rc < 2   ) {
		rc = clock.elapsedusec();
		tstr = "microseconds";
	} else if ( rc <= 10000 ) {
		tstr = "milliseconds";
	} else {
		rc = rc / 1000;
		tstr = "seconds";
	}

	if (  cnt >= 0 ) {
		if ( 1 == cnt ) printf("Done in %lld %s (%lld row) \n", rc, tstr, cnt );
		else  printf("Done in %lld %s (%lld rows) \n", rc, tstr, cnt );
	} else {
		printf("Done in %lld %s \n", rc, tstr );
	}

    fflush( stdout );
}

// return -1 for fatal eror
// 0: for OK
int executeCommands( JagClock &clock, const Jstr &sqlFile, JaguarCPPClient &jcli, 
                     const Jstr &quiet, int echo, int saveNewline, const Jstr &username )
{
	// prt(("c29393 executeCommands sqlFile=[%s]\n", sqlFile.c_str() ));
	char *pcmd;
	Jstr sqlcmd;
	FILE *infp = stdin;
	if ( sqlFile.size() > 0 ) {
		infp = jagfopen( sqlFile.c_str(), "rb" );
		if ( ! infp ) {
			printit( jcli._outf, "Error open [%s] for commands\n", sqlFile.c_str());
			return -1;
		}
	}

	Jstr pass1, pass2;
	Jstr passwd;
	int rc = 0;

	lastCmdIsExpect = false;
	expectCorrectRows = -1;
	expectErrorRows = -1;

	abaxint cnt;
    while ( 1 ) {
		cnt = 0;

		if ( quiet != "yes" ) {
			//printit( jcli._outf, "%s:%s> ", JAG_BRAND, jcli._dbname.c_str() ); fflush( stdout );
			fprintf(stdout, "%s:%s> ", JAG_BRAND, jcli._dbname.c_str() ); fflush( stdout );
		}

		if ( ! jcli.getSQLCommand( sqlcmd, echo, infp, saveNewline ) ) {
			break;
		}

		// prt(("c7173 sqlcmd=[%s]\n", sqlcmd.c_str() ));
		if ( sqlcmd.length() < 1 || ';' == *(sqlcmd.c_str() ) ) {
			continue;
		}

		pcmd = (char*)sqlcmd.c_str();
		while ( jagisspace(*pcmd) ) ++pcmd;
		// if ( _debug ) { prt(("c3301 cmd=[%s]\n", pcmd )); }

		if (  pcmd[0] == 'q' ) { break; } // quit
		if ( strncasecmp( pcmd, "exit", 4 ) == 0 ) { break; }

		if ( pcmd[0] == 27 ) { continue; } 
		if ( strncasecmp( pcmd, "changepass", 9 ) == 0 ) {
			Jstr tcmd1 = trimTailChar( pcmd, ';' );
			Jstr tcmd2 = trimTailChar( tcmd1, ' ' );
			const char *passtr = NULL;
			if ( ! (passtr = strchr( tcmd2.c_str(), ' ')) ) {
    			// no password is provided
    			prt(("New password: " ));
    			getPassword( pass1 );
    			prt(("New password again: " ));
    			getPassword( pass2 );
    			if ( pass1 != pass2 ) {
    				prt(("Entered passwords do not match. Please try again\n"));
    				continue;
    			}
    			sqlcmd = "changepass " + username + ":" + pass1;
    			pcmd = (char*) sqlcmd.c_str();
			} else {
				while ( isspace(*passtr) ) ++passtr;
				if ( strchr(passtr, ':') ) {
					sqlcmd = Jstr("changepass ") + passtr;
				} else {
					sqlcmd = Jstr("changepass ") + username + ":" + passtr;
				}
				pcmd = (char*) sqlcmd.c_str();
			}
		} else if ( strncasecmp( pcmd, "createuser", 9 ) == 0 && ! strchr(pcmd, ':') ) {
			if ( strchr(pcmd, ':') ) {
				JagStrSplit sp( pcmd, ':' );
				if ( sp[1].length() < 12 ) {
					prt(("Password is too short. It must have at least 12 letters. Please try again\n"));
					continue;
				}
			} else {
    			// no password is provided
    			Jstr cmd = trimTailChar( sqlcmd, ';' );
    			JagStrSplit sp( cmd, ' ', true );
    			if ( sp.length() < 2 ) {
    				prt(("E6300 Error createuser syntax. Please try again\n"));
    				continue;
    			}
    			prt(("New user password: " ));
    			getPassword( pass1 );
    			prt(("New user password again: " ));
    			getPassword( pass2 );
    			if ( pass1 != pass2 ) {
    				prt(("Entered passwords do not match. Please try again\n"));
    				continue;
    			}
    			if ( pass1.length() < 12 ) {
    				prt(("Password is too short. It must have at least 12 letters. Please try again\n"));
    				continue;
    			}
    
    			sqlcmd = "createuser " + sp[1] + ":" + pass1;
    			pcmd = (char*) sqlcmd.c_str();
			}
		} else if ( strncasecmp( pcmd, "sleep ", 6 ) == 0 ) {
			JagStrSplit sp( pcmd, ' ', true );
			jagsleep(atoi(sp[1].c_str()), JAG_SEC);
			continue;
		} else if ( strncasecmp( pcmd, "!", 1 ) == 0 ) {
			++pcmd;
			while ( isspace(*pcmd) ) ++pcmd;
			if ( *pcmd == '\r' || *pcmd == '\n' || *pcmd == ';' || *pcmd == '\0' ) continue;
			Jstr outs = psystem(pcmd);
			printit( jcli._outf, "%s\n", outs.c_str() );
			continue;
		} else if ( strncasecmp( pcmd, "source ", 7 ) == 0 ) {
			JagStrSplit sp( pcmd, ' ', true );
			if ( sp.length() < 2 ) {
				printit( jcli._outf, "Error: a command file must be provided.\n");
			} else {
				rc = executeCommands( clock, trimTailChar(sp[1], ';'), jcli, quiet, echo, saveNewline, username );
				if ( rc < 0 ) {
					printit( jcli._outf, "Execution error in file %s\n", sp[1].c_str() ); 
				}
			}
			continue;
		} else if ( strncasecmp( pcmd, "@", 1 ) == 0 ) {
			if ( strlen(pcmd) >= 3 ) {
				rc = executeCommands( clock, trimTailChar(pcmd+1, ';'), jcli, quiet, echo, saveNewline, username );
				if ( rc < 0 ) {
					printit( jcli._outf, "Execution error in file %s\n", pcmd+1 ); 
				}
			}
			continue;
		} else if ( strncasecmp( pcmd, "expect", 6 ) == 0 ) {
			// expect correct rows 12
			// expect error rows 1
			JagStrSplit sp( pcmd, ' ', true );
			if ( sp.length() < 4 ) {
				printit( jcli._outf, "Error: expect correct/error rows N\n"); 
			} else {
				if ( sp[2].caseEqual("rows") ) {
					if ( sp[1].caseEqual("correct") ) {
						expectCorrectRows = jagatoi( sp[3].c_str() );
						if ( expectCorrectRows < 0 ) {
							printit( jcli._outf, "Error: rows N must be >= 0\n"); 
							continue;
						}

					} else {
						expectErrorRows = jagatoi( sp[3].c_str() );
						if ( expectErrorRows < 0 ) {
							printit( jcli._outf, "Error: rows N must be >= 0\n"); 
							continue;
						}
					}
					lastCmdIsExpect = true;
				}
			}
			continue;
		}

		clock.start();	
        rc = jcli.query( pcmd );
        if ( ! rc ) {
			if ( jcli.hasError() ) {
				printit( jcli._outf, "%s\n", jcli.error() );
			}

			if ( lastCmdIsExpect && expectErrorRows == 1 ) {
				printit( jcli._outf, "TESTRESULT: good (expect error=1, query error=1)\n" );
			}

			lastCmdIsExpect = false;
			expectCorrectRows = -1;
			expectErrorRows = -1;
			continue;
        }
		
		int numOK = 0;
		int numError = 0;
        while ( jcli.reply() ) {
			jcli.printRow();
			if ( ! jcli.hasError() ) {
				++numOK; ++cnt;
			} else {
				++numError;
			}

			jcli.printAll();
        } 

		while ( jcli.printAll() ) { ++numOK; }
		jcli.flush();

		if ( lastCmdIsExpect ) {
			if ( expectErrorRows >= 0 ) {
				if ( expectErrorRows == numError ) {
					printit( jcli._outf, "TESTRESULT: good(expect error=%d  reply error=%d)\n", expectErrorRows, numError );
				} else {
					printit( jcli._outf, "TESTRESULT: bad (expect error=%d  reply error=%d)\n", expectErrorRows, numError );
				}
			} else if ( expectCorrectRows >= 0 ) {
				if ( expectCorrectRows == numOK ) {
					printit( jcli._outf, "TESTRESULT: good(expect correct=%d  reply correct=%d)\n", expectCorrectRows, numOK );
				} else {
					printit( jcli._outf, "TESTRESULT: bad (expect correct=%d  reply correct=%d)\n", expectCorrectRows, numOK );
				}
			}
		}


		lastCmdIsExpect = false;
		expectCorrectRows = -1;
		expectErrorRows = -1;

		// if ( jcli.hasError() ) { printf("%s\n", jcli.error() ); }
		if ( strncasecmp( pcmd, "load", 4 ) == 0 ) {
			printit( jcli._outf, "%s\n", jcli.status() );
		}

        jcli.freeResult();
		clock.stop();
		rc = clock.elapsed();

		if ( quiet == "yes" ) {
			// nothing done
		} else {
			if ( jcli._queryCode == JAG_SELECT_OP || isJoin( jcli._queryCode ) ) {
				printResult( jcli, clock, cnt );
			} else {
				printResult( jcli, clock, -1 );
			}
		}
    }

	if ( infp ) jagfclose( infp );

}

void printit( FILE *outf, const char *format, ...)
{
	FILE *out = outf;
	if ( ! out ) out = stdout;

	va_list args;
	va_start(args, format);
	vfprintf(out, format, args );
	fflush( out );
	va_end( args);
}

