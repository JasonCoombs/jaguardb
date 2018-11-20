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

#include <JagServer.h>
#include <JagDBServer.h>
#include <JagUtil.h>
#include <JagProduct.h>
#include <JagFileMgr.h>
#include <JagDBConnector.h>
#include <signal.h>
#include <JagCfg.h>

class JagSigPass
{
    public:
        JagDBServer   *servobj;
        #ifndef _WINDOWS64_
        sigset_t    sigset;
        #endif
};
static int setupSignalHandler( JagDBServer *serv );
static void* signalHandler( void *arg );

JagDBServer *g_servobj;


JagServer::JagServer()
{
	_raydbServ = new JagDBServer();
	g_servobj = _raydbServ;
}

JagServer::~JagServer()
{
	delete _raydbServ;
}

int JagServer::main(int argc, char *argv[] )
{
	checkLicense();
	setupSignalHandler( _raydbServ );
	int rc = _raydbServ->main( argc, argv );
	return rc;
}

void JagServer::checkLicense()
{
    return;
}


#ifndef _WINDOWS64_
int setupSignalHandler( JagDBServer *serv )
{
    sigset_t set;
    int s;

    pthread_t thread;
    sigfillset(&set);
    s = pthread_sigmask(SIG_BLOCK, &set, NULL);  // all signals passthrough
    JagSigPass *sigpass = new JagSigPass;
    sigpass->servobj = serv;
    sigpass->sigset = set;
    s = jagpthread_create(&thread, NULL, &signalHandler, (void *)sigpass);
    if (s != 0) {
        raydebug( stdout, JAG_LOG_LOW, "Error creating blocking all signals, exit\n" );
        prt(( "Error creating blocking all signals, exit\n" ));
        exit(1);
    }

    sigemptyset(&set);  // no signals can passthrough to all new threads
    s = pthread_sigmask(SIG_BLOCK, &set, NULL);
    if ( s != 0 ) {
        raydebug( stdout, JAG_LOG_LOW, "Error setting up signal mask, exit\n" );
        prt(("error setup sig mask 22333\n"));
        exit(1);
    }

    return 1;
}

void* signalHandler( void *arg )
{
    JagSigPass *p = (JagSigPass*)arg;
    sigset_t set = p->sigset;
    int s, sig;
    JagDBServer *servobj = p->servobj;

    sigfillset(&set); // all signal pass through
    s = pthread_sigmask(SIG_BLOCK, &set, NULL);
    while ( true ) {
        s = sigwait( &set,  &sig);
        if ( s != 0 ) {
            prt(("s6381 sigwait error\n"));
            continue;
        }

        if ( servobj ) {
            servobj->processSignal( sig );
        }
    }

    return NULL;
}

#else
BOOL CtrlHandler( DWORD fdwCtrlType ) ;
int setupSignalHandler( JagDBServer *serv )
{
    if( SetConsoleCtrlHandler( (PHANDLER_ROUTINE) CtrlHandler, TRUE ) ) {
        printf( "\nThe Control Handler is installed.\n" );
    }

    return 0;
}
void* signalHandler( void *arg )
{
	return NULL;
}

BOOL CtrlHandler( DWORD fdwCtrlType )
{
  switch( fdwCtrlType )
  {
    // Handle the CTRL-C signal.
    case CTRL_C_EVENT:
      Beep( 750, 300 );
	  raydebug( stdout, JAG_LOG_LOW, "Processing Ctrl-C event ignore...\n" );
	  // g_servobj->processSignal( JAG_CTRL_CLOSE );
      return( TRUE );

    // CTRL-CLOSE: confirm that the user wants to exit.
    case CTRL_CLOSE_EVENT:
      Beep( 600, 200 );
	  raydebug( stdout, JAG_LOG_LOW, "Processing Ctrl-CLOSE event JAG_CTRL_CLOSE...\n" );
	  g_servobj->processSignal( JAG_CTRL_CLOSE );
      return( TRUE );

    // Pass other signals to the next handler.
    case CTRL_BREAK_EVENT:
      Beep( 900, 200 );
	  raydebug( stdout, JAG_LOG_LOW, "Processing Ctrl-Break event ignore ...\n" );
      return FALSE;

    case CTRL_LOGOFF_EVENT:
      Beep( 1000, 200 );
	  raydebug( stdout, JAG_LOG_LOW, "Processing LogOff event ignore ...\n" );
      return FALSE;

    case CTRL_SHUTDOWN_EVENT:
      Beep( 750, 500 );
	  raydebug( stdout, JAG_LOG_LOW, "Processing ShutDown event JAG_CTRL_CLOSE...\n" );
	  g_servobj->processSignal( JAG_CTRL_CLOSE );
      return FALSE;

    default:
      Beep( 1000, 200 );
	  raydebug( stdout, JAG_LOG_LOW, "Processing unknown event ignore ...\n" );
      return FALSE;
  }
}

#endif

