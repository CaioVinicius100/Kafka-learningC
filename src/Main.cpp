#include <stdio.h>	
#include <stdlib.h>	
#include <string.h>	
#include <signal.h>	
#include <iostream>	
#include <exception>	
	
#include <shc.h>	
#include <ist_otrace.h>	
#include <ist_catsig.h>	
	
#include "FmtBroker.hpp"	
	
void progExit( int n )	
{	
    syslg( "Exit due to signal %d\n", n );	
    OTraceLog( "Exit due to signal %d\n", n );	
	
    OTraceInfo( "mb_disconnect...\n" );	
    mb_disconnect();	
	
    OTraceOff();	
	
    if(n >= 2 && n < 100)	
    {	
        kill( getpid( ), n );	
    }	
	
    exit(n);	
}	
	
	
int main( int argc, char **argv )	
{	
    catch_all_signals( progExit );	
	
    try	
    {	
        std::string pathName( argv[0] );	
        std::string binName( ( ( pathName.find_last_of( "/" ) != std::string::npos ) ? ( pathName.substr(  pathName.find_last_of( "/" ) + 1 ) ) : ( pathName ) ).c_str( ) );	
	
        FmtBroker fmt;	
        fmt.setFmtName( binName.c_str() );	
	
        // Configura objetos de debug/trace/dump	
        syslg_setargv0( ( char * )binName.c_str() );	
        OTraceOn( binName.c_str() );	
        ODumpOn( binName.c_str() );	
	
        fmt.init();	
        fmt.exec();	
    }	
    catch( std::exception& e )	
    {	
        std::cerr << "Standard exception: " << e.what() << std::endl;	
	
        syslg( "Standard exception: %s\n", e.what() );	
    }	
    catch ( ... )	
	{
        std::cerr << "Generic exception" << std::endl;	
        syslg( "Generic exception\n" );	
    }	
	
    progExit(0);	
    return 0;	
}	
