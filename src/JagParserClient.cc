
#include <JagGlobalDef.h>
#include "JagParser.h"

const JagColumn* JagParser::getColumn( const Jstr &db, const Jstr &objname, const Jstr &colName ) const
{
	if ( ! _isCli ) {
        prt(("cl088838 fatal error getColumn JagParserClient.cc\n"));
		exit(32);
	}
	const JaguarCPPClient *cli = (const JaguarCPPClient*)_obj;

    if ( cli ) {
        JagHashMap<AbaxString, JagTableOrIndexAttrs> *schemaMap = cli->_schemaMap;
        if ( ! schemaMap ) {
            //prt(("s939 tmp9999 no schemaMap\n"));
            return NULL;
        }
         bool rc2;
         AbaxString dbobj = AbaxString( db ) + "." + objname;
         JagTableOrIndexAttrs& objAttr = schemaMap->getValue( dbobj, rc2 );
         if ( ! rc2 ) {
            //prt(("s939 tmp9999 schemaMap->getValue(%s) no value\n", dbobj.c_str() ));
            return NULL;
         }

         // objAttr.schAttr[i]
         int pos =  objAttr.schemaRecord.getPosition( colName );
         if ( pos < 0 ) {
            //prt(("s8049 objAttr.schemaRecord.getPosition(%s) pos=%d\n", colName.c_str(), pos ));
            return NULL;
         }
         //prt(("s8049 objAttr.schemaRecord.getPosition(%s) pos=%d\n", colName.c_str(), pos ));

         return &(*objAttr.schemaRecord.columnVector)[pos];
    }

    return NULL; // rc is false
}

