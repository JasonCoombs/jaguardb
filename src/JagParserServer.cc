
#include <JagGlobalDef.h>
#include <JagDBServer.h>
#include <JagTableSchema.h>
#include <JagIndexSchema.h>
#include "JagParser.h"
#include "JagLocalDiskHash.h"

const JagColumn* JagParser::getColumn( const Jstr &db, const Jstr &objname, const Jstr &colName ) const
{
    // prt(("s7383 _srv=%0x\n", _srv ));
	if ( _isCli ) {
		prt(("s300823 fatal error getColumn() in ParserServer.cc\n" ));
		exit(31);
	}
	const JagDBServer *srv = (const JagDBServer*)_obj;

    if ( srv ) {
        // check if is indexschema
        JagIndexSchema *schema = srv->_indexschema;
        if ( schema ) {
            const JagColumn* ptr = schema->getColumn( db, objname, colName );
            if ( ptr ) return ptr;
        }

        JagTableSchema *schema2 = srv->_tableschema;
        if ( ! schema2 ) {
            return NULL;
        }
        // prt(("s2208 schema->getColumn(db=%s objname=%s colname=%s\n", db.c_str(), objname.c_str(), colName.c_str() ));
        return schema2->getColumn( db, objname, colName );
    }
    return NULL; // rc is false
}

