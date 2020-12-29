<?php
$jag = new Jaguar();

$jag->connect( "localhost", 8888, "admin", "jaguarjaguarjaguar", "test" );
$jag->query( "select * from jbench limit 10");
while ( $jag->reply() ) {
	$jag->printRow();
}

print "done select\n";

$jag->execute( "drop table phptable" );
print "done drop phptable\n";
$jag->execute( "create table phptable ( key: uid char(32), value: addr char(32), zip char(10) )" );
print "done create phptable\n";
$jag->execute( "insert into phptable values ( k1, '123 A', '94500' )" );
$jag->execute( "insert into phptable values ( k2, '123 B', '94502' )" );
$jag->execute( "insert into phptable values ( k3, '123 C', '94504' )" );
print "done insert into phptable\n";

$jag->query( "select * from phptable");
while ( $jag->reply() ) {
	$jag->printRow();

	$val = $jag->getNthValue(2);
	print "addr: [$val]\n";

	$val = $jag->getValue("zip");
	print "zip: [$val]\n";
}

if ( $jag->hasError() ) {
	$err = $jag->error();
	print( "Error: [$err]\n");
}

$jag->execute("drop table phptable");

$jag->close();

?>

