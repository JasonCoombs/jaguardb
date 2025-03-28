PHP_ARG_ENABLE(jaguarphp,
    [Whether to enable the "jaguarphp" extension],
    [  --enable-jaguarphp      Enable "jaguarphp" extension support])
 
if test $PHP_JAGUARPHP != "no"; then
    PHP_REQUIRE_CXX()
	PHP_ADD_LIBPATH( /home/yzhj/jaguar/lib, JAGUARPHP_SHARED_LIBADD)
    PHP_SUBST(JAGUARPHP_SHARED_LIBADD)
    PHP_ADD_LIBRARY(stdc++, 1, JAGUARPHP_SHARED_LIBADD)
    PHP_ADD_LIBRARY(JaguarClient, 1, JAGUARPHP_SHARED_LIBADD)
    PHP_NEW_EXTENSION( jaguarphp, jaguarphp.cc Jaguar.cc, $ext_shared, "-O3", "", "yes" )
fi


