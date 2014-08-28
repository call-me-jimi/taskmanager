#!/bin/bash
HOST="$1"
PORT="$2"
MUSER="$3"
MPASS="$4"
MDB="$5"
 
# Detect paths
MYSQL=$(which mysql)
MYSQL=/project/P0001-1/package/usr/bin/mysql
AWK=$(which awk)
GREP=$(which grep)
 
if [ $# -ne 5 ]
then
	echo "Usage: $0 {MySQL-Host} {MySQL-Port} {MySQL-User-Name} {MySQL-User-Password} {MySQL-Database-Name}"
	echo "Drops all tables from a MySQL"
	exit 1
fi
echo $MYSQL -u$MUSER -p$MPASS -h$HOST -P$PORT $MDB
TABLES=$($MYSQL -u$MUSER -p$MPASS -h$HOST -P$PORT $MDB -e 'show tables' | $AWK '{ print $1}' | $GREP -v '^Tables' )
 
for t in $TABLES
do
	echo "Deleting $t table from $MDB database..."
	$MYSQL -h$HOST -P$PORT -u $MUSER -p$MPASS $MDB -e "drop table $t"
done
