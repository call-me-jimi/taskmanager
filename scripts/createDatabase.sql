CREATE DATABASE IF NOT EXISTS taskdispatcherdb;

GRANT USAGE ON *.* TO tdadmin@localhost identified BY 'tAskd1sPatcher';

GRANT ALL PRIVILEGES ON taskdispatcherdb.* TO tdadmin@localhost;
