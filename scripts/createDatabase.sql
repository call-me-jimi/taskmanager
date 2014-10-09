CREATE DATABASE IF NOT EXISTS taskmanagerdb;

GRANT USAGE ON *.* TO tmadmin@'%' identified BY 'tAskd1sPatcher';

GRANT ALL PRIVILEGES ON taskmanagerdb.* TO tmadmin@'%';
