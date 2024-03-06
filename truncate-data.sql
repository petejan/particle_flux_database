delete from data;
delete from file_variables ;
delete from variables ;

delete from file_metadata ;
delete from file ;

UPDATE SQLITE_SEQUENCE SET seq = 10000 WHERE name = 'file';
UPDATE SQLITE_SEQUENCE SET seq = 100000 WHERE name = 'variables';

