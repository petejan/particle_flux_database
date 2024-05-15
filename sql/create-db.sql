CREATE TABLE IF NOT EXISTS file (file_id integer primary key autoincrement, cite TEXT, doi TEXT, source TEXT, date_loaded REAL, mintimeextent REAL, maxtimeextent REAL, number_params integer, number_samples integer, meanLatitude REAL, meanLongitude REAL, CONSTRAINT file_uniq UNIQUE (meanlatitude, meanlongitude, number_params, number_samples, mintimeextent, maxtimeextent));
CREATE TABLE IF NOT EXISTS file_metadata (file_id, name TEXT, value TEXT, FOREIGN KEY(file_id) REFERENCES file(file_id));
CREATE TABLE IF NOT EXISTS variables (var_id integer primary key autoincrement, name TEXT, output_name TEXT);
CREATE TABLE IF NOT EXISTS file_variables (var_id integer, file_id integer, name TEXT, type TEXT, units TEXT, comment TEXT, output_name TEXT, CONSTRAINT FK_file_variables_file_id FOREIGN KEY(file_id) REFERENCES file(file_id) CONSTRAINT FK_file_variables_var_id FOREIGN KEY(var_id) REFERENCES variables(var_id));
CREATE TABLE IF NOT EXISTS data (file_id integer, var_id integer, sample_id integer,  value TEXT, CONSTRAINT FK_data_var_id FOREIGN KEY(var_id) REFERENCES variables(var_id) CONSTRAINT FK_data_file_id FOREIGN KEY(file_id) REFERENCES file(file_id));

CREATE INDEX data_file_id_IDX ON "data" (file_id);
CREATE INDEX data_var_id_IDX ON "data" (var_id);
CREATE INDEX data_sample_IDX ON "data" (sample_id);


UPDATE SQLITE_SEQUENCE SET seq = 100000 WHERE name = 'file';
UPDATE SQLITE_SEQUENCE SET seq = 1000000 WHERE name = 'variables';


PRAGMA foreign_keys = ON;
