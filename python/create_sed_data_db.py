# Create a sql database to which I can add the Mouw data and our SOTS data and play with it before touching the 17GB Pangaea download

import sqlite3
import pandas as pd
import os
from datetime import datetime


#dbname = "test_sed_data.sqlite"
dbname = "test.sqlite"

con = sqlite3.connect(dbname, detect_types=sqlite3.PARSE_DECLTYPES)

cur = con.cursor()
cur.execute("CREATE TABLE IF NOT EXISTS file (file_id integer primary key autoincrement, citation TEXT, doi TEXT, source TEXT, "
            "date_loaded REAL, mintimeextent REAL, maxtimeextent REAL, number_params integer, number_samples integer, "
            "meanLatitude REAL, meanLongitude REAL, meandepth REAL)")
con.commit()
cur.execute("CREATE TABLE IF NOT EXISTS file_metadata (file_id, name TEXT, value TEXT, FOREIGN KEY(file_id) REFERENCES file(file_id))")
con.commit()

cur.execute("CREATE TABLE IF NOT EXISTS variables (var_id integer primary key autoincrement, name TEXT, output_name TEXT)")
con.commit()

cur.execute("CREATE TABLE IF NOT EXISTS file_variables (var_id integer, file_id integer, name TEXT, type TEXT, "
            "units TEXT, comment TEXT, output_name TEXT, CONSTRAINT FK_file_variables_file_id FOREIGN KEY(file_id) "
            "REFERENCES file(file_id) CONSTRAINT FK_file_variables_var_id FOREIGN KEY(var_id) REFERENCES variables(var_id))")
con.commit()

cur.execute("CREATE TABLE IF NOT EXISTS data (file_id integer, var_id integer, sample_id integer,  value TEXT, "
            "CONSTRAINT FK_data_var_id FOREIGN KEY(var_id) REFERENCES variables(var_id) "
            "CONSTRAINT FK_data_file_id FOREIGN KEY(file_id) REFERENCES file(file_id))")
con.commit()

cur.execute("CREATE INDEX data_file_id_IDX ON 'data' (file_id)")
con.commit()

cur.execute("CREATE INDEX data_var_id_IDX ON 'data' (var_id)")
con.commit()

cur.execute("CREATE INDEX data_sample_IDX ON 'data' (sample_id)")
con.commit()

cur.execute("UPDATE SQLITE_SEQUENCE SET seq = 100000 WHERE name = 'file'")
con.commit()

cur.execute("UPDATE SQLITE_SEQUENCE SET seq = 1000000 WHERE name = 'variables'")
con.commit()

cur.execute("PRAGMA foreign_keys = ON")
con.commit()