import sqlite3
import sys
from datetime import datetime
import os

dbname = "sediment_data.sqlite"
con = sqlite3.connect(dbname, detect_types=sqlite3.PARSE_DECLTYPES)
cur = con.cursor()

process_data = False

#for uri in sys.argv[1:]:

file1 = open(sys.argv[1], 'r')
Lines = file1.readlines()

count = 0
# Strips the newline character
for line in Lines:
    uri = line.strip()

    print('inserting dataset', uri)
    cur.execute('INSERT INTO file (source) VALUES (?)', [uri])
    con.commit()
    file_id = cur.lastrowid  # get the file_id of the row just inserted

cur.close()
con.close()
