import sqlite3
import sys
from datetime import datetime
import os

import pangaeapy.pandataset as pd

dbname = "sediment_data.sqlite"
con = sqlite3.connect(dbname, detect_types=sqlite3.PARSE_DECLTYPES)
cur = con.cursor()

#for uri in sys.argv[1:]:

file1 = open(sys.argv[1], 'r')
Lines = file1.readlines()

count = 0
# Strips the newline character
for line in Lines:
    uri = line.strip()
    print('opening dataset', uri)

    ds = pd.PanDataSet(uri, enable_cache=True, cache_expiry_days=100)

    print('dataset title', ds.title)

    try:
        parameters = len(ds.parameters)
        samples = len(ds.parameters.values())
        minTime = ds.mintimeextent
        maxTime = ds.maxtimeextent
        cur.execute('INSERT INTO file (doi, cite, source, number_params, number_samples, mintimeextent, maxtimeextent) VALUES (?, ?, ?, ?, ?, ?, ?)', [ds.doi, ds.citation, uri, parameters, samples, minTime, maxTime])
        con.commit()
    except sqlite3.IntegrityError:
        # cur.close()
        print("skipping, non-unique", ds.doi)
        continue

    file_id = cur.lastrowid  # get the file_id of the row just inserted
    print('file_id', file_id)
    print('file_id events', len(ds.events))
    try:
        lat = ds.geometryextent['meanLatitude']
        lon = ds.geometryextent['meanLongitude']
        cur.execute('UPDATE file SET meanLatitude=?, meanLongitude=? WHERE file_id = ?', [lat, lon, file_id])
        con.commit()
    except sqlite3.IntegrityError:
        # cur.close()
        print("skipping, non-unique", ds.doi)
        cur.execute('DELETE FROM file WHERE file_id = ?', [file_id])
        con.commit()
        continue
    except KeyError:
        pass

    # need to add in when the data was cached
    try:
        date_downloaded = datetime.fromtimestamp(os.path.getmtime(ds.get_pickle_path()))
        print('cache path', ds.get_pickle_path())
        cur.execute('INSERT INTO file_metadata (file_id, name, value) VALUES (?,?,?)', [file_id, 'date_downloaded', date_downloaded])
        con.commit()
    except FileNotFoundError:
        pass

    #print('ds-dir', dir(ds))
    for md in dir(ds):
        if not md.startswith("_"):
            print('metadata', md, type(getattr(ds, md)))
            if isinstance(getattr(ds, md), str):
                cur.execute('INSERT INTO file_metadata (file_id, name, value) VALUES (?,?,?)', [file_id, md, getattr(ds, md)])
    con.commit()

    print('file-geometry', ds.geometryextent)
    for md in ds.geometryextent:
        #print('metadata-geom', md, ds.geometryextent[md])
        cur.execute('INSERT INTO file_metadata (file_id, name, value) VALUES (?,?,?)', [file_id, 'geometry-'+md, ds.geometryextent[md]])
    con.commit()

    en = 1
    for e in ds.events:
        #print('event - dir', dir(e))
        for md in dir(e):
            if not md.startswith("_"):
                print('event', md, type(getattr(e, md)))
                if isinstance(getattr(e, md), (float, str)):
                    cur.execute('INSERT INTO file_metadata (file_id, name, value) VALUES (?,?,?)', [file_id, 'event-'+str(en)+'-'+md, getattr(e, md)])
        en += 1
    con.commit()

    try:
        cur.execute('INSERT INTO file_metadata (file_id, name, value) VALUES (?,?,?)',[file_id, 'method', ds.events[0].device])
        con.commit()
    except IndexError:
        print('No events in', uri)

    cur = con.cursor()

    # load variables
    for var_name in ds.parameters:
        p = ds.parameters[var_name]
        cur.execute('INSERT OR IGNORE INTO variables (name) VALUES (?)', [var_name])

        res = cur.execute("SELECT var_id FROM variables WHERE name = ?", [var_name])
        var_id = res.fetchone()[0]

        print(var_id, 'variable', var_name, p.shortName)

        cur.execute('INSERT INTO file_variables (file_id, var_id, name, type, units, comment) VALUES (?,?,?,?,?,?)', [file_id, var_id, p.shortName, p.type, p.unit, p.comment])

        i = 0
        d = ds.data[var_name].values
        for m in range(len(ds.data[var_name])):
            if str(ds.data[var_name][m]) != 'nan' and str(ds.data[var_name][m]) != 'unknown':
                #print('variable', var_name, ds.data[var_name][m])
                cur.execute('INSERT INTO data (file_id, sample_id, var_id, value) VALUES (?, ?, ?, ?)',[file_id, i, var_id, str(d[m])])
            i += 1

    con.commit()

cur.close()
con.close()
