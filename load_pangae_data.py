import sqlite3
import sys
from datetime import datetime
import os

import pangaeapy.pandataset as pd

dbname = "sediment_data.sqlite"
con = sqlite3.connect(dbname, detect_types=sqlite3.PARSE_DECLTYPES)
cur = con.cursor()

# select files from file table which are from PANGAEA and have not been processed
files_to_load_res = cur.execute("SELECT file_id, source FROM file WHERE source LIKE 'doi:10.1594/PANGAEA%' AND date_loaded IS NULL")
files_to_load = files_to_load_res.fetchall()

for fn in files_to_load:
    print()

    uri = fn[1]
    file_id = fn[0]

    # retrieve the data set
    ds = pd.PanDataSet(uri, enable_cache=True, cache_expiry_days=100)

    print('dataset title', uri, ds.title)
    print('file_id', file_id)

    try:
        parameters = len(ds.parameters)
        samples = len(ds.parameters.values())
        minTime = ds.mintimeextent
        maxTime = ds.maxtimeextent
        cur.execute('UPDATE file SET doi=?, cite=?, number_params=?, number_samples=?, mintimeextent=?, maxtimeextent=? WHERE file_id = ?', (ds.doi, ds.citation, parameters, samples, minTime, maxTime, file_id))
    except sqlite3.IntegrityError:
        # cur.close()
        print("skipping, non-unique", ds.doi)
        continue

    try:
        lat = ds.geometryextent['meanLatitude']
        lon = ds.geometryextent['meanLongitude']
        cur.execute('UPDATE file SET meanLatitude=?, meanLongitude=? WHERE file_id = ?', (lat, lon, file_id))
    except sqlite3.IntegrityError:
        # cur.close()
        print("skipping, non-unique", ds.doi)
        cur.execute('DELETE FROM file WHERE file_id = ?', (file_id,))
        continue
    except KeyError:
        pass

    # need to add in when the data was cached
    try:
        date_downloaded = datetime.fromtimestamp(os.path.getmtime(ds.get_pickle_path()))
        print('cache path', ds.get_pickle_path())
        cur.execute('INSERT INTO file_metadata (file_id, name, value) VALUES (?,?,?)', (file_id, 'date_downloaded', date_downloaded.strftime("%Y-%m-%d, %H:%M:%S")))
    except FileNotFoundError:
        pass

    #print('ds-dir', dir(ds))
    for md in dir(ds):
        if not md.startswith("_"):
            #print('metadata', md, type(getattr(ds, md)))
            if isinstance(getattr(ds, md), str):
                cur.execute('INSERT INTO file_metadata (file_id, name, value) VALUES (?,?,?)', (file_id, md, getattr(ds, md)))

    print('file-geometry', ds.geometryextent)
    for md in ds.geometryextent:
        #print('metadata-geom', md, ds.geometryextent[md])
        cur.execute('INSERT INTO file_metadata (file_id, name, value) VALUES (?,?,?)', (file_id, 'geometry-'+md, ds.geometryextent[md]))

    # add event metadata
    en = 1
    for e in ds.events:
        #print('event - dir', dir(e))
        for md in dir(e):
            if not md.startswith("_"):
                print('event', md, type(getattr(e, md)))
                if isinstance(getattr(e, md), (float, str)):
                    cur.execute('INSERT INTO file_metadata (file_id, name, value) VALUES (?,?,?)', (file_id, 'event-'+str(en)+'-'+md, getattr(e, md)))
        en += 1

    print('file_id events', len(ds.events))

    # try:
    #     cur.execute('INSERT INTO file_metadata (file_id, name, value) VALUES (?,?,?)',[file_id, 'method', ds.events[0].device])
    #     con.commit()
    # except IndexError:
    #     print('No events in', uri)

    cur = con.cursor()

    # load variables into variables and file-variables
    for var_name in ds.parameters:
        p = ds.parameters[var_name]

        # variables simply the variable name to var-id map
        cur.execute('INSERT OR IGNORE INTO variables (name) VALUES (?)', (var_name,))

        res = cur.execute("SELECT var_id FROM variables WHERE name = ?", (var_name,))
        var_id = res.fetchone()[0]

        print(var_id, 'variable', var_name, p.shortName)

        # save the variable metadata to the file_variables table
        cur.execute('INSERT INTO file_variables (file_id, var_id, name, type, units, comment) VALUES (?,?,?,?,?,?)', (file_id, var_id, p.shortName, p.type, p.unit, p.comment))

        # load the variable data
        i = 0
        try:
            d = ds.data[var_name].values
            for m in range(len(ds.data[var_name])):
                # load the data where its not unknown or nan
                if str(ds.data[var_name][m]) != 'nan' and str(ds.data[var_name][m]) != 'unknown':
                    #print('variable', var_name, ds.data[var_name][m])
                    cur.execute('INSERT INTO data (file_id, sample_id, var_id, value) VALUES (?, ?, ?, ?)',(file_id, i, var_id, str(d[m])))
                i += 1
        except KeyError:
            pass

    # save time when we processed this file
    cur.execute('UPDATE file SET date_loaded=? WHERE file_id = ?', (datetime.now().strftime("%Y-%m-%d, %H:%M:%S"), file_id))
    con.commit()  # only commit when entire file inserted

cur.close()
con.close()
