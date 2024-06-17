import sqlite3
import datetime
import sys
import datetime
import os
import netCDF4
from netCDF4 import Dataset
import requests
import shutil as shutil

# function to download netCDF files from the IMOS thredds server
def download_file_from_server_endpoint(server_endpoint, local_file_path):
   # Send HTTP GET request to server and attempt to receive a response
   response = requests.get(server_endpoint)
# If the HTTP GET request can be served
   if response.status_code == 200:
        # Write the file contents in the response to a file specified by local_file_path
    with open(local_file_path, 'wb') as local_file:
        for chunk in response.iter_content(chunk_size=128):
            local_file.write(chunk)



dbname = r"C:\Users\wyn028\OneDrive - CSIRO\Manuscripts\Particle_flux_database\DATA_Mining\sediment_data.sqlite\sediment_data.sqlite"
con = sqlite3.connect(dbname, detect_types=sqlite3.PARSE_DECLTYPES)
cur = con.cursor()

# select files from file table which are from SOTS and have not been processed
files_to_load_res = cur.execute("SELECT file_id, source FROM file WHERE source LIKE 'https://imos-data.s3-ap-southeast-2.amazonaws.com/IMOS/DWM/SOTS/%' AND date_loaded IS NULL")
files_to_load = files_to_load_res.fetchall()

for fn in files_to_load:
    print()

    uri = fn[1]
    file_id = fn[0]
    fnsplit = uri.split('/')
    filename = fnsplit[-1]

    # download netCDF file to retrieve the data set

    download_file_from_server_endpoint(uri, filename)
    # move the files
    dest = r"C:\Users\wyn028\OneDrive - CSIRO\Manuscripts\Particle_flux_database\SOTS\netCDF"
    shutil.copy(filename, dest)

    # open netCDF file to retrieve the data set
    ds = Dataset(filename,'r', format="NETCDF4")

    print('dataset title', uri, ds.title)
    print('file_id', file_id)

    try:
        parameters = len(ds.variables)
        samples = len(ds.variables["sample"])
        minTime = ds.time_deployment_start
        maxTime = ds.time_deployment_end
        cur.execute('UPDATE file SET doi=?, cite=?, number_params=?, number_samples=?, mintimeextent=?, maxtimeextent=? WHERE file_id = ?', (uri, ds.citation, parameters, samples, minTime, maxTime, file_id))
    except sqlite3.IntegrityError:
        # cur.close()
        print("skipping, non-unique", uri)
        continue

    try:
        lat = ds.geospatial_lat_max
        lon = ds.geospatial_lon_max
        cur.execute('UPDATE file SET meanLatitude=?, meanLongitude=? WHERE file_id = ?', (lat, lon, file_id))
    except sqlite3.IntegrityError:
        # cur.close()
        print("skipping, non-unique", uri)
        cur.execute('DELETE FROM file WHERE file_id = ?', (file_id,))
        continue
    except KeyError:
        pass

    # need to add in when the data was cached
    try:
        date_downloaded = datetime.datetime.now() # the list of urls might be outdated, but the files are downloaded when this script is run, hence date downloaded is always now
        #print('cache path', ds.get_pickle_path())
        cur.execute('INSERT INTO file_metadata (file_id, name, value) VALUES (?,?,?)', (file_id, 'date_downloaded', date_downloaded.strftime("%Y-%m-%d, %H:%M:%S")))
    except FileNotFoundError:
        pass

    #print('ds-dir', dir(ds))
    for md in dir(ds):
        if not md.startswith("_"):
            #print('metadata', md, type(getattr(ds, md)))
            if isinstance(getattr(ds, md), str) or isinstance(getattr(ds, md), float): # to include the geospatial lat and lon info
                print('metadata', md, getattr(ds, md))
                cur.execute('INSERT INTO file_metadata (file_id, name, value) VALUES (?,?,?)', (file_id, md, getattr(ds, md)))


# not needed for netCDF files since lat and lon min and max are captured by cycling through all global attributes?
    # print('file-geometry', ds.geometryextent)
    # for md in ds.geometryextent:
    #     #print('metadata-geom', md, ds.geometryextent[md])
    #     cur.execute('INSERT INTO file_metadata (file_id, name, value) VALUES (?,?,?)', (file_id, 'geometry-'+md, ds.geometryextent[md]))

# not needed since all our metadata is in the global attributes of each netCDF file, which is already captured
    # add event metadata
    # en = 1
    # for e in ds.events:
    #     #print('event - dir', dir(e))
    #     for md in dir(e):
    #         if not md.startswith("_"):
    #             print('event', md, type(getattr(e, md)))
    #             if isinstance(getattr(e, md), (float, str)):
    #                 cur.execute('INSERT INTO file_metadata (file_id, name, value) VALUES (?,?,?)', (file_id, 'event-'+str(en)+'-'+md, getattr(e, md)))
    #     en += 1
    #
    # print('file_id events', len(ds.events))

    # try:
    #     cur.execute('INSERT INTO file_metadata (file_id, name, value) VALUES (?,?,?)',[file_id, 'method', ds.events[0].device])
    #     con.commit()
    # except IndexError:
    #     print('No events in', uri)

    cur = con.cursor()

    # load variables into variables and file-variables
    for var_name in ds.variables.keys():
        print("variable name", var_name)
        p = ds.variables[var_name]

        res = cur.execute("SELECT var_id FROM variables WHERE name = ?", (var_name,))
        var_id_n = res.fetchone()

        if var_id_n is None:
            # variables simply the variable name to var-id map
            cur.execute('INSERT OR IGNORE INTO variables (name) VALUES (?)', (var_name,))

        res = cur.execute("SELECT var_id FROM variables WHERE name = ?", (var_name,))
        var_id = res.fetchone()[0]

        print(var_id, 'variable', var_name, p.name, p.dtype)
        print(var_id, 'variable', var_name, 'unit', p.units)

        try:
            comment = p.comment
        except AttributeError:
            comment = 'NA'

        # save the variable metadata to the file_variables table
        cur.execute('INSERT INTO file_variables (file_id, var_id, name, type, units, comment) VALUES (?,?,?,?,?,?)', (file_id, var_id, p.name, p.dtype.name, p.units, comment))

        # load the variable data
        i = 0
        try:
            d = ds.variables[var_name][:]
            for m in range(len(ds.variables[var_name])):
                # load the data where it's not unknown or nan
                if str(ds.variables[var_name][m]) != ds.variables[var_name]._FillValue: #and str(ds.variables[var_name][m]) != 'unknown':
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
