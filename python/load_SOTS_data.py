import sqlite3
import datetime
from netCDF4 import Dataset
import requests
import pandas as pd
#import shutil as shutil

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

fn = 'Pangaea_wanted_variables_2.csv'
var_interp = pd.read_csv(fn, encoding="ISO-8859-1")
filename = 'conversion_factors.csv'
var_dict = pd.read_csv(filename).to_dict()

#dbname = r"C:\Users\wyn028\OneDrive - CSIRO\Manuscripts\Particle_flux_database\DATA_Mining\sediment_data.sqlite\sediment_data.sqlite"
#dbname = "data/sediment_data.sqlite"
dbname = "test_sed_data.sqlite"

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

    # open netCDF file to retrieve the data set
    ds = Dataset(filename,'r', format="NETCDF4")
#    ds = Dataset("data/" + filename,'r', format="NETCDF4")
    ds.set_auto_mask(False)

    print('dataset title', uri, ds.title)
    print('file_id', file_id)

    try:
        parameters = len(ds.variables)
        samples = len(ds.variables["sample"])
        minTime = ds.time_deployment_start
        maxTime = ds.time_deployment_end
        meanDepth = ds.geospatial_vertical_max
        # cur.execute('UPDATE file SET doi=?, cite=?, number_params=?, number_samples=?, mintimeextent=?, maxtimeextent=? WHERE file_id = ?', (uri, ds.citation, parameters, samples, minTime, maxTime, file_id))
    # except sqlite3.IntegrityError:
    #     # cur.close()
    #     print("skipping, non-unique", uri)
    #     continue

    # try:
        lat = ds.geospatial_lat_max
        lon = ds.geospatial_lon_max
        cur.execute('UPDATE file SET doi=?, cite=?, number_params=?, number_samples=?, mintimeextent=?, maxtimeextent=?, '
                    'meanLatitude=?, meanLongitude=?, meandepth=? WHERE file_id = ?', (uri, ds.citation,
                    parameters, samples, minTime, maxTime, lat, lon, meanDepth, file_id))
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

    cur = con.cursor()

# dimensions:
# 	TIME = 21 ;
# 	bnds = 2 ;
# variables:
# 	double TIME(TIME) ;
# 		TIME:name = "time" ;
# 		TIME:standard_name = "time" ;
# 		TIME:long_name = "time of sample midpoint" ;
# 		TIME:units = "days since 1950-01-01T00:00:00 UTC" ;
# 		TIME:axis = "T" ;
# 		TIME:valid_min = 10957. ;
# 		TIME:valid_max = 54787. ;
# 		TIME:calendar = "gregorian" ;
# 		TIME:ancillary_variables = "TIME_bnds" ;
# 	double TIME_bnds(TIME, bnds) ;
# 		TIME_bnds:name = "time open, closed" ;
# 		TIME_bnds:long_name = "time sample open, closed" ;
# 		TIME_bnds:units = "days since 1950-01-01T00:00:00 UTC" ;
# 		TIME_bnds:axis = "T" ;
# 		TIME_bnds:valid_min = 10957. ;
# 		TIME_bnds:valid_max = 54787. ;
# 		TIME_bnds:calendar = "gregorian" ;
# 	double NOMINAL_DEPTH ;
# 		NOMINAL_DEPTH:axis = "Z" ;
# 		NOMINAL_DEPTH:long_name = "nominal depth" ;
# 		NOMINAL_DEPTH:positive = "down" ;
# 		NOMINAL_DEPTH:reference_datum = "sea surface" ;
# 		NOMINAL_DEPTH:standard_name = "depth" ;
# 		NOMINAL_DEPTH:units = "m" ;
# 		NOMINAL_DEPTH:valid_max = 12000. ;
# 		NOMINAL_DEPTH:valid_min = -5. ;
# 	double LATITUDE ;
# 		LATITUDE:standard_name = "latitude" ;
# 		LATITUDE:long_name = "latitude of anchor" ;
# 		LATITUDE:units = "degrees_north" ;
# 		LATITUDE:axis = "Y" ;
# 		LATITUDE:valid_min = -90. ;
# 		LATITUDE:valid_max = 90. ;
# 		LATITUDE:reference_datum = "WGS84 coordinate reference system" ;
# 		LATITUDE:coordinate_reference_frame = "urn:ogc:crs:EPSG::4326" ;
# 	double LONGITUDE ;
# 		LONGITUDE:standard_name = "longitude" ;
# 		LONGITUDE:long_name = "longitude of anchor" ;
# 		LONGITUDE:units = "degrees_east" ;
# 		LONGITUDE:axis = "X" ;
# 		LONGITUDE:valid_min = -180. ;
# 		LONGITUDE:valid_max = 180. ;
# 		LONGITUDE:reference_datum = "WGS84 coordinate reference system" ;
# 		LONGITUDE:coordinate_reference_frame = "urn:ogc:crs:EPSG::4326" ;
# 	float pressure_actual(TIME) ;
# 		pressure_actual:_FillValue = NaNf ;
# 		pressure_actual:long_name = "actual pressure" ;
# 		pressure_actual:units = "dbar" ;
# 		pressure_actual:uncertainty = "3" ;
# 		pressure_actual:comment = "actual" ;
# 		pressure_actual:comment_method = "pressure from nearest instrument on mooring, extrapolated to trap position" ;
# 		pressure_actual:valid_min = -2.f ;
# 		pressure_actual:valid_max = 12000.f ;
# 		pressure_actual:coordinates = "TIME LATITUDE LONGITUDE NOMINAL_DEPTH" ;
# 	float sample(TIME) ;
# 		sample:_FillValue = NaNf ;
# 		sample:long_name = "sample number" ;
# 		sample:units = "1" ;
# 		sample:coordinates = "TIME LATITUDE LONGITUDE NOMINAL_DEPTH" ;
# 		sample:ancillary_variables = "sample_quality_control" ;

    # load variables into variables and file-variables
    for var_name in ds.variables.keys():
        print("variable name", var_name)
        p = ds.variables[var_name]

        var_names = [var_name]
        if var_name == "TIME_bnds":
            var_names = ["TIME_OPEN", "TIME_CLOSE"]

        for name in var_names:
            output = var_interp.column_name[var_interp.name == name]
            output_name = output_name.values[0]
            if output_name != 'NA':
                res = cur.execute("SELECT var_id FROM variables WHERE name = ?", (name,))
                var_id_n = res.fetchone()

                if var_id_n is None:
                    # variables simply the variable name to var-id map
                    cur.execute('INSERT OR IGNORE INTO variables (name, output_name) VALUES (?,?)', (name, output_name.values[0]))

                res = cur.execute("SELECT var_id FROM variables WHERE name = ?", (name,))
                var_id = res.fetchone()[0]

                print(var_id, 'variable', name, p.long_name, p.dtype)
                try:
                    units = p.units
                except AttributeError:
                    units = 'NA'

                print(var_id, 'variable', name, 'unit', units)

                try:
                    comment = p.comment
                except AttributeError:
                    comment = 'NA'

                print(var_id, 'variable', name, 'comments', comment)

                # save the variable metadata to the file_variables table
                cur.execute('INSERT INTO file_variables (file_id, var_id, name, type, units, comment, output_name) '
                            'VALUES (?,?,?,?,?,?,?)', (file_id, var_id, p.long_name, p.dtype.name, units, comment,
                                                       output_name.values[0]))

                # load the variable data
                i = 0
                try:
                    for m in range(len(ds.variables["TIME"])):
                        # load the data
                        d = ds.variables[var_name][:]
                        data_idx = m
                        if not p.shape:  # for latitude, longitude, nominal_depth
                            d = ds.variables[var_name]
                            data_idx = 0

                        if name == "TIME_OPEN":
                            cur.execute('INSERT INTO data (file_id, sample_id, var_id, value) VALUES (?, ?, ?, ?)',(file_id, i, var_id, str(d[data_idx][0])))
                        elif name == "TIME_CLOSE":
                            cur.execute('INSERT INTO data (file_id, sample_id, var_id, value) VALUES (?, ?, ?, ?)',(file_id, i, var_id, str(d[data_idx][1])))
                        else:
                            cur.execute('INSERT INTO data (file_id, sample_id, var_id, value) VALUES (?, ?, ?, ?)', (file_id, i, var_id, str(d[data_idx])))
                        i += 1

                except KeyError as e:
                    print("KeyError", e)
                    pass


    # save time when we processed this file
    cur.execute('UPDATE file SET date_loaded=? WHERE file_id = ?', (datetime.datetime.now().strftime("%Y-%m-%d, %H:%M:%S"), file_id))
    con.commit()  # only commit when entire file inserted

cur.close()
con.close()
