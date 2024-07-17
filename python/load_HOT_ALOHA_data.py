# to parse HOTA_ALOHA datafiles to pangaea_sqlite database


import sqlite3
import pandas as pd
import numpy as np
import os

#dbname = r'test.sqlite'
dbname = r'part_flux_data.sqlite'
con = sqlite3.connect(dbname, detect_types=sqlite3.PARSE_DECLTYPES)
cur = con.cursor()
fn = r'Pangaea_wanted_variables_2.csv'
var_interp = pd.read_csv(fn, encoding="ISO-8859-1")


# put all the .txt files into one panda frame first
file_folder = r'C:\Users\wyn028\OneDrive - CSIRO\Manuscripts\Particle_flux_database\HOT_ALOHA'
f = os.listdir(file_folder)

column_names = ['Cruise','date_start','start_hour','date_end','end_hour', 'Depth','Treatment','Carbon_flux','Carbon_flux_SD','Carbon_flux_n','Nitrogen_flux','Nitrogen_flux_SD','Nitrogen_flux_n',
            'Phosphorus_flux','Phosphorus_flux_SD','Phosphorus_flux_n','Total_mass_flux','Total_mass_flux_SD','Total_mass_flux_n','Silica_flux','Silica_flux_SD','Silica_flux_n',
            'Delta_15N_flux','Delta_15N_flux_SD','Delta_15N_flux_n','Delta_13C_flux','Delta_13C_flux_SD','Delta_13C_flux_n','Particulate_inorganic_carbon_flux','Particulate_inorganic_carbon_flux_SD','Particulate_inorganic_carbon_flux_n']

data = pd.DataFrame()
for i in f:
    if i.endswith('.txt'):
        fn = file_folder + '/' + i
        print(fn)
        df = pd.read_csv(fn, sep='\s+', on_bad_lines='skip', skip_blank_lines=True, skiprows=6, header=None)
        df.columns = column_names
        # missing values are denoted by -9
        df = df.replace(-9, np.NaN)
        df['date_start'] = pd.to_datetime(df['date_start'], format='%Y%m%d')
        df['date_end'] = pd.to_datetime(df['date_end'], format='%Y%m%d')
        data = pd.concat([data, df])

# according to https://hahana.soest.hawaii.edu/hot/reports/rep_y33.pdf, couldn't find more specifics for each deployment
data['latitude'] = 22.45
data['longitude'] = -158
mean_depth = data['Depth'].mean()
mean_lat = data['latitude'].mean()
mean_lon = data['longitude'].mean()
minext = data['date_start'].min().strftime('%Y-%m-%d')
maxext = data['date_start'].max().strftime('%Y-%m-%d')
parameters = data.columns
num_parameters = data.shape[1]
num_samples = data.shape[0]

# the relevant filenames
metadata_filename = r'C:\Users\wyn028\OneDrive - CSIRO\Manuscripts\Particle_flux_database\HOT_ALOHA\metadata\README.txt'
unit_metadata_fn = r'C:\Users\wyn028\OneDrive - CSIRO\Manuscripts\Particle_flux_database\HOT_ALOHA\metadata\HOT_units.csv'
# read the csv file

# read the metadata from the README.txt file I created
metadata_keys = ("URI", "date_downloaded", "Citation", "Abstract", "Method", "Title", "DOI", "Description")
metadata = dict.fromkeys(metadata_keys)
for m in metadata_keys:
    if m == "Method" or m == "Description" or m == "Citation":
        with open(metadata_filename, 'r', encoding='utf8') as f:
            content = f.read().splitlines()
            index = []
            index = [x for x in range(len(content)) if m in content[x]]
            subtext = []
            for i in range(index[0], index[-1]):
                subtext.append(content[i])
            p = " ".join(subtext).split(":")
            p.pop(0)
            metadata[m] = " ".join(p)
            print(m, ": ", " ".join(p))

    else:
        with open(metadata_filename, 'r', encoding='utf8') as f:
            contents = f.read()
            for line in contents.splitlines():
                if line.startswith(m + ":"):
                    d = line.split(m + ":")
                    print(m, ": ", d[-1])
                    metadata[m] = d[-1]


try:
    cur.execute('INSERT INTO file (citation, doi, source, date_loaded, mintimeextent, maxtimeextent, number_params, number_samples, meanLatitude,'
                'meanLongitude, meandepth) VALUES (?,?,?,?,?,?,?,?,?,?,?)', [metadata["Citation"], metadata["DOI"], metadata["URI"], metadata["date_downloaded"],
                minext, maxext, num_parameters, num_samples, mean_lat, mean_lon, mean_depth])
    con.commit()
except sqlite3.IntegrityError:
    print("skipping, non-unique doi", metadata["DOI"])

file_id = cur.lastrowid  # get the file_id of the row just inserted
print('file_id', file_id)

cur.execute('INSERT INTO file_metadata (file_id, name, value) VALUES (?,?,?)', [file_id, 'title', metadata["Title"]])
cur.execute('INSERT INTO file_metadata (file_id, name, value) VALUES (?,?,?)', [file_id, 'abstract', metadata["Abstract"]])
cur.execute('INSERT INTO file_metadata (file_id, name, value) VALUES (?,?,?)', [file_id, 'method', metadata["Method"]])
cur.execute('INSERT INTO file_metadata (file_id, name, value) VALUES (?,?,?)', [file_id, 'description', metadata["Description"]])

con.commit()

# load units from the *_units file I created
unit_metadata = pd.read_csv(unit_metadata_fn, encoding='utf-8')



# load variables
for var_name in data.keys():
    print("variable name", var_name)

    res = cur.execute("SELECT var_id FROM variables WHERE name = ?", (var_name,))
    var_id_n = res.fetchone()

    if var_id_n is None:
        # variables simply the variable name to var-id map
        cur.execute('INSERT OR IGNORE INTO variables (name) VALUES (?)', (var_name,))

    res = cur.execute("SELECT var_id FROM variables WHERE name = ?", (var_name,))
    var_id = res.fetchone()[0]

    print(var_id, 'variable', var_name, data[var_name].dtype.name)
    try:
        units = unit_metadata['Supplied Units'][unit_metadata['Supplied Name'] == var_name].values[0]
    except KeyError:
        units = 'NA'

    print(var_id, 'variable', var_name, 'unit', units)

    try:
        comment = unit_metadata['Supplied description'][unit_metadata['Supplied Name'] == var_name].values[0]
    except KeyError:
        comment = 'NA'

    print(var_id, 'variable', var_name, 'comments', comment)

    # save the variable metadata to the file_variables table
    cur.execute('INSERT INTO file_variables (file_id, var_id, name, type, units, comment) '
                'VALUES (?,?,?,?,?,?)', (file_id, var_id, var_name, data[var_name].dtype.name, units, comment))


    i = 0
    try:
        for m in range(len(data)):
            # load the data
            d = data[var_name].values
            data_idx = m
            cur.execute('INSERT INTO data (file_id, sample_id, var_id, value) VALUES (?, ?, ?, ?)',
                            (file_id, i, var_id, str(d[data_idx])))
            i += 1

    except KeyError as e:
        print("KeyError", e)
        pass

    con.commit()  # only commit when entire file inserted

cur.close()
con.close()
