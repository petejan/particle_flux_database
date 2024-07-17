# to parse BCO_DMO datafiles to pangaea_sqlite database
import sqlite3
import pandas as pd
import os


#dbname = r'test.sqlite'
dbname = r'part_flux_data.sqlite'
con = sqlite3.connect(dbname, detect_types=sqlite3.PARSE_DECLTYPES)
cur = con.cursor()
fn = r'Pangaea_wanted_variables_2.csv'
var_interp = pd.read_csv(fn, encoding="ISO-8859-1")


# the root directory where all the BCO_DMO files are
root = r'C:\Users\wyn028\OneDrive - CSIRO\Manuscripts\Particle_flux_database\BCO_DMO_database'
# create a list of folders
for folder in os.listdir(root):
    folderpath = os.path.join(root, folder)
    print(folderpath)

    for f in os.listdir(folderpath):
        if f.endswith('.csv') and not f.endswith('units.csv'):
            filename = os.path.join(folderpath, f)
            print('filename', filename)

        elif f.endswith('.txt'):
            metadata_filename = os.path.join(folderpath, f)
            print('metadata', metadata_filename)

        elif f.endswith('units.csv'):
            unit_metadata_fn = os.path.join(folderpath, f)
            print('unit filename', unit_metadata_fn)

    # read the csv file
    data = pd.read_csv(filename)
    parameters = data.columns
    num_parameters = data.shape[1]
    num_samples = data.shape[0]

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


    geo_vars = {'latitude', 'longitude', 'depth'}
    new_data = dict.fromkeys(geo_vars)
    for var in geo_vars:
        vars = var_interp[var_interp.column_name == var]
        print(vars)
        var_names = set(vars.name)
        print(var + ' names', str(var_names)[1:-1])

        for v in var_names:
            print('working on ', v)
            # update the processed data table db
            try:
                new_data[var] = data[v]
                print(new_data)
            except KeyError:
                continue
    try:
        min_ext_time = pd.to_datetime(data.date_start, format='mixed', dayfirst=True).min().strftime('%Y-%m-%d')
    except ValueError:
        min_ext_time = 'NaT'
    try:
        max_ext_time = pd.to_datetime(data.date_start, format='mixed', dayfirst=True).max().strftime('%Y-%m-%d')
    except ValueError:
        max_ext_time = 'NaT'
    mean_lon = new_data['longitude'].mean()
    mean_lat = new_data['latitude'].mean()
    mean_depth = new_data['depth'].mean()

    try:
        cur.execute('INSERT INTO file (citation, doi, source, date_loaded, mintimeextent, maxtimeextent, number_params, number_samples, meanLatitude,'
                    'meanLongitude, meandepth) VALUES (?,?,?,?,?,?,?,?,?,?,?)', [metadata["Citation"], metadata["DOI"], metadata["URI"], metadata["date_downloaded"],
                    min_ext_time, max_ext_time, num_parameters, num_samples, mean_lat, mean_lon, mean_depth])
        con.commit()
    except sqlite3.IntegrityError:
        print("skipping, non-unique doi", metadata["DOI"])
        continue

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
