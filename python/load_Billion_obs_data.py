# a script to parse the disparate .xls files from the SEANOE database Billion observatory data folder

import sqlite3
import pandas as pd
import os


## first convert all the .xls files to .csv files

#data_folder = r'C:\Users\wyn028\OneDrive - CSIRO\Manuscripts\Particle_flux_database\SEANOE data base\BILLION observatory data\66777\base donne╠ües Suivilion et Lionceau'

# files = []
# f = os.listdir(data_folder)
# for i in f:
#     if i.endswith('.xls'):
#         filename = [i]
#         files += filename
#
# for f in files:
#     filename = os.path.join(data_folder,f)
#     read_file = pd.read_excel(filename, header=None)
#     fn = f.split('.')
#     fn = fn[0] + '.csv'
#     new_filename = os.path.join(data_folder,fn)
#     # Write the dataframe object
#     # into csv file
#     read_file.to_csv(new_filename, index=None, header=False)

#dbname = r'test.sqlite'
dbname = r'part_flux_data.sqlite'

con = sqlite3.connect(dbname, detect_types=sqlite3.PARSE_DECLTYPES)
cur = con.cursor()

# the relevant filenames

data_folder = r'C:\Users\wyn028\OneDrive - CSIRO\Manuscripts\Particle_flux_database\SEANOE data base\BILLION observatory data\66777\base donne╠ües Suivilion et Lionceau'
metadata_filename = r'C:\Users\wyn028\OneDrive - CSIRO\Manuscripts\Particle_flux_database\SEANOE data base\BILLION observatory data\README.txt'
unit_metadata_fn = r'C:\Users\wyn028\OneDrive - CSIRO\Manuscripts\Particle_flux_database\SEANOE data base\BILLION observatory data\66777\Billion_obs_units.csv'
csv_files = []
f = os.listdir(data_folder)
for i in f:
    if i.endswith('.csv'):
        filename = [i]
        csv_files += filename


# first extract the metadata from the header rows and add to description section of metadata
for v in csv_files:
    fn = os.path.join(data_folder, v)
    print(fn)

    #    with open(fn, 'r', encoding='utf8') as f:
    with open(fn, 'r') as f:
        content = f.read().splitlines()
        index = []
        index = [x for x in range(len(content)) if '* ' in content[x]]
        text = []
        for c in range(len(index)):
            line = content[index[c]]
            p = line.split("* ")
            p = p[1].split(",")
            p = p[0]
            text.append(p)
            #print(p)

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
    # add the header info from every csv file to the Description in the metadata
    for i in range(len(text)):
        metadata["Description"] += text[i]

    # in this case I will make file_id the DOI_filename

    fi = metadata["DOI"] + '_' + v

    data = pd.read_csv(fn, encoding="ISO-8859-1", header=None)
    # find the row number from where the actual data starts
    df = data[data[0].str.contains('Cup#', na=False)].index
    data = pd.read_csv(fn, encoding="ISO-8859-1", skiprows=range(df[0]))

    # convert date to the right format
    data['Start'] = pd.to_datetime(data.Start, dayfirst=True)
    data['Start'].dt.strftime('%Y/%m/%d')
    mean_depth = data['Depth'].mean()
    try:
        mean_lat = data['lat_start'][0]
        mean_lon = data['lon_start'][0]
    except KeyError:
        mean_lat = data['Nominal_lat'][0]
        mean_lon = data['Nominal_lon'][0]

    minext = data['Start'].min().strftime('%Y-%m-%d')
    maxext = data['Start'].max().strftime('%Y-%m-%d')
    parameters = data.columns
    num_parameters = data.shape[1]
    num_samples = data.shape[0]

    try:
        cur.execute(
            'INSERT INTO file (citation, doi, source, date_loaded, mintimeextent, maxtimeextent, number_params, number_samples, meanLatitude,'
            'meanLongitude, meandepth) VALUES (?,?,?,?,?,?,?,?,?,?,?)',
            [metadata["Citation"], metadata["DOI"], metadata["URI"], metadata["date_downloaded"],
             minext, maxext, num_parameters, num_samples, mean_lat, mean_lon, mean_depth])
        con.commit()
    except sqlite3.IntegrityError:
        print("skipping, non-unique doi", metadata["DOI"])

    file_id = cur.lastrowid  # get the file_id of the row just inserted
    print('file_id', file_id)

    cur.execute('INSERT INTO file_metadata (file_id, name, value) VALUES (?,?,?)',
                [file_id, 'title', metadata["Title"]])
    cur.execute('INSERT INTO file_metadata (file_id, name, value) VALUES (?,?,?)',
                [file_id, 'abstract', metadata["Abstract"]])
    cur.execute('INSERT INTO file_metadata (file_id, name, value) VALUES (?,?,?)',
                [file_id, 'method', metadata["Method"]])
    cur.execute('INSERT INTO file_metadata (file_id, name, value) VALUES (?,?,?)',
                [file_id, 'description', metadata["Description"]])

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

