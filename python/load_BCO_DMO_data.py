# to parse BCO_DMO datafiles to pangaea_sqlite database
import sqlite3
import pandas as pd
import os
from datetime import datetime


#dbname = "BCO_test.sqlite"
dbname = r'python\test.sqlite'
con = sqlite3.connect(dbname, detect_types=sqlite3.PARSE_DECLTYPES)
cur = con.cursor()

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
#        data['date_start'] = pd.to_datetime(data.date_start, format = '%d/%m/%Y %H:%M')
    data['date_start'] = pd.to_datetime(data.date_start, format='mixed', dayfirst=True)
    data['date_start'].dt.strftime('%Y/%m/%d')
    parameters = data.columns
    num_parameters = data.shape[1]
    num_samples = data.shape[0]
    # TO DO: find aliases for lat, lon and depth --> use mean for file metadata

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
        cur.execute('INSERT INTO file (citation, doi, source, date_loaded) VALUES (?,?,?,?)', [metadata["Citation"], metadata["DOI"], metadata["URI"], metadata["date_downloaded"]])
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

    try:
        res = cur.execute("SELECT sample_id FROM data ORDER BY sample_id DESC LIMIT 1")
        s_idx = res.fetchone()[0]
    except TypeError:
        s_idx = 0

    try:
        ts = data['date_start'].values
        s_id = list(range(s_idx + 1, s_idx + 1 + len(ts)))

        for var_name in parameters:
#               if var_name != 'date_start':
            p = data[var_name].values
            print('variable', data[var_name].name)

            # get the unit for the variable in process
            u_idx = unit_metadata.loc[unit_metadata['Supplied Name'] == var_name, 'Supplied Units']

            try:
                unit = u_idx._values[0].split('(', 1)[1].split(')')[0]
            except (IndexError, AttributeError):
                unit = 'unitless'

            # pull in the descriptions for the variables as well
            c_idx = unit_metadata.loc[unit_metadata['Supplied Name'] == var_name, 'Supplied description']
            unit_comment = c_idx._values[0]

            print('unit', unit)

            cur.execute('INSERT INTO variables (file_id, name, type, units, comment) VALUES (?,?,?,?,?)',
                        [file_id, data[var_name].name, data[var_name].dtypes.name, unit, unit_comment])
            con.commit()

            var_id = cur.lastrowid  # get the var_id of the row just inserted

            for m in range(len(data[var_name])):
                cur.execute('INSERT INTO data (file_id, sample_id, var_id, timestamp, value) VALUES (?, ?, ?, julianday(?), ?)', [file_id, s_id[m], var_id, str(ts[m]), str(p[m])])
                con.commit()

    except KeyError as KE:
        print(KE)
 #       continue
cur.close()
con.close()
