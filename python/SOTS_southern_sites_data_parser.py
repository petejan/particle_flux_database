# a script to parse the different SOTS southern site .nc files from OceanSites data folder
# I used Matlab to convert the .nc files into .csv files first

import sqlite3
import pandas as pd
import os

def SOTS_southern_data_parser(dbname):

    con = sqlite3.connect(dbname, detect_types=sqlite3.PARSE_DECLTYPES)

    cur = con.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS file (file_id integer primary key autoincrement, id UNIQUE)")
    con.commit()
    cur.execute(
        "CREATE TABLE IF NOT EXISTS file_metadata (file_id, name TEXT, value TEXT, FOREIGN KEY(file_id) REFERENCES file(file_id))")
    con.commit()
    cur.execute(
        "CREATE TABLE IF NOT EXISTS variables (var_id integer primary key autoincrement, file_id, name TEXT, type TEXT, units TEXT, comment TEXT, FOREIGN KEY(file_id) REFERENCES file(file_id))")
    con.commit()
    cur.execute(
        "CREATE TABLE IF NOT EXISTS data (sample_id integer, var_id, timestamp REAL, value TEXT, file_id, CONSTRAINT var_id_fk FOREIGN KEY(var_id) REFERENCES variables(var_id) FOREIGN KEY(file_id) REFERENCES file(file_id))")
    con.commit()

    # the relevant filenames

    data_folder = r'C:\Users\wyn028\OneDrive - CSIRO\Manuscripts\Particle_flux_database\SOTS\southern_sites\csv_files'
    metadata_filename = r'C:\Users\wyn028\OneDrive - CSIRO\Manuscripts\Particle_flux_database\SOTS\southern_sites\README.txt'
    unit_metadata_fn = r'C:\Users\wyn028\OneDrive - CSIRO\Manuscripts\Particle_flux_database\SOTS\southern_sites\helper_files\SOTS_units.csv'

    csv_files = []
    f = os.listdir(data_folder)
    for i in f:
        if i.endswith('.csv'):
            filename = [i]
            csv_files += filename


   # extract the URI (= DOI here) from the IMOS txt file
    for v in csv_files:
        fn = os.path.join(data_folder, v)
        print(fn)
        # read the csv file
        data = pd.read_csv(fn, encoding="ISO-8859-1")

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
        # add the url info for every csv/nc file to the URL and DOI in the metadata
        metadata["DOI"] = data['DOI'][0]
        metadata["Citation"] = data['Reference'][0]
        metadata["Method"] += ", data has not been quality controlled"

        try:
            cur.execute('INSERT INTO file (id) VALUES (?)', [v])
            con.commit()
        except sqlite3.IntegrityError:
            print("skipping, non-unique file identifier", v)
            continue

        # read the csv file
        #data = pd.read_csv(fn, header = None)
        #data = pd.read_csv(fn, encoding="ISO-8859-1", header = None)
        data = pd.read_csv(fn, encoding="ISO-8859-1")

         # convert date to the right format
        data['TIME_START'] = pd.to_datetime(data.TIME_START, dayfirst = True)
        # data['TIME_START'] = pd.to_datetime(data.TIME_START,format='mixed', dayfirst = True)
        data['TIME_START'].dt.strftime('%Y/%m/%d')

        file_id = cur.lastrowid  # get the file_id of the row just inserted
        print('file_id', file_id)

        cur.execute('INSERT INTO file_metadata (file_id, name, value) VALUES (?,?,?)', [file_id, 'uri', metadata["URI"]])
        cur.execute('INSERT INTO file_metadata (file_id, name, value) VALUES (?,?,?)', [file_id, 'title', metadata["Title"]])
        cur.execute('INSERT INTO file_metadata (file_id, name, value) VALUES (?,?,?)', [file_id, 'doi', metadata["DOI"]])
        cur.execute('INSERT INTO file_metadata (file_id, name, value) VALUES (?,?,?)', [file_id, 'abstract', metadata["Abstract"]])
        cur.execute('INSERT INTO file_metadata (file_id, name, value) VALUES (?,?,?)', [file_id, 'citation', metadata["Citation"]])
        cur.execute('INSERT INTO file_metadata (file_id, name, value) VALUES (?,?,?)', [file_id, 'method', metadata["Method"]])
        cur.execute('INSERT INTO file_metadata (file_id, name, value) VALUES (?,?,?)', [file_id, 'date_downloaded', metadata["date_downloaded"]])
        cur.execute('INSERT INTO file_metadata (file_id, name, value) VALUES (?,?,?)', [file_id, 'description', metadata["Description"]])

        con.commit()

        cur.execute("CREATE VIEW IF NOT EXISTS data_names AS "
                    "SELECT variables.file_id, variables.name, datetime(timestamp) AS timestamp, type, value, units "
                    "FROM data JOIN variables ON (data.var_id = variables.var_id)")
        con.commit()

        # load units from the *_units file I created
        unit_metadata = pd.read_csv(unit_metadata_fn, encoding='utf-8')

        # load variables
        parameters = data.columns
        try:
            res = cur.execute("SELECT sample_id FROM data ORDER BY sample_id DESC LIMIT 1")
            s_idx = res.fetchone()[0]
        except TypeError:
            s_idx = 0

        try:
            ts = data['TIME_START'].values
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

                cur.execute('INSERT INTO variables (file_id, name, type, units, comment) VALUES (?,?,?,?,?)', [file_id, data[var_name].name, data[var_name].dtypes.name, unit, unit_comment])
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


if __name__ == "__main__":
    dbname = "SOTS_southern_test.sqlite"
    #dbname = "pangaea.sqlite"
    #dbname = "Mini_pangaea.sqlite"
    SOTS_southern_data_parser(dbname)