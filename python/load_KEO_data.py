# to parse KEO datafiles to pangaea_sqlite database


import sqlite3
import pandas as pd


dbname = r'test.sqlite'
#dbname = r'part_flux_data.sqlite'
fn = r'Pangaea_wanted_variables_2.csv'
var_interp = pd.read_csv(fn, encoding="ISO-8859-1")

con = sqlite3.connect(dbname, detect_types=sqlite3.PARSE_DECLTYPES)
cur = con.cursor()

# the relevant filenames
filename = r'C:\Users\wyn028\OneDrive - CSIRO\Manuscripts\Particle_flux_database\JAMSTEC_KEO\for database\KEO_data.csv'
metadata_filename = r'C:\Users\wyn028\OneDrive - CSIRO\Manuscripts\Particle_flux_database\JAMSTEC_KEO\for database\README.txt'
unit_metadata_fn = r'C:\Users\wyn028\OneDrive - CSIRO\Manuscripts\Particle_flux_database\JAMSTEC_KEO\for database\KEO_units.csv'
# read the csv file
data = pd.read_csv(filename)
geo_vars = {'latitude', 'longitude', 'depth', 'time_deployed'}
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
# convert date to the right format
new_data['date_deployed'] = pd.to_datetime(data.date_start)
new_data['date_deployed'].dt.strftime('%Y/%m/%d')
minext = new_data['date_deployed'].min().strftime('%Y-%m-%d')
maxext = new_data['date_deployed'].max().strftime('%Y-%m-%d')
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

try:
    cur.execute('INSERT INTO file (id) VALUES (?)', [metadata["DOI"]])
    con.commit()

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
except sqlite3.IntegrityError:
    print("skipping, non-unique doi", metadata["DOI"])

