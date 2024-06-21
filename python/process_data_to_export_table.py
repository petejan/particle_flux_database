import sqlite3
import pandas as pd


def create_processed_data_db():
    dbname = "test_sed_data.sqlite"
    con = sqlite3.connect(dbname, detect_types=sqlite3.PARSE_DECLTYPES)
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    # create the database table to take the data
    cur.execute("CREATE TABLE IF NOT EXISTS processed_data"
                "(file_id integer, var_id integer, sample_id integer, "
                "timestamp REAL, duration REAL, duration_units REAL, time_recovered REAL, time_mid REAL, latitude REAL, "
                "longitude REAL, data_type TEXT, instrument_descriptor TEXT, instrument_collector_size REAL,"
                "bathymetry_depth_ETOPO1 REAL, depth REAL, depth_units REAL, mass_total REAL, "
                "mass_total_units TEXT, mass_total_sd REAL, mass_total_qc TEXT, carbon_total REAL, "
                "carbon_total_units_raw TEXT, carbon_total_units TEXT, carbon_total_sd REAL, carbon_total_qc TEXT, "
                "poc REAL, poc_units_raw TEXT, poc_units TEXT, poc_sd REAL, poc_qc TEXT, pic REAL, pic_units_raw TEXT, "
                "pic_units TEXT, pic_sd REAL, pic_qc TEXT, pon REAL, pon_units_raw TEXT, pon_units TEXT, pon_sd REAL, "
                "pon_qc TEXT, pop REAL, pop_units_raw TEXT, pop_units TEXT, pop_sd REAL, pop_qc TEXT, opal REAL, "
                "opal_units_raw TEXT, opal_units REAL, psi REAL, psi_units_raw TEXT, psi_units TEXT, psi_sd REAL, "
                "psi_qc TEXT, psio2 REAL, psio2_units_raw TEXT, psio2_units TEXT, psio2_sd REAL, psio2_qc TEXT, "
                "psi_oh_4 REAL, psi_oh_4_units_raw TEXT, psi_oh_4_units TEXT, psi_oh_4_sd REAL, psi_oh_4_qc TEXT, "
                "pal REAL, pal_units_raw TEXT, pal_units TEXT, pal_sd REAL, pal_qc TEXT, chl REAL, chl_units_raw TEXT, "
                "chl_units TEXT, chl_sd REAL, chl_qc TEXT, pheop REAL, pheop_units_raw TEXT, pheop_units TEXT, "
                "pheop_sd REAL, pheop_qc TEXT, caco3 REAL, caco3_units_raw TEXT, caco3_units TEXT, caco3_sd REAL, "
                "caco3_qc TEXT, ca REAL, ca_units_raw TEXT, ca_units REAL, ca_sd REAL, ca_qc TEXT, fe REAL, "
                "fe_units_raw TEXT, fe_units TEXT, fe_sd REAL, fe_qc TEXT, mn REAL, mn_units_raw TEXT, mn_units TEXT, "
                "mn_sd REAL, mn_qc TEXT, ba REAL, ba_units_raw TEXT, ba_units TEXT, ba_sd REAL, ba_qc TEXT, "
                "lithogenic REAL, lithogenic_units_raw TEXT, lithogenic_units TEXT, lithogenic_sd REAL, "
                "lithogenic_qc REAL, detrital REAL, detrital_units_raw TEXT, detrital_units TEXT, detrital_sd REAL, "
                "detrital_qc TEXT, ti REAL, ti_units_raw TEXT, ti_units TEXT, ti_sd REAL, ti_qc TEXT, reference TEXT, "
                "comments TEXT, doi TEXT, url TEXT, citation TEXT, date_downloaded REAL, sst REAL, "
                "mixed_layer_depth REAL, chl_tot_gsm REAL, npp_c REAL, kd490 REAL, z_eu REAL, par REAL, sfm REAL, "
                "CONSTRAINT FK_data_var_id FOREIGN KEY(var_id) REFERENCES variables(var_id) CONSTRAINT FK_data_file_id "
                "FOREIGN KEY(file_id) REFERENCES file(file_id))")

    cur.execute("CREATE INDEX processed_data_file_id_IDX ON 'processed_data' (file_id)")
    cur.execute("CREATE INDEX processed_data_var_id_IDX ON 'processed_data' (var_id)")
    cur.execute("CREATE INDEX processed_data_sample_IDX ON 'processed_data' (sample_id)")

    # start populating the table with file_id and sample_id to get started
    cur.execute(
        "SELECT data.file_id as file_id, sample_id, var_id FROM data GROUP BY file_id, sample_id, var_id ORDER BY file_id, sample_id, var_id")
    insert = con.cursor()

    for row in cur:
        d = dict(row)
        print(d)

        new_data = (d['file_id'], d['sample_id'], d['var_id'])
        insert.execute('INSERT INTO processed_data (file_id, sample_id, var_id) VALUES (?,?,?)', new_data)

    con.commit()


def convert_units_mg_p_day(value, units_in, conversion_factor):
    if units_in in u_mg or units_in in u_lat or units_in in u_lon or units_in in u_day or units_in in u_m:
        pass
    elif units_in in u_ga:
        try:
            val_in = value * 1000 / 365
        except TypeError:
            val_in = 'NA'
    elif units_in in u_ug:
        try:
            val_in = value / 1000
        except TypeError:
            val_in = 'NA'
    elif units_in in u_ugcma:
        try:
            val_in = value / 1000 / 10000 / 365
        except TypeError:
            val_in = 'NA'
    elif units_in in u_mola:
        try:
            val_in = value * conversion_factor * 1000 / 365
        except TypeError:
            val_in = 'NA'
    elif units_in in u_umolcma:
        try:
            val_in = value * conversion_factor / 1000 / 10000 / 365
        except TypeError:
            val_in = 'NA'
    elif units_in in u_pmol:
        try:
            val_in = value * conversion_factor / 10 ** -9
        except TypeError:
            val_in = 'NA'
    elif units_in in u_mmol or units_in in u_hours:
        try:
            val_in = value * conversion_factor
        except TypeError:
            val_in = 'NA'
    else:
        print('did not understand : ', units_in, ' unit')

    return val_in


def add_timestamp_var(var_interp, dbname):
    var = 'timestamp'
    con = sqlite3.connect(dbname, detect_types=sqlite3.PARSE_DECLTYPES)
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    vars = var_interp[var_interp.column_name == var]
    print(vars)
    var_names = set(vars.name)
    print(var + ' names', str(var_names)[1:-1])

    # # update the processed data table db
    for v in var_names:
        print('working on ', v)
        # update the processed data table db
        try:
            cur.execute("SELECT var_id FROM variables WHERE name = '" + v + "'")
            insert = con.cursor()

            for row in cur:
                d = dict(row)
                print(d)
                new_data = (d['var_id'])
                insert.execute(
                    'UPDATE processed_data set ' + var + ' = data.value FROM data WHERE data.file_id = processed_data.file_id AND data.sample_id = processed_data.sample_id AND data.var_id = ?',
                    new_data)
                con.commit()
        except sqlite3.OperationalError:
            continue


def add_time_recovered_var(var_interp, dbname):
    var = 'time_recovered'
    con = sqlite3.connect(dbname, detect_types=sqlite3.PARSE_DECLTYPES)
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    vars = var_interp[var_interp.column_name == var]
    print(vars)
    var_names = set(vars.name)
    print(var + ' names', str(var_names)[1:-1])

    # update the processed data table db
    for v in var_names:
        print('working on ', v)
        try:
            cur.execute(
                "SELECT data.file_id as file_id, sample_id, value, units, var_id FROM data JOIN variables using (var_id) WHERE variables.name = '" + v + "'")
            insert = con.cursor()
            for row in cur:
                d = dict(row)
                print(d)
                val_in = pd.to_datetime(d['value'])
                val_in = str(val_in)
                new_data = (val_in, d['file_id'], d['sample_id'])
                insert.execute(
                    'UPDATE processed_data set ' + var + '= ?,' + var + '_units = ? WHERE data.file_id = processed_data.file_id'
                                                                        'AND data.sample_id = processed_data.sample_id AND data.var_id = ?',
                    new_data)
            con.commit()
        except sqlite3.OperationalError:
            continue


def add_time_mid_var(var_interp, dbname):
    var = 'time_mid'
    con = sqlite3.connect(dbname, detect_types=sqlite3.PARSE_DECLTYPES)
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    vars = var_interp[var_interp.column_name == var]
    print(vars)
    var_names = set(vars.name)
    print(var + ' names', str(var_names)[1:-1])

    for v in var_names:
        print('working on ', v)
        try:
            cur.execute(
                "SELECT data.file_id as file_id, sample_id, value, units, var_id FROM data JOIN variables using (var_id) WHERE variables.name = '" + v + "'")
            insert = con.cursor()
            for row in cur:
                d = dict(row)
                print(d)
                val_in = pd.to_datetime(d['value'])
                val_in = str(val_in)
                new_data = (val_in, d['var_id'])
                insert.execute(
                    'UPDATE processed_data set ' + var + '= ? WHERE data.file_id = processed_data.file_id'
                                                         'AND data.sample_id = processed_data.sample_id AND data.var_id = ?',
                    new_data)
            con.commit()
        except sqlite3.OperationalError:
            continue


def add_variables(var, var_interp, dbname, var_dict):
    conv_factor = var_dict['conversion_factor']
    conv_unit = var_dict['final_unit']

    con = sqlite3.connect(dbname, detect_types=sqlite3.PARSE_DECLTYPES)
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    vars = var_interp[var_interp.column_name == var]
    print(vars)
    var_names = set(vars.name)
    print(var + ' names', str(var_names)[1:-1])

    # update the processed data table db for the variable
    for v in var_names:
        print('working on ', v)
        # update the processed data table db
        try:
            cur.execute(
                "SELECT data.file_id as file_id, sample_id, value, units, var_id FROM data JOIN variables using (var_id) WHERE variables.name = '" + v + "'")
            insert = con.cursor()

            for row in cur:
                d = dict(row)
                vu = d['units']
                print(vu)
                if vu in u_pc:
                    new_data = (d['value'], conv_unit, vu, d['var_id'])
                    insert.execute(
                        'UPDATE processed_data set ' + var + ' = mass_total * ?/100, ' + var + '_units = ?, ' + var + '_units_raw = ? '
                                                                                                                      'WHERE data.file_id = processed_data.file_id AND data.sample_id = processed_data.sample_id AND data.var_id = ? AND ' + var + ' IS NULL',
                        new_data)
                else:
                    val_in = convert_units_mg_p_day(d['value'], d['units'], conv_factor)
                    new_data = (val_in, conv_unit, vu, d['var_id'])
                    insert.execute(
                        'UPDATE processed_data set ' + var + '= ?,' + var + '_units = ?, ' + var + '_units_raw = ? WHERE data.file_id = processed_data.file_id'
                                                                                                   'AND data.sample_id = processed_data.sample_id AND data.var_id = ?',
                        new_data)
            con.commit()
        except sqlite3.OperationalError:
            continue

    # SD
    vars = var_interp[var_interp.column_name == var + '_sd']
    print(vars)
    if not vars.empty:
        var_names = set(vars.name)
        print(var + '_sd names', str(var_names)[1:-1])
        # update the processed data table db for the variable
        for v in var_names:
            print('working on ', v)
            # update the processed data table db
            try:
                cur.execute(
                    "SELECT data.file_id as file_id, sample_id, value, units, var_id FROM data JOIN variables using (var_id) WHERE variables.name = '" + v + "'")
                insert = con.cursor()

                for row in cur:
                    d = dict(row)
                    vu = d['units']
                    # get the sample units
                    get_units = con.cursor()
                    unit_file_id = (d['file_id'], d['sample_id'])
                    get_units.execute(
                        "SELECT '" + var + '_units_raw FROM processed_data WHERE file_id = ? AND sample_id = ?',
                        unit_file_id)

                    units_raw = get_units.fetchone()
                    var_units = dict(units_raw)[var + '_units_raw']
                    print(vu)
                    if vu in u_pc:
                        new_data = (d['value'], d['var_id'])
                        insert.execute(
                            'UPDATE processed_data set ' + var + ' = mass_total * ?/100 WHERE data.file_id = processed_data.file_id AND data.sample_id = processed_data.sample_id AND data.var_id = ? AND ' + var + ' IS NULL',
                            new_data)
                    elif v in u_plus_minus:
                        val_in = convert_units_mg_p_day(d['value'], var_units, conv_factor)
                        new_data = (val_in, d['var_id'])
                        insert.execute(
                            'UPDATE processed_data set ' + var + ' = mass_total * ?/100 WHERE data.file_id = processed_data.file_id AND data.sample_id = processed_data.sample_id AND data.var_id = ? AND ' + var + ' IS NULL',
                            new_data)
                    else:
                        val_in = convert_units_mg_p_day(d['value'], d['units'], conv_factor)
                        new_data = (val_in, d['var_id'])
                        insert.execute(
                            'UPDATE processed_data set ' + var + '= ? WHERE data.file_id = processed_data.file_id AND data.sample_id = processed_data.sample_id AND data.var_id = ?',
                            new_data)
                con.commit()
            except sqlite3.OperationalError:
                continue

    # QC

    vars = var_interp[var_interp.column_name == var + '_qc']
    print(vars)
    if not vars.empty:
        var_names = set(vars.name)
        print(var + '_qc names', str(var_names)[1:-1])
        # update the processed data table db for the variable
        for v in var_names:
            print('working on ', v)
            # update the processed data table db
            try:
                cur.execute(
                    "SELECT data.file_id as file_id, sample_id, value, units, var_id FROM data JOIN variables using (var_id) WHERE variables.name = '" + v + "'")
                insert = con.cursor()

                for row in cur:
                    d = dict(row)
                    new_data = (d['value'], d['var_id'])
                    insert.execute(
                        'UPDATE processed_data set ' + var + '_qc = ? WHERE data.file_id = processed_data.file_id AND data.sample_id = processed_data.sample_id AND data.var_id = ?',
                        new_data)
                con.commit()
            except sqlite3.OperationalError:
                continue


# to be deleted
def add_duration_var(var_interp, dbname):
    var = 'duration'
    con = sqlite3.connect(dbname, detect_types=sqlite3.PARSE_DECLTYPES)
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    # duration
    vars = var_interp[var_interp.column_name == var]
    print(vars)
    var_names = set(vars.name)
    print(var + ' names', str(var_names)[1:-1])

    # update the processed data table db for the variable
    for v in var_names:
        print('working on ', v)
        # update the processed data table db
        try:
            cur.execute(
                "SELECT data.file_id as file_id, sample_id, value, units, var_id FROM data JOIN variables using (var_id) WHERE variables.name = '" + v + "'")
            insert = con.cursor()

            for row in cur:
                d = dict(row)
                vu = d['units']
                print(vu)
                if vu in u_day:
                    new_data = (d['value'], d['units'], d['var_id'])
                    print(new_data)
                    insert.execute(
                        'UPDATE processed_data set ' + var + '= ?,' + var + '_units = ? WHERE data.file_id = processed_data.file_id'
                                                                            'AND data.sample_id = processed_data.sample_id AND data.var_id = ?',
                        new_data)
                elif vu in u_hours:
                    val_in = convert_units_mg_p_day(d['value'], d['units'], 1 / 24)
                    conv_unit = 'd'
                    new_data = (val_in, conv_unit, d['var_id'])
                    insert.execute(
                        'UPDATE processed_data set ' + var + '= ?,' + var + '_units = ? WHERE data.file_id = processed_data.file_id'
                                                                            'AND data.sample_id = processed_data.sample_id AND data.var_id = ?',
                        new_data)
                else:
                    print('did not understand', vu)
            con.commit()
        except sqlite3.OperationalError:
            continue


# to be deleted
def add_depth_var(var_interp, dbname):
    var = 'depth'
    con = sqlite3.connect(dbname, detect_types=sqlite3.PARSE_DECLTYPES)
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    # depth
    vars = var_interp[var_interp.column_name == var]
    print(vars)
    var_names = set(vars.name)
    print(var + ' names', str(var_names)[1:-1])

    for v in var_names:
        print('working on ', v)
        # update the processed data table db
        try:
            cur.execute(
                "SELECT data.file_id as file_id, sample_id, value, units, var_id FROM data JOIN variables using (var_id) WHERE variables.name = '" + v + "'")
            insert = con.cursor()

            for row in cur:
                d = dict(row)
                vu = d['units']
                print(vu)
                if v in u_m:
                    new_data = (d['value'], d['units'], d['var_id'])
                    insert.execute(
                        'UPDATE processed_data set ' + var + '= ?,' + var + '_units = ? WHERE data.file_id = processed_data.file_id'
                                                                            'AND data.sample_id = processed_data.sample_id AND data.var_id = ?',
                        new_data)
                else:
                    print('did not understand', v)
            con.commit()
        except sqlite3.OperationalError:
            continue


# to be deleted
def add_lat_var(var_interp, dbname):
    var = 'latitude'
    con = sqlite3.connect(dbname, detect_types=sqlite3.PARSE_DECLTYPES)
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    # latitude
    vars = var_interp[var_interp.column_name == var]
    print(vars)
    var_names = set(vars.name)
    print(var + ' names', str(var_names)[1:-1])

    # update the processed data table db for the variable
    cur.execute(
        "SELECT data.file_id as file_id, sample_id, value, units FROM data JOIN variables v using (var_id) WHERE v.name IN (" + str(
            var_names)[1:-1] + ")")
    insert = con.cursor()

    for row in cur:
        d = dict(row)
        #    print(d)
        v = d['units']
        # print(v)
        if v in u_lat:
            # print('inserted', d['value'], 'with unit ', v)
            new_data = (d['value'], d['file_id'], d['sample_id'])
            insert.execute('UPDATE processed_data set ' + var + ' = ? WHERE file_id = ? AND sample_id = ?',
                           new_data)
        else:
            print('did not understand', v)

    con.commit()


# to be deleted
def add_lon_var(var_interp, dbname):
    var = 'longitude'
    con = sqlite3.connect(dbname, detect_types=sqlite3.PARSE_DECLTYPES)
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    u = {'degrees', 'deg', 'unitless', 'decimal degrees', 'degrees_east', 'degrees East'}

    # depth
    vars = var_interp[var_interp.column_name == var]
    print(vars)
    var_names = set(vars.name)
    print(var + ' names', str(var_names)[1:-1])

    # update the processed data table db for the variable
    cur.execute(
        "SELECT data.file_id as file_id, sample_id, value, units FROM data JOIN variables v using (var_id) WHERE v.name IN (" + str(
            var_names)[1:-1] + ")")
    insert = con.cursor()

    for row in cur:
        d = dict(row)
        #    print(d)
        v = d['units']
        # print(v)
        if v in u:
            # print('inserted', d['value'], 'with unit ', v)
            new_data = (d['value'], d['file_id'], d['sample_id'])
            insert.execute('UPDATE processed_data set ' + var + ' = ? WHERE file_id = ? AND sample_id = ?',
                           new_data)
        else:
            print('did not understand', v)

    con.commit()


# to be deleted
def add_mass_total_var(var_interp, dbname):
    var = 'mass_total'
    con = sqlite3.connect(dbname, detect_types=sqlite3.PARSE_DECLTYPES)
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    u_mg = {'mg m-2 d-1', 'mg m-2 day-1', 'mg/m2/d', 'mg/m2/day', 'mg m**2 d*1', 'mg m**2 day*1',
            'mg/m**2/day', 'mg/m**2/d', 'mg_m^-2_d^-1', 'milligram/m2/day'}

    # mass_total
    vars = var_interp[var_interp.column_name == var]
    print(vars)
    var_names = set(vars.name)
    print(var + ' names', str(var_names)[1:-1])

    # update the processed data table db for the variable
    cur.execute(
        "SELECT data.file_id as file_id, sample_id, value, units FROM data JOIN variables v using (var_id) WHERE v.name IN (" + str(
            var_names)[1:-1] + ")")
    insert = con.cursor()

    for row in cur:
        d = dict(row)
        #    print(d)
        v = d['units']
        # print(v)
        if v in u_mg:
            # print('inserted', d['value'], 'with unit ', v)
            new_data = (d['value'], d['units'], d['file_id'], d['sample_id'])
            insert.execute(
                'UPDATE processed_data set ' + var + ' = ?, ' + var + '_units = ? WHERE file_id = ? AND sample_id = ?',
                new_data)
        else:
            print('did not understand', v)

    con.commit()

    # mass_total_sd
    vars = var_interp[var_interp.column_name == var + '_sd']
    print(vars)
    var_names = set(vars.name)
    print(var + '_sd names', str(var_names)[1:-1])

    cur.execute(
        "SELECT data.file_id as file_id, sample_id, value, units FROM data JOIN variables v using (var_id) WHERE v.name IN (" + str(
            var_names)[1:-1] + ")")
    insert = con.cursor()

    for row in cur:
        d = dict(row)
        #    print(d)
        v = d['units']
        # print(v)
        if v in u_mg:
            # print('inserted', d['value'], 'with unit ', v)
            new_data = (d['value'], d['file_id'], d['sample_id'])
            insert.execute('UPDATE processed_data set ' + var + '_sd = ? WHERE file_id = ? AND sample_id = ?', new_data)
        else:
            print('did not understand', v)

    con.commit()

    # mass_total_qc

    vars = var_interp[var_interp.column_name == var + '_qc']
    print(vars)
    var_names = set(vars.name)
    print(var + '_qc names', str(var_names)[1:-1])

    cur.execute(
        "SELECT data.file_id as file_id, sample_id, value FROM data JOIN variables v using (var_id) WHERE v.name IN (" + str(
            var_names)[1:-1] + ")")
    insert = con.cursor()
    for row in cur:
        d = dict(row)
        new_data = (d['value'], d['file_id'], d['sample_id'])
        insert.execute('UPDATE processed_data set ' + var + '_qc = ? WHERE file_id = ? AND sample_id = ?', new_data)

    con.commit()


# to be deleted
def add_carbon_total_var(var_interp, dbname):
    var = 'carbon_total'
    con = sqlite3.connect(dbname, detect_types=sqlite3.PARSE_DECLTYPES)
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    # carbon_total
    vars = var_interp[var_interp.column_name == var]
    print(vars)
    var_names = set(vars.name)
    print(var + ' names', str(var_names)[1:-1])

    for v in var_names:
        print('working on ', v)
        # update the processed data table db
        try:
            cur.execute("SELECT var_id FROM variables WHERE name = '" + v + "'")
            insert = con.cursor()

            for row in cur:
                d = dict(row)
                #    print(d)
                v = d['units']
                conv_unit = 'mg m-2 d-1'
                # print(v)
                if v in u_pc:
                    new_data = (d['value'], conv_unit, v, d['file_id'], d['sample_id'])
                    insert.execute(
                        'UPDATE processed_data set ' + var + ' = mass_total * ?/100, ' + var + '_units = ?, ' + var + '_units_raw = ? WHERE file_id = ? AND sample_id = ? AND ' + var + ' IS NULL',
                        new_data)
                else:
                    new_value = convert_units_mg_p_day(d['value'], d['units'], 12.01)
                    new_data = (new_value, conv_unit, v, d['file_id'], d['sample_id'])
                    insert.execute(
                        'UPDATE processed_data set ' + var + ' = ?, ' + var + '_units = ?, ' + var + '_units_raw = ? WHERE file_id = ? AND sample_id = ? AND ' + var + ' IS NULL',
                        new_data)

                con.commit()
        except sqlite3.OperationalError:
            continue

    # carbon_total_sd
    vars = var_interp[var_interp.column_name == var + '_sd']
    print(vars)
    var_names = set(vars.name)
    print(var + '_sd names', str(var_names)[1:-1])

    for v in var_names:
        print('working on ', v)
        # update the processed data table db
        try:
            cur.execute("SELECT var_id FROM variables WHERE name = '" + v + "'")
            insert = con.cursor()
            for row in cur:
                d = dict(row)
                #    print(d)
                v = d['units']
                # get the sample units
                get_units = con.cursor()
                unit_file_id = (d['file_id'], d['sample_id'])
                get_units.execute(
                    "SELECT poc_units_raw FROM processed_data WHERE file_id = ? AND sample_id = ?', unit_file_id")

                units_raw = get_units.fetchone()
                poc_units = dict(units_raw)['poc_units_raw']

                # print(v)
                if v in u_mg:
                    # print('inserted', d['value'], 'with unit ', v)
                    new_data = (d['value'], d['file_id'], d['sample_id'])
                    insert.execute('UPDATE processed_data set ' + var + '_sd = ? WHERE file_id = ? AND sample_id = ?',
                                   new_data)
                elif v in u_ga:
                    try:
                        val_in = d['value'] * 1000 / 365
                    except TypeError:
                        val_in = 'NA'
                    new_data = (val_in, d['file_id'], d['sample_id'])
                    insert.execute(
                        'UPDATE processed_data set ' + var + '_sd = ? WHERE file_id = ? AND sample_id = ? AND ' + var + '_sd IS NULL',
                        new_data)
                elif v in u_mmol:
                    try:
                        val_in = d['value'] * 12.01
                    except TypeError:
                        val_in = 'NA'
                    new_data = (val_in, d['file_id'], d['sample_id'])
                    insert.execute('UPDATE processed_data set ' + var + '_sd = ? WHERE file_id = ? AND sample_id = ?',
                                   new_data)
                elif v in u_pc:
                    new_data = (d['value'], d['file_id'], d['sample_id'])
                    insert.execute(
                        'UPDATE processed_data set ' + var + '_sd = mass_total * ?/100 WHERE file_id = ? AND sample_id = ? AND ' + var + '_sd IS NULL',
                        new_data)
                else:
                    print('did not understand', v)

            con.commit()
        except sqlite3.OperationalError:
            continue

    # carbon_total_qc

    vars = var_interp[var_interp.column_name == var + '_qc']
    print(vars)
    var_names = set(vars.name)
    print(var + '_qc names', str(var_names)[1:-1])

    cur.execute(
        "SELECT data.file_id as file_id, sample_id, value FROM data JOIN variables v using (var_id) WHERE v.name IN (" + str(
            var_names)[1:-1] + ")")
    insert = con.cursor()
    for row in cur:
        d = dict(row)
        new_data = (d['value'], d['file_id'], d['sample_id'])
        insert.execute('UPDATE processed_data set ' + var + '_qc = ? WHERE file_id = ? AND sample_id = ?', new_data)
    con.commit()


# to be deleted
def add_poc_var(var_interp, dbname):
    var = 'poc'
    con = sqlite3.connect(dbname, detect_types=sqlite3.PARSE_DECLTYPES)
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    u_mg = {'mg m-2 d-1', 'mg m-2 day-1', 'mg C m-2 d-1', 'mg/m2/d', 'mg/m2/day', 'mg m**2 d*1', 'mg m**2 day*1',
            'mg/m**2/day', 'mg/m**2/d'}
    u_mmol = {'mmol m-2 d-1', 'mmol/m**2/day', 'mmol/m**2/d', 'mmol_m^-2_d^-1'}

    u_ga = {'g/m**2/a'}

    u_mola = {'mol/m**2/a'}

    u_pc = {'%', 'percent'}

    # u_plus_minus = {'±'} in this case I have to find the unit of the original variable

    # poc
    vars = var_interp[var_interp.column_name == var]
    print(vars)
    var_names = set(vars.name)
    print(var + ' names', str(var_names)[1:-1])

    for v in var_names:
        print('working on ', v)
        # update the processed data table db
        try:
            cur.execute("SELECT var_id FROM variables WHERE name = '" + v + "'")
            insert = con.cursor()

            for row in cur:
                d = dict(row)
                #    print(d)
                v = d['units']
                # print(v)
                if v in u_mg:
                    # print('inserted', d['value'], 'with unit ', v)
                    new_data = (d['value'], d['units'], d['file_id'], d['sample_id'])
                    insert.execute(
                        'UPDATE processed_data set ' + var + ' = ?, ' + var + '_units = ? WHERE file_id = ? AND sample_id = ?',
                        new_data)
                elif v in u_ga:
                    try:
                        val_in = d['value'] * 1000 / 365
                    except TypeError:
                        val_in = 'NA'
                    conv_unit = 'mg m-2 d-1'
                    new_data = (val_in, conv_unit, d['file_id'], d['sample_id'])
                    insert.execute(
                        'UPDATE processed_data set ' + var + ' = ?, ' + var + '_units = ? WHERE file_id = ? AND sample_id = ? AND ' + var + ' IS NULL',
                        new_data)
                elif v in u_mola:
                    try:
                        val_in = d['value'] * 12.01 * 1000 / 365
                    except TypeError:
                        val_in = 'NA'
                    conv_unit = 'mg m-2 d-1'
                    new_data = (val_in, conv_unit, d['file_id'], d['sample_id'])
                    insert.execute(
                        'UPDATE processed_data set ' + var + ' = ?, ' + var + '_units = ? WHERE file_id = ? AND sample_id = ? AND ' + var + ' IS NULL',
                        new_data)
                elif v in u_mmol:
                    try:
                        val_in = d['value'] * 12.01
                    except TypeError:
                        val_in = 'NA'
                    conv_unit = 'mg m-2 d-1'
                    new_data = (val_in, conv_unit, d['file_id'], d['sample_id'])
                    insert.execute(
                        'UPDATE processed_data set ' + var + ' = ?, ' + var + '_units = ? WHERE file_id = ? AND sample_id = ?',
                        new_data)
                elif v in u_pc:
                    conv_unit = 'mg m-2 d-1'
                    new_data = (d['value'], conv_unit, d['file_id'], d['sample_id'])
                    insert.execute(
                        'UPDATE processed_data set ' + var + ' = mass_total * ?/100, ' + var + '_units = ? WHERE file_id = ? AND sample_id = ? AND ' + var + ' IS NULL',
                        new_data)
                else:
                    print('did not understand', v)

            con.commit()
        except sqlite3.OperationalError:
            continue

    # poc_sd
    vars = var_interp[var_interp.column_name == var + '_sd']
    print(vars)
    var_names = set(vars.name)
    print(var + '_sd names', str(var_names)[1:-1])

    cur.execute(
        "SELECT data.file_id as file_id, sample_id, value, units FROM data JOIN variables v using (var_id) WHERE v.name IN (" + str(
            var_names)[1:-1] + ")")
    insert = con.cursor()

    for row in cur:
        d = dict(row)
        #    print(d)
        v = d['units']
        # print(v)
        if v in u_mg:
            # print('inserted', d['value'], 'with unit ', v)
            new_data = (d['value'], d['file_id'], d['sample_id'])
            insert.execute('UPDATE processed_data set ' + var + '_sd = ? WHERE file_id = ? AND sample_id = ?', new_data)
        elif v in u_ga:
            try:
                val_in = d['value'] * 1000 / 365
            except TypeError:
                val_in = 'NA'
            new_data = (val_in, d['file_id'], d['sample_id'])
            insert.execute(
                'UPDATE processed_data set ' + var + '_sd = ? WHERE file_id = ? AND sample_id = ? AND ' + var + '_sd IS NULL',
                new_data)
        elif v in u_mola:
            try:
                val_in = d['value'] * 12.01 * 1000 / 365
            except TypeError:
                val_in = 'NA'
            new_data = (val_in, d['file_id'], d['sample_id'])
            insert.execute(
                'UPDATE processed_data set ' + var + '_sd = ? WHERE file_id = ? AND sample_id = ? AND ' + var + '_sd IS NULL',
                new_data)
        elif v in u_mmol:
            try:
                val_in = d['value'] * 12.01
            except TypeError:
                val_in = 'NA'
            new_data = (val_in, d['file_id'], d['sample_id'])
            insert.execute('UPDATE processed_data set ' + var + '_sd = ? WHERE file_id = ? AND sample_id = ?', new_data)
        elif v in u_pc:
            new_data = (d['value'], d['file_id'], d['sample_id'])
            insert.execute(
                'UPDATE processed_data set ' + var + '_sd = mass_total * ?/100 WHERE file_id = ? AND sample_id = ? AND ' + var + '_sd IS NULL',
                new_data)
        else:
            print('did not understand', v)

    con.commit()

    # poc_qc

    vars = var_interp[var_interp.column_name == var + '_qc']
    print(vars)
    var_names = set(vars.name)
    print(var + '_qc names', str(var_names)[1:-1])

    cur.execute(
        "SELECT data.file_id as file_id, sample_id, value FROM data JOIN variables v using (var_id) WHERE v.name IN (" + str(
            var_names)[1:-1] + ")")
    insert = con.cursor()
    for row in cur:
        d = dict(row)
        new_data = (d['value'], d['file_id'], d['sample_id'])
        insert.execute('UPDATE processed_data set ' + var + '_qc = ? WHERE file_id = ? AND sample_id = ?', new_data)
    con.commit()


# to be deleted
def add_pic_var(var_interp, dbname):
    var = 'pic'
    con = sqlite3.connect(dbname, detect_types=sqlite3.PARSE_DECLTYPES)
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    u_mg = {'mg m-2 d-1', 'mg m-2 day-1', 'mg C m-2 d-1', 'mg/m2/d', 'mg/m2/day', 'mg m**2 d*1', 'mg m**2 day*1',
            'mg/m**2/day', 'mg/m**2/d'}
    u_mmol = {'mmol m-2 d-1', 'mmol/m**2/day', 'mmol/m**2/d', 'mmol_m^-2_d^-1'}

    u_umol = {'umol/m**2/d'}

    u_mola = {'mol/m**2/a'}

    u_pc = {'%', 'percent'}

    # pic
    vars = var_interp[var_interp.column_name == var]
    print(vars)
    var_names = set(vars.name)
    print(var + ' names', str(var_names)[1:-1])

    # update the processed data table db for the variable
    cur.execute(
        "SELECT data.file_id as file_id, sample_id, value, units FROM data JOIN variables v using (var_id) WHERE v.name IN (" + str(
            var_names)[1:-1] + ")")
    insert = con.cursor()

    for row in cur:
        d = dict(row)
        #    print(d)
        v = d['units']
        # print(v)
        if v in u_mg:
            # print('inserted', d['value'], 'with unit ', v)
            new_data = (d['value'], d['units'], d['file_id'], d['sample_id'])
            insert.execute(
                'UPDATE processed_data set ' + var + ' = ?,' + var + '_units = ? WHERE file_id = ? AND sample_id = ?',
                new_data)
        elif v in u_mmol:
            try:
                val_in = d['value'] * 12.01
            except TypeError:
                val_in = 'NA'
            conv_unit = 'mg m-2 d-1'
            new_data = (val_in, conv_unit, d['file_id'], d['sample_id'])
            insert.execute(
                'UPDATE processed_data set ' + var + ' = ?,' + var + '_units = ? WHERE file_id = ? AND sample_id = ?',
                new_data)
        elif v in u_umol:
            try:
                val_in = d['value'] * 12.01 / 1000
            except TypeError:
                val_in = 'NA'
            conv_unit = 'mg m-2 d-1'
            new_data = (val_in, conv_unit, d['file_id'], d['sample_id'])
            insert.execute(
                'UPDATE processed_data set ' + var + ' = ?,' + var + '_units = ? WHERE file_id = ? AND sample_id = ?',
                new_data)
        elif v in u_mola:
            try:
                val_in = d['value'] * 12.01 * 1000 / 365
            except TypeError:
                val_in = 'NA'
            conv_unit = 'mg m-2 d-1'
            new_data = (val_in, conv_unit, d['file_id'], d['sample_id'])
            insert.execute(
                'UPDATE processed_data set ' + var + ' = ?,' + var + '_units = ? WHERE file_id = ? AND sample_id = ? AND ' + var + ' IS NULL',
                new_data)
        elif v in u_pc:
            conv_unit = 'mg m-2 d-1'
            new_data = (d['value'], conv_unit, d['file_id'], d['sample_id'])
            insert.execute(
                'UPDATE processed_data set ' + var + ' = mass_total * ?/100, ' + var + '_units = ? WHERE file_id = ? AND sample_id = ? AND ' + var + ' IS NULL',
                new_data)
        else:
            print('did not understand', v)

    con.commit()

    # pic_sd
    vars = var_interp[var_interp.column_name == var + '_sd']
    print(vars)
    var_names = set(vars.name)
    print(var + '_sd names', str(var_names)[1:-1])

    cur.execute(
        "SELECT data.file_id as file_id, sample_id, value, units FROM data JOIN variables v using (var_id) WHERE v.name IN (" + str(
            var_names)[1:-1] + ")")
    insert = con.cursor()

    for row in cur:
        d = dict(row)
        #    print(d)
        v = d['units']
        # print(v)
        if v in u_mg:
            # print('inserted', d['value'], 'with unit ', v)
            new_data = (d['value'], d['file_id'], d['sample_id'])
            insert.execute('UPDATE processed_data set ' + var + '_sd = ? WHERE file_id = ? AND sample_id = ?', new_data)
        elif v in u_mmol:
            try:
                val_in = d['value'] * 12.01
            except TypeError:
                val_in = 'NA'
            new_data = (val_in, d['file_id'], d['sample_id'])
            insert.execute('UPDATE processed_data set ' + var + '_sd = ? WHERE file_id = ? AND sample_id = ?', new_data)
        elif v in u_umol:
            try:
                val_in = d['value'] * 12.01 / 1000
            except TypeError:
                val_in = 'NA'
            new_data = (val_in, d['file_id'], d['sample_id'])
            insert.execute('UPDATE processed_data set ' + var + '_sd = ? WHERE file_id = ? AND sample_id = ?',
                           new_data)
        elif v in u_mola:
            try:
                val_in = d['value'] * 12.01 * 1000 / 365
            except TypeError:
                val_in = 'NA'
            new_data = (val_in, d['file_id'], d['sample_id'])
            insert.execute(
                'UPDATE processed_data set ' + var + ' _sd = ? WHERE file_id = ? AND sample_id = ? AND ' + var + '_sd IS NULL',
                new_data)
        elif v in u_pc:
            new_data = (d['value'], d['file_id'], d['sample_id'])
            insert.execute(
                'UPDATE processed_data set ' + var + '_sd = mass_total * ?/100 WHERE file_id = ? AND sample_id = ? AND ' + var + '_sd IS NULL',
                new_data)
        else:
            print('did not understand', v)

    con.commit()

    # pic_qc

    vars = var_interp[var_interp.column_name == var + '_qc']
    print(vars)
    var_names = set(vars.name)
    print(var + '_qc names', str(var_names)[1:-1])

    cur.execute(
        "SELECT data.file_id as file_id, sample_id, value FROM data JOIN variables v using (var_id) WHERE v.name IN (" + str(
            var_names)[1:-1] + ")")
    insert = con.cursor()
    for row in cur:
        d = dict(row)
        new_data = (d['value'], d['file_id'], d['sample_id'])
        insert.execute('UPDATE processed_data set ' + var + '_qc = ? WHERE file_id = ? AND sample_id = ?', new_data)
    con.commit()


# to be deleted
def add_pon_var(var_interp, dbname):
    var = 'pon'
    con = sqlite3.connect(dbname, detect_types=sqlite3.PARSE_DECLTYPES)
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    u_mg = {'mg m-2 d-1', 'mg m-2 day-1', 'mg N m-2 d-1', 'mg/m2/d', 'mg/m2/day', 'mg m**2 d*1', 'mg m**2 day*1',
            'mg/m**2/day', 'mg/m**2/d', 'milligram/m2/day'}
    u_mmol = {'mmol m-2 d-1', 'mmol/m**2/day', 'mmol/m**2/d', 'mmol_m^-2_d^-1'}

    u_ga = {'g/m**2/a'}

    u_pc = {'%', 'percent'}

    # pon
    vars = var_interp[var_interp.column_name == var]
    print(vars)
    var_names = set(vars.name)
    print(var + ' names', str(var_names)[1:-1])

    # update the processed data table db for the variable
    cur.execute(
        "SELECT data.file_id as file_id, sample_id, value, units FROM data JOIN variables v using (var_id) WHERE v.name IN (" + str(
            var_names)[1:-1] + ")")
    insert = con.cursor()

    for row in cur:
        d = dict(row)
        #    print(d)
        v = d['units']
        # print(v)
        if v in u_mg:
            # print('inserted', d['value'], 'with unit ', v)
            new_data = (d['value'], d['units'], d['file_id'], d['sample_id'])
            insert.execute(
                'UPDATE processed_data set ' + var + ' = ?, ' + var + '_units = ? WHERE file_id = ? AND sample_id = ?',
                new_data)
        elif v in u_mmol:
            try:
                val_in = d['value'] * 14.007
            except TypeError:
                val_in = 'NA'
            conv_unit = 'mg m-2 d-1'
            new_data = (val_in, conv_unit, d['file_id'], d['sample_id'])
            insert.execute(
                'UPDATE processed_data set ' + var + ' = ?, ' + var + '_units = ? WHERE file_id = ? AND sample_id = ?',
                new_data)
        elif v in u_ga:
            try:
                val_in = d['value'] * 1000 / 365
            except TypeError:
                val_in = 'NA'
            conv_unit = 'mg m-2 d-1'
            new_data = (val_in, conv_unit, d['file_id'], d['sample_id'])
            insert.execute(
                'UPDATE processed_data set ' + var + ' = ?, ' + var + '_units = ? WHERE file_id = ? AND sample_id = ? AND ' + var + ' IS NULL',
                new_data)
        elif v in u_pc:
            conv_unit = 'mg m-2 d-1'
            new_data = (d['value'], conv_unit, d['file_id'], d['sample_id'])
            insert.execute(
                'UPDATE processed_data set ' + var + ' = mass_total * ?/100, ' + var + '_units = ? WHERE file_id = ? AND sample_id = ? AND ' + var + ' IS NULL',
                new_data)
        else:
            print('did not understand', v)

    con.commit()

    # pon_sd
    vars = var_interp[var_interp.column_name == var + '_sd']
    print(vars)
    var_names = set(vars.name)
    print(var + '_sd names', str(var_names)[1:-1])

    cur.execute(
        "SELECT data.file_id as file_id, sample_id, value, units FROM data JOIN variables v using (var_id) WHERE v.name IN (" + str(
            var_names)[1:-1] + ")")
    insert = con.cursor()

    for row in cur:
        d = dict(row)
        #    print(d)
        v = d['units']
        # print(v)
        if v in u_mg:
            # print('inserted', d['value'], 'with unit ', v)
            new_data = (d['value'], d['file_id'], d['sample_id'])
            insert.execute('UPDATE processed_data set ' + var + '_sd = ? WHERE file_id = ? AND sample_id = ?', new_data)
        elif v in u_mmol:
            try:
                val_in = d['value'] * 14.007
            except TypeError:
                val_in = 'NA'
            new_data = (val_in, d['file_id'], d['sample_id'])
            insert.execute('UPDATE processed_data set ' + var + '_sd = ? WHERE file_id = ? AND sample_id = ?', new_data)
        elif v in u_ga:
            try:
                val_in = d['value'] * 1000 / 365
            except TypeError:
                val_in = 'NA'
            new_data = (val_in, d['file_id'], d['sample_id'])
            insert.execute(
                'UPDATE processed_data set ' + var + '_sd = ? WHERE file_id = ? AND sample_id = ? AND ' + var + '_sd IS NULL',
                new_data)
        elif v in u_pc:
            new_data = (d['value'], d['file_id'], d['sample_id'])
            insert.execute(
                'UPDATE processed_data set ' + var + '_sd = mass_total * ?/100 WHERE file_id = ? AND sample_id = ? AND ' + var + '_sd IS NULL',
                new_data)
        else:
            print('did not understand', v)

    con.commit()

    # pon_qc

    vars = var_interp[var_interp.column_name == var + '_qc']
    print(vars)
    var_names = set(vars.name)
    print(var + '_qc names', str(var_names)[1:-1])

    cur.execute(
        "SELECT data.file_id as file_id, sample_id, value FROM data JOIN variables v using (var_id) WHERE v.name IN (" + str(
            var_names)[1:-1] + ")")
    insert = con.cursor()
    for row in cur:
        d = dict(row)
        new_data = (d['value'], d['file_id'], d['sample_id'])
        insert.execute('UPDATE processed_data set ' + var + '_qc = ? WHERE file_id = ? AND sample_id = ?', new_data)
    con.commit()


# to be deleted
def add_pop_var(var_interp, dbname):
    var = 'pop'
    con = sqlite3.connect(dbname, detect_types=sqlite3.PARSE_DECLTYPES)
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    u_mg = {'mg m-2 d-1', 'mg m-2 day-1', 'mg/m2/d', 'mg/m2/day', 'mg m**2 d*1', 'mg m**2 day*1',
            'mg/m**2/day', 'mg/m**2/d', 'milligram/m2/day'}

    u_ga = {'g/m**2/a'}

    u_mga = {'mg/m**2/a'}

    u_ug = {'ug/m2/day', 'µg/m**2/day'}

    u_mmol = {'mmol m-2 d-1', 'mmol/m**2/day', 'mmol/m**2/d', 'mmol_m^-2_d^-1'}

    u_pc = {'%', 'percent'}

    # pop
    vars = var_interp[var_interp.column_name == var]
    print(vars)
    var_names = set(vars.name)
    print(var + ' names', str(var_names)[1:-1])

    # update the processed data table db for the variable
    cur.execute(
        "SELECT data.file_id as file_id, sample_id, value, units FROM data JOIN variables v using (var_id) WHERE v.name IN (" + str(
            var_names)[1:-1] + ")")
    insert = con.cursor()

    for row in cur:
        d = dict(row)
        #    print(d)
        v = d['units']
        # print(v)
        if v in u_mg:
            # print('inserted', d['value'], 'with unit ', v)
            new_data = (d['value'], d['units'], d['file_id'], d['sample_id'])
            insert.execute(
                'UPDATE processed_data set ' + var + ' = ?, ' + var + '_units = ? WHERE file_id = ? AND sample_id = ?',
                new_data)
        elif v in u_mga:
            try:
                val_in = d['value'] / 365
            except TypeError:
                val_in = 'NA'
            conv_unit = 'mg m-2 d-1'
            new_data = (val_in, conv_unit, d['file_id'], d['sample_id'])
            insert.execute(
                'UPDATE processed_data set ' + var + ' = ?, ' + var + '_units = ? WHERE file_id = ? AND sample_id = ? AND ' + var + ' IS NULL',
                new_data)
        elif v in u_ga:
            try:
                val_in = d['value'] * 1000 / 365
            except TypeError:
                val_in = 'NA'
            conv_unit = 'mg m-2 d-1'
            new_data = (val_in, conv_unit, d['file_id'], d['sample_id'])
            insert.execute(
                'UPDATE processed_data set ' + var + ' = ?, ' + var + '_units = ? WHERE file_id = ? AND sample_id = ? AND ' + var + ' IS NULL',
                new_data)
        elif v in u_ug:
            try:
                val_in = d['value'] / 1000
            except TypeError:
                val_in = 'NA'
            conv_unit = 'mg m-2 d-1'
            new_data = (val_in, conv_unit, d['file_id'], d['sample_id'])
            insert.execute(
                'UPDATE processed_data set ' + var + ' = ?, ' + var + '_units = ? WHERE file_id = ? AND sample_id = ?',
                new_data)
        elif v in u_mmol:
            try:
                val_in = d['value'] * 30.974
            except TypeError:
                val_in = 'NA'
            conv_unit = 'mg m-2 d-1'
            new_data = (val_in, conv_unit, d['file_id'], d['sample_id'])
            insert.execute(
                'UPDATE processed_data set ' + var + ' = ?, ' + var + '_units = ? WHERE file_id = ? AND sample_id = ?',
                new_data)
        elif v in u_pc:
            conv_unit = 'mg m-2 d-1'
            new_data = (d['value'], conv_unit, d['file_id'], d['sample_id'])
            insert.execute(
                'UPDATE processed_data set ' + var + ' = mass_total * ?/100, ' + var + '_units = ? WHERE file_id = ? AND sample_id = ? AND ' + var + ' IS NULL',
                new_data)
        else:
            print('did not understand', v)

    con.commit()

    # pop_sd
    vars = var_interp[var_interp.column_name == var + '_sd']
    print(vars)
    var_names = set(vars.name)
    print(var + '_sd names', str(var_names)[1:-1])

    cur.execute(
        "SELECT data.file_id as file_id, sample_id, value, units FROM data JOIN variables v using (var_id) WHERE v.name IN (" + str(
            var_names)[1:-1] + ")")
    insert = con.cursor()

    for row in cur:
        d = dict(row)
        #    print(d)
        v = d['units']
        # print(v)
        if v in u_mg:
            # print('inserted', d['value'], 'with unit ', v)
            new_data = (d['value'], d['file_id'], d['sample_id'])
            insert.execute('UPDATE processed_data set ' + var + '_sd = ? WHERE file_id = ? AND sample_id = ?', new_data)
        elif v in u_mga:
            try:
                val_in = d['value'] / 365
            except TypeError:
                val_in = 'NA'
            new_data = (val_in, d['file_id'], d['sample_id'])
            insert.execute(
                'UPDATE processed_data set ' + var + '_sd = ? WHERE file_id = ? AND sample_id = ? AND ' + var + '_sd IS NULL',
                new_data)
        elif v in u_ga:
            try:
                val_in = d['value'] * 1000 / 365
            except TypeError:
                val_in = 'NA'
            new_data = (val_in, d['file_id'], d['sample_id'])
            insert.execute(
                'UPDATE processed_data set ' + var + '_sd = ? WHERE file_id = ? AND sample_id = ? AND ' + var + '_sd IS NULL',
                new_data)
        elif v in u_ug:
            try:
                val_in = d['value'] / 1000
            except TypeError:
                val_in = 'NA'
            new_data = (val_in, d['file_id'], d['sample_id'])
            insert.execute('UPDATE processed_data set ' + var + '_sd = ? WHERE file_id = ? AND sample_id = ?',
                           new_data)
        elif v in u_mmol:
            try:
                val_in = d['value'] * 30.974
            except TypeError:
                val_in = 'NA'
            new_data = (val_in, d['file_id'], d['sample_id'])
            insert.execute('UPDATE processed_data set ' + var + '_sd = ? WHERE file_id = ? AND sample_id = ?',
                           new_data)
        elif v in u_pc:
            new_data = (d['value'], d['file_id'], d['sample_id'])
            insert.execute(
                'UPDATE processed_data set ' + var + '_sd = mass_total * ?/100 = ? WHERE file_id = ? AND sample_id = ? AND ' + var + '_sd IS NULL',
                new_data)
        else:
            print('did not understand', v)

    con.commit()


# to be deleted
def add_opal_var(var_interp, dbname):
    var = 'opal'
    con = sqlite3.connect(dbname, detect_types=sqlite3.PARSE_DECLTYPES)
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    u_mg = {'mg m-2 d-1', 'mg m-2 day-1', 'mg/m2/d', 'mg/m2/day', 'mg m**2 d*1', 'mg m**2 day*1',
            'mg/m**2/day', 'mg/m**2/d', 'milligram/m2/day'}

    u_pc = {'%', 'percent'}

    # opal
    vars = var_interp[var_interp.column_name == var]
    print(vars)
    var_names = set(vars.name)
    print(var + ' names', str(var_names)[1:-1])

    # update the processed data table db for the variable
    cur.execute(
        "SELECT data.file_id as file_id, sample_id, value, units FROM data JOIN variables v using (var_id) WHERE v.name IN (" + str(
            var_names)[1:-1] + ")")
    insert = con.cursor()

    for row in cur:
        d = dict(row)
        #    print(d)
        v = d['units']
        # print(v)
        if v in u_mg:
            # print('inserted', d['value'], 'with unit ', v)
            new_data = (d['value'], d['units'], d['file_id'], d['sample_id'])
            insert.execute(
                'UPDATE processed_data set ' + var + ' = ?, ' + var + '_units = ? WHERE file_id = ? AND sample_id = ?',
                new_data)
        elif v in u_pc:
            conv_unit = 'mg m-2 d-1'
            new_data = (d['value'], conv_unit, d['file_id'], d['sample_id'])
            insert.execute(
                'UPDATE processed_data set ' + var + ' = mass_total * ?/100, ' + var + '_units = ? WHERE file_id = ? AND sample_id = ? AND ' + var + ' IS NULL',
                new_data)
        else:
            print('did not understand', v)

    con.commit()


# to be deleted
def add_psi_var(var_interp, dbname):
    var = 'psi'
    con = sqlite3.connect(dbname, detect_types=sqlite3.PARSE_DECLTYPES)
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    u_mg = {'mg m-2 d-1', 'mg m-2 day-1', 'mg/m2/d', 'mg/m2/day', 'mg m**2 d*1', 'mg m**2 day*1',
            'mg/m**2/day', 'mg/m**2/d', 'milligram/m2/day'}

    u_ga = {'g/m**2/a'}

    u_mmol = {'mmol m-2 d-1', 'mmol/m**2/day', 'mmol/m**2/d', 'mmol_m^-2_d^-1'}

    u_umol = {'umol/m**2/d'}

    u_pc = {'%', 'percent'}

    # psi
    vars = var_interp[var_interp.column_name == var]
    print(vars)
    var_names = set(vars.name)
    print(var + ' names', str(var_names)[1:-1])

    # update the processed data table db for the variable
    cur.execute(
        "SELECT data.file_id as file_id, sample_id, value, units FROM data JOIN variables v using (var_id) WHERE v.name IN (" + str(
            var_names)[1:-1] + ")")
    insert = con.cursor()

    for row in cur:
        d = dict(row)
        #    print(d)
        v = d['units']
        # print(v)
        if v in u_mg:
            # print('inserted', d['value'], 'with unit ', v)
            new_data = (d['value'], d['units'], d['file_id'], d['sample_id'])
            insert.execute(
                'UPDATE processed_data set ' + var + ' = ?, ' + var + '_units = ? WHERE file_id = ? AND sample_id = ?',
                new_data)
        elif v in u_ga:
            try:
                val_in = d['value'] * 1000 / 365
            except TypeError:
                val_in = 'NA'
            conv_unit = 'mg m-2 d-1'
            new_data = (val_in, conv_unit, d['file_id'], d['sample_id'])
            insert.execute(
                'UPDATE processed_data set ' + var + ' = ?, ' + var + '_units = ? WHERE file_id = ? AND sample_id = ? AND ' + var + ' IS NULL',
                new_data)
        elif v in u_mmol:
            try:
                val_in = d['value'] * 28.085
            except TypeError:
                val_in = 'NA'
            conv_unit = 'mg m-2 d-1'
            new_data = (val_in, conv_unit, d['file_id'], d['sample_id'])
            insert.execute(
                'UPDATE processed_data set ' + var + ' = ?, ' + var + '_units = ? WHERE file_id = ? AND sample_id = ?',
                new_data)
        elif v in u_umol:
            try:
                val_in = d['value'] * 28.085 / 1000
            except TypeError:
                val_in = 'NA'
            conv_unit = 'mg m-2 d-1'
            new_data = (val_in, conv_unit, d['file_id'], d['sample_id'])
            insert.execute(
                'UPDATE processed_data set ' + var + ' = ?, ' + var + '_units = ? WHERE file_id = ? AND sample_id = ?',
                new_data)
        elif v in u_pc:
            conv_unit = 'mg m-2 d-1'
            new_data = (d['value'], conv_unit, d['file_id'], d['sample_id'])
            insert.execute(
                'UPDATE processed_data set ' + var + ' = mass_total * ?/100, ' + var + '_units = ? WHERE file_id = ? AND sample_id = ? AND ' + var + ' IS NULL',
                new_data)
        else:
            print('did not understand', v)

    con.commit()

    # psi_sd
    vars = var_interp[var_interp.column_name == var + '_sd']
    print(vars)
    var_names = set(vars.name)
    print(var + '_sd names', str(var_names)[1:-1])

    cur.execute(
        "SELECT data.file_id as file_id, sample_id, value, units FROM data JOIN variables v using (var_id) WHERE v.name IN (" + str(
            var_names)[1:-1] + ")")
    insert = con.cursor()

    for row in cur:
        d = dict(row)
        #    print(d)
        v = d['units']
        # print(v)
        if v in u_mg:
            # print('inserted', d['value'], 'with unit ', v)
            new_data = (d['value'], d['file_id'], d['sample_id'])
            insert.execute('UPDATE processed_data set ' + var + '_sd = ? WHERE file_id = ? AND sample_id = ?', new_data)
        elif v in u_ga:
            try:
                val_in = d['value'] * 1000 / 365
            except TypeError:
                val_in = 'NA'
            new_data = (val_in, d['file_id'], d['sample_id'])
            insert.execute(
                'UPDATE processed_data set ' + var + '_sd = ? WHERE file_id = ? AND sample_id = ? AND ' + var + '_sd IS NULL',
                new_data)
        elif v in u_mmol:
            try:
                val_in = d['value'] * 28.085
            except TypeError:
                val_in = 'NA'
            new_data = (val_in, d['file_id'], d['sample_id'])
            insert.execute('UPDATE processed_data set ' + var + '_sd = ? WHERE file_id = ? AND sample_id = ?',
                           new_data)
        elif v in u_umol:
            try:
                val_in = d['value'] * 28.085 / 1000
            except TypeError:
                val_in = 'NA'
            new_data = (val_in, d['file_id'], d['sample_id'])
            insert.execute('UPDATE processed_data set ' + var + '_sd = ? WHERE file_id = ? AND sample_id = ?',
                           new_data)
        elif v in u_pc:
            new_data = (d['value'], d['file_id'], d['sample_id'])
            insert.execute(
                'UPDATE processed_data set ' + var + '_sd = mass_total * ?/100 WHERE file_id = ? AND sample_id = ? AND ' + var + '_sd IS NULL',
                new_data)
        else:
            print('did not understand', v)

    con.commit()

    # psi_qc

    vars = var_interp[var_interp.column_name == var + '_qc']
    print(vars)
    var_names = set(vars.name)
    print(var + '_qc names', str(var_names)[1:-1])

    cur.execute(
        "SELECT data.file_id as file_id, sample_id, value FROM data JOIN variables v using (var_id) WHERE v.name IN (" + str(
            var_names)[1:-1] + ")")
    insert = con.cursor()
    for row in cur:
        d = dict(row)
        new_data = (d['value'], d['file_id'], d['sample_id'])
        insert.execute('UPDATE processed_data set ' + var + '_qc = ? WHERE file_id = ? AND sample_id = ?', new_data)
    con.commit()


# to be deleted
def add_psio2_var(var_interp, dbname):
    var = 'psio2'
    con = sqlite3.connect(dbname, detect_types=sqlite3.PARSE_DECLTYPES)
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    u_mg = {'mg m-2 d-1', 'mg m-2 day-1', 'mg/m2/d', 'mg/m2/day', 'mg m**2 d*1', 'mg m**2 day*1',
            'mg/m**2/day', 'mg/m**2/d', 'milligram/m2/day'}

    u_mmol = {'mmol m-2 d-1', 'mmol/m**2/day', 'mmol/m**2/d', 'mmol_m^-2_d^-1'}

    u_ga = {'g/m**2/a'}

    u_mola = {'mol/m**2/a'}

    u_pc = {'%', 'percent'}

    # psio2
    vars = var_interp[var_interp.column_name == var]
    print(vars)
    var_names = set(vars.name)
    print(var + ' names', str(var_names)[1:-1])

    # update the processed data table db for the variable
    cur.execute(
        "SELECT data.file_id as file_id, sample_id, value, units FROM data JOIN variables v using (var_id) WHERE v.name IN (" + str(
            var_names)[1:-1] + ")")
    insert = con.cursor()

    for row in cur:
        d = dict(row)
        #    print(d)
        v = d['units']
        # print(v)
        if v in u_mg:
            # print('inserted', d['value'], 'with unit ', v)
            new_data = (d['value'], d['units'], d['file_id'], d['sample_id'])
            insert.execute(
                'UPDATE processed_data set ' + var + ' = ?, ' + var + '_units = ? WHERE file_id = ? AND sample_id = ?',
                new_data)
        elif v in u_ga:
            try:
                val_in = d['value'] * 1000 / 365
            except TypeError:
                val_in = 'NA'
            conv_unit = 'mg m-2 d-1'
            new_data = (val_in, conv_unit, d['file_id'], d['sample_id'])
            insert.execute(
                'UPDATE processed_data set ' + var + ' = ?, ' + var + '_units = ? WHERE file_id = ? AND sample_id = ? AND ' + var + ' IS NULL',
                new_data)
        elif v in u_mmol:
            try:
                val_in = d['value'] * (28.085 + 2 * 15.999)
            except TypeError:
                val_in = 'NA'
            conv_unit = 'mg m-2 d-1'
            new_data = (val_in, conv_unit, d['file_id'], d['sample_id'])
            insert.execute(
                'UPDATE processed_data set ' + var + ' = ?, ' + var + '_units = ? WHERE file_id = ? AND sample_id = ?',
                new_data)
        elif v in u_mola:
            try:
                val_in = d['value'] * (28.085 + 2 * 15.999) / 365
            except TypeError:
                val_in = 'NA'
            conv_unit = 'mg m-2 d-1'
            new_data = (val_in, conv_unit, d['file_id'], d['sample_id'])
            insert.execute(
                'UPDATE processed_data set ' + var + ' = ?, ' + var + '_units = ? WHERE file_id = ? AND sample_id = ? AND ' + var + ' IS NULL',
                new_data)
        elif v in u_pc:
            conv_unit = 'mg m-2 d-1'
            new_data = (d['value'], conv_unit, d['file_id'], d['sample_id'])
            insert.execute(
                'UPDATE processed_data set ' + var + ' = mass_total * ?/100, ' + var + '_units = ? WHERE file_id = ? AND sample_id = ? AND ' + var + ' IS NULL',
                new_data)
        else:
            print('did not understand', v)

    con.commit()

    # psio2_sd
    vars = var_interp[var_interp.column_name == var + '_sd']
    print(vars)
    var_names = set(vars.name)
    print(var + '_sd names', str(var_names)[1:-1])

    cur.execute(
        "SELECT data.file_id as file_id, sample_id, value, units FROM data JOIN variables v using (var_id) WHERE v.name IN (" + str(
            var_names)[1:-1] + ")")
    insert = con.cursor()

    for row in cur:
        d = dict(row)
        #    print(d)
        v = d['units']
        # print(v)
        if v in u_mg:
            # print('inserted', d['value'], 'with unit ', v)
            new_data = (d['value'], d['file_id'], d['sample_id'])
            insert.execute('UPDATE processed_data set ' + var + '_sd = ? WHERE file_id = ? AND sample_id = ?', new_data)
        elif v in u_ga:
            try:
                val_in = d['value'] * 1000 / 365
            except TypeError:
                val_in = 'NA'
            new_data = (val_in, d['file_id'], d['sample_id'])
            insert.execute(
                'UPDATE processed_data set ' + var + '_sd = ? WHERE file_id = ? AND sample_id = ? AND ' + var + '_sd IS NULL',
                new_data)
        elif v in u_mmol:
            try:
                val_in = d['value'] * (28.085 + 2 * 15.999)
            except TypeError:
                val_in = 'NA'
            conv_unit = 'mg m-2 d-1'
            new_data = (val_in, d['file_id'], d['sample_id'])
            insert.execute('UPDATE processed_data set ' + var + '_sd = ? WHERE file_id = ? AND sample_id = ?',
                           new_data)
        elif v in u_mola:
            try:
                val_in = d['value'] * (28.085 + 2 * 15.999) / 1000 / 365
            except TypeError:
                val_in = 'NA'
            new_data = (val_in, d['file_id'], d['sample_id'])
            insert.execute(
                'UPDATE processed_data set ' + var + '_sd = ? WHERE file_id = ? AND sample_id = ? AND ' + var + '_sd IS NULL',
                new_data)
        elif v in u_pc:
            new_data = (d['value'], d['file_id'], d['sample_id'])
            insert.execute(
                'UPDATE processed_data set ' + var + '_sd = mass_total * ?/100 WHERE file_id = ? AND sample_id = ? AND ' + var + ' IS NULL',
                new_data)
        else:
            print('did not understand', v)

    con.commit()


# to be deleted
def add_psioh4_var(var_interp, dbname):
    var = 'psi_oh_4'
    con = sqlite3.connect(dbname, detect_types=sqlite3.PARSE_DECLTYPES)
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    u_mg = {'mg m-2 d-1', 'mg m-2 day-1', 'mg/m2/d', 'mg/m2/day', 'mg m**2 d*1', 'mg m**2 day*1',
            'mg/m**2/day', 'mg/m**2/d', 'milligram/m2/day'}

    u_mmol = {'mmol m-2 d-1', 'mmol/m**2/day', 'mmol/m**2/d', 'mmol_m^-2_d^-1'}

    # psi_oh_4
    vars = var_interp[var_interp.column_name == var]
    print(vars)
    var_names = set(vars.name)
    print(var + ' names', str(var_names)[1:-1])

    # update the processed data table db for the variable
    cur.execute(
        "SELECT data.file_id as file_id, sample_id, value, units FROM data JOIN variables v using (var_id) WHERE v.name IN (" + str(
            var_names)[1:-1] + ")")
    insert = con.cursor()

    for row in cur:
        d = dict(row)
        #    print(d)
        v = d['units']
        # print(v)
        if v in u_mg:
            # print('inserted', d['value'], 'with unit ', v)
            new_data = (d['value'], d['units'], d['file_id'], d['sample_id'])
            insert.execute(
                'UPDATE processed_data set ' + var + ' = ?, ' + var + '_units = ? WHERE file_id = ? AND sample_id = ?',
                new_data)
        elif v in u_mmol:
            try:
                val_in = d['value'] * (28.085 + 4 * 15.999 + 2 * 1.008)
            except TypeError:
                val_in = 'NA'
            conv_unit = 'mg m-2 d-1'
            new_data = (val_in, conv_unit, d['file_id'], d['sample_id'])
            insert.execute(
                'UPDATE processed_data set ' + var + ' = ?, ' + var + '_units = ? WHERE file_id = ? AND sample_id = ?',
                new_data)
        # elif v in u_%:
        #     val_in = d['value'] *
        else:
            print('did not understand', v)

    con.commit()


# to be deleted
def add_pal_var(var_interp, dbname):
    var = 'pal'
    con = sqlite3.connect(dbname, detect_types=sqlite3.PARSE_DECLTYPES)
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    u_mg = {'mg m-2 d-1', 'mg m-2 day-1', 'mg/m2/d', 'mg/m2/day', 'mg m**2 d*1', 'mg m**2 day*1',
            'mg/m**2/day', 'mg/m**2/d', 'milligram/m2/day'}

    u_umola = {'µmol/cm**2/a'}

    u_pc = {'%', 'percent'}

    # pal
    vars = var_interp[var_interp.column_name == var]
    print(vars)
    var_names = set(vars.name)
    print(var + ' names', str(var_names)[1:-1])

    # update the processed data table db for the variable
    cur.execute(
        "SELECT data.file_id as file_id, sample_id, value, units FROM data JOIN variables v using (var_id) WHERE v.name IN (" + str(
            var_names)[1:-1] + ")")
    insert = con.cursor()

    for row in cur:
        d = dict(row)
        #    print(d)
        v = d['units']
        # print(v)
        if v in u_mg:
            # print('inserted', d['value'], 'with unit ', v)
            new_data = (d['value'], d['units'], d['file_id'], d['sample_id'])
            insert.execute(
                'UPDATE processed_data set ' + var + ' = ?,' + var + '_units = ? WHERE file_id = ? AND sample_id = ?',
                new_data)
        elif v in u_umola:
            try:
                val_in = d['value'] * 26.982 / 1000 / 10000 / 365
            except TypeError:
                val_in = 'NA'
            conv_unit = 'mg m-2 d-1'
            new_data = (val_in, conv_unit, d['file_id'], d['sample_id'])
            insert.execute(
                'UPDATE processed_data set ' + var + ' = ?, ' + var + '_units = ? WHERE file_id = ? AND sample_id = ? AND ' + var + ' IS NULL',
                new_data)
        elif v in u_pc:
            conv_unit = 'mg m-2 d-1'
            new_data = (d['value'], conv_unit, d['file_id'], d['sample_id'])
            insert.execute(
                'UPDATE processed_data set ' + var + ' = mass_total * ?/100, ' + var + '_units = ? WHERE file_id = ? AND sample_id = ? AND ' + var + ' IS NULL',
                new_data)
        else:
            print('did not understand', v)

    con.commit()

    # pal_sd
    vars = var_interp[var_interp.column_name == var + '_sd']
    print(vars)
    var_names = set(vars.name)
    print(var + '_sd names', str(var_names)[1:-1])

    cur.execute(
        "SELECT data.file_id as file_id, sample_id, value, units FROM data JOIN variables v using (var_id) WHERE v.name IN (" + str(
            var_names)[1:-1] + ")")
    insert = con.cursor()

    for row in cur:
        d = dict(row)
        #    print(d)
        v = d['units']
        # print(v)
        if v in u_mg:
            # print('inserted', d['value'], 'with unit ', v)
            new_data = (d['value'], d['file_id'], d['sample_id'])
            insert.execute('UPDATE processed_data set ' + var + '_sd = ? WHERE file_id = ? AND sample_id = ?', new_data)
        elif v in u_umola:
            try:
                val_in = d['value'] * 26.982 / 1000 / 10000 / 365
            except TypeError:
                val_in = 'NA'
            new_data = (val_in, d['file_id'], d['sample_id'])
            insert.execute(
                'UPDATE processed_data set ' + var + '_sd = ? WHERE file_id = ? AND sample_id = ? AND ' + var + ' IS NULL',
                new_data)
        elif v in u_pc:
            new_data = (d['value'], d['file_id'], d['sample_id'])
            insert.execute(
                'UPDATE processed_data set ' + var + '_sd = mass_total * ?/100 WHERE file_id = ? AND sample_id = ? AND ' + var + ' IS NULL',
                new_data)
        else:
            print('did not understand', v)

    con.commit()

    # pal_qc

    vars = var_interp[var_interp.column_name == var + '_qc']
    print(vars)
    var_names = set(vars.name)
    print(var + '_qc names', str(var_names)[1:-1])

    cur.execute(
        "SELECT data.file_id as file_id, sample_id, value FROM data JOIN variables v using (var_id) WHERE v.name IN (" + str(
            var_names)[1:-1] + ")")
    insert = con.cursor()
    for row in cur:
        d = dict(row)
        new_data = (d['value'], d['file_id'], d['sample_id'])
        insert.execute('UPDATE processed_data set ' + var + '_qc = ? WHERE file_id = ? AND sample_id = ?', new_data)
    con.commit()


# to be deleted
def add_chl_var(var_interp, dbname):
    var = 'chl'
    con = sqlite3.connect(dbname, detect_types=sqlite3.PARSE_DECLTYPES)
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    u_mg = {'mg m-2 d-1', 'mg Chl a m-2 d-1', 'mg m-2 day-1', 'mg/m2/d', 'mg/m2/day', 'mg m**2 d*1', 'mg m**2 day*1',
            'mg/m**2/day', 'mg/m**2/d', 'milligram/m2/day'}

    u_pmol = {'pmol/m**2/day'}

    u_ug = {'µg/m**2/day'}

    u_pc = {'%', 'percent'}

    # Chl
    vars = var_interp[var_interp.column_name == var]
    print(vars)
    var_names = set(vars.name)
    print(var + ' names', str(var_names)[1:-1])

    # update the processed data table db for the variable
    cur.execute(
        "SELECT data.file_id as file_id, sample_id, value, units FROM data JOIN variables v using (var_id) WHERE v.name IN (" + str(
            var_names)[1:-1] + ")")
    insert = con.cursor()

    for row in cur:
        d = dict(row)
        #    print(d)
        v = d['units']
        # print(v)
        if v in u_mg:
            # print('inserted', d['value'], 'with unit ', v)
            new_data = (d['value'], d['units'], d['file_id'], d['sample_id'])
            insert.execute(
                'UPDATE processed_data set ' + var + ' = ?, ' + var + '_units = ? WHERE file_id = ? AND sample_id = ?',
                new_data)
        elif v in u_pmol:
            try:
                val_in = d['value'] * 892.54 / 10 ** -9
            except TypeError:
                val_in = 'NA'
            conv_unit = 'mg m-2 d-1'
            new_data = (val_in, conv_unit, d['file_id'], d['sample_id'])
            insert.execute(
                'UPDATE processed_data set ' + var + ' = ?, ' + var + '_units = ? WHERE file_id = ? AND sample_id = ?',
                new_data)
        elif v in u_ug:
            try:
                val_in = d['value'] / 1000
            except TypeError:
                val_in = 'NA'
            conv_unit = 'mg m-2 d-1'
            new_data = (val_in, conv_unit, d['file_id'], d['sample_id'])
            insert.execute(
                'UPDATE processed_data set ' + var + ' = ?, ' + var + '_units = ? WHERE file_id = ? AND sample_id = ?',
                new_data)
        elif v in u_pc:
            conv_unit = 'mg m-2 d-1'
            new_data = (d['value'], conv_unit, d['file_id'], d['sample_id'])
            insert.execute(
                'UPDATE processed_data set ' + var + ' = mass_total * ?/100, ' + var + '_units = ? WHERE file_id = ? AND sample_id = ? AND ' + var + ' IS NULL',
                new_data)
        else:
            print('did not understand', v)

    con.commit()

    # Chl_sd
    vars = var_interp[var_interp.column_name == var + '_sd']
    print(vars)
    var_names = set(vars.name)
    print(var + '_sd names', str(var_names)[1:-1])

    cur.execute(
        "SELECT data.file_id as file_id, sample_id, value, units FROM data JOIN variables v using (var_id) WHERE v.name IN (" + str(
            var_names)[1:-1] + ")")
    insert = con.cursor()

    for row in cur:
        d = dict(row)
        #    print(d)
        v = d['units']
        # print(v)
        if v in u_mg:
            # print('inserted', d['value'], 'with unit ', v)
            new_data = (d['value'], d['file_id'], d['sample_id'])
            insert.execute('UPDATE processed_data set ' + var + '_sd = ? WHERE file_id = ? AND sample_id = ?', new_data)
        elif v in u_pmol:
            try:
                val_in = d['value'] * 892.54 / 10 ** -9
            except TypeError:
                val_in = 'NA'
            conv_unit = 'mg m-2 d-1'
            new_data = (val_in, d['file_id'], d['sample_id'])
            insert.execute('UPDATE processed_data set ' + var + '_sd = ? WHERE file_id = ? AND sample_id = ?',
                           new_data)
        elif v in u_ug:
            try:
                val_in = d['value'] / 1000
            except TypeError:
                val_in = 'NA'
            new_data = (val_in, d['file_id'], d['sample_id'])
            insert.execute('UPDATE processed_data set ' + var + '_sd = ? WHERE file_id = ? AND sample_id = ?',
                           new_data)
        elif v in u_pc:
            new_data = (d['value'], d['file_id'], d['sample_id'])
            insert.execute(
                'UPDATE processed_data set ' + var + '_sd = mass_total * ?/100 WHERE file_id = ? AND sample_id = ? AND ' + var + ' IS NULL',
                new_data)
        else:
            print('did not understand', v)

    con.commit()


# to be deleted
def add_pheop_var(var_interp, dbname):
    var = 'pheop'
    con = sqlite3.connect(dbname, detect_types=sqlite3.PARSE_DECLTYPES)
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    u_mg = {'mg m-2 d-1', 'mg Chl a m-2 d-1', 'mg m-2 day-1', 'mg/m2/d', 'mg/m2/day', 'mg m**2 d*1', 'mg m**2 day*1',
            'mg/m**2/day', 'mg/m**2/d', 'milligram/m2/day'}

    u_ug = {'µg/m**2/day'}

    # pheop
    vars = var_interp[var_interp.column_name == var]
    print(vars)
    var_names = set(vars.name)
    print(var + ' names', str(var_names)[1:-1])

    # update the processed data table db for the variable
    cur.execute(
        "SELECT data.file_id as file_id, sample_id, value, units FROM data JOIN variables v using (var_id) WHERE v.name IN (" + str(
            var_names)[1:-1] + ")")
    insert = con.cursor()

    for row in cur:
        d = dict(row)
        #    print(d)
        v = d['units']
        # print(v)
        if v in u_mg:
            # print('inserted', d['value'], 'with unit ', v)
            new_data = (d['value'], d['units'], d['file_id'], d['sample_id'])
            insert.execute(
                'UPDATE processed_data set ' + var + ' = ?, ' + var + '_units = ? WHERE file_id = ? AND sample_id = ?',
                new_data)
        elif v in u_ug:
            try:
                val_in = d['value'] / 1000
            except TypeError:
                val_in = 'NA'
            conv_unit = 'mg m-2 d-1'
            new_data = (val_in, conv_unit, d['file_id'], d['sample_id'])
            insert.execute(
                'UPDATE processed_data set ' + var + ' = ?, ' + var + '_units = ? WHERE file_id = ? AND sample_id = ?',
                new_data)
        else:
            print('did not understand', v)

    con.commit()

    # pheop_sd
    vars = var_interp[var_interp.column_name == var + '_sd']
    print(vars)
    var_names = set(vars.name)
    print(var + '_sd names', str(var_names)[1:-1])

    cur.execute(
        "SELECT data.file_id as file_id, sample_id, value, units FROM data JOIN variables v using (var_id) WHERE v.name IN (" + str(
            var_names)[1:-1] + ")")
    insert = con.cursor()

    for row in cur:
        d = dict(row)
        #    print(d)
        v = d['units']
        # print(v)
        if v in u_mg:
            # print('inserted', d['value'], 'with unit ', v)
            new_data = (d['value'], d['file_id'], d['sample_id'])
            insert.execute('UPDATE processed_data set ' + var + '_sd = ? WHERE file_id = ? AND sample_id = ?', new_data)
        elif v in u_ug:
            try:
                val_in = d['value'] / 1000
            except TypeError:
                val_in = 'NA'
            new_data = (val_in, d['file_id'], d['sample_id'])
            insert.execute('UPDATE processed_data set ' + var + '_sd = ? WHERE file_id = ? AND sample_id = ?',
                           new_data)
        else:
            print('did not understand', v)

    con.commit()


# to be deleted
def add_caco3_var(var_interp, dbname):
    var = 'caco3'
    con = sqlite3.connect(dbname, detect_types=sqlite3.PARSE_DECLTYPES)
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    u_mg = {'mg m-2 d-1', 'mg m-2 day-1', 'mg C m-2 d-1', 'mg/m2/d', 'mg/m2/day', 'mg m**2 d*1', 'mg m**2 day*1',
            'mg/m**2/day', 'mg/m**2/d'}

    u_mmol = {'mmol m-2 d-1', 'mmol/m**2/day', 'mmol/m**2/d', 'mmol_m^-2_d^-1'}

    u_ga = {'g/m**2/a'}

    u_pc = {'%', 'percent'}

    # caco3
    vars = var_interp[var_interp.column_name == var]
    print(vars)
    var_names = set(vars.name)
    print(var + ' names', str(var_names)[1:-1])

    # update the processed data table db for the variable
    cur.execute(
        "SELECT data.file_id as file_id, sample_id, value, units FROM data JOIN variables v using (var_id) WHERE v.name IN (" + str(
            var_names)[1:-1] + ")")
    insert = con.cursor()

    for row in cur:
        d = dict(row)
        #    print(d)
        v = d['units']
        # print(v)
        if v in u_mg:
            # print('inserted', d['value'], 'with unit ', v)
            new_data = (d['value'], d['units'], d['file_id'], d['sample_id'])
            insert.execute(
                'UPDATE processed_data set ' + var + ' = ?,' + var + '_units = ? WHERE file_id = ? AND sample_id = ?',
                new_data)
        elif v in u_ga:
            try:
                val_in = d['value'] * 1000 / 365
            except TypeError:
                val_in = 'NA'
            conv_unit = 'mg m-2 d-1'
            new_data = (val_in, conv_unit, d['file_id'], d['sample_id'])
            insert.execute(
                'UPDATE processed_data set ' + var + ' = ?,' + var + '_units = ? WHERE file_id = ? AND sample_id = ? AND ' + var + ' IS NULL',
                new_data)
        elif v in u_mmol:
            try:
                val_in = d['value'] * 100.0869
            except TypeError:
                val_in = 'NA'
            conv_unit = 'mg m-2 d-1'
            new_data = (val_in, conv_unit, d['file_id'], d['sample_id'])
            insert.execute(
                'UPDATE processed_data set ' + var + ' = ?,' + var + '_units = ? WHERE file_id = ? AND sample_id = ?',
                new_data)
        elif v in u_pc:
            conv_unit = 'mg m-2 d-1'
            new_data = (d['value'], conv_unit, d['file_id'], d['sample_id'])
            insert.execute(
                'UPDATE processed_data set ' + var + ' = mass_total * ?/100, ' + var + '_units = ? WHERE file_id = ? AND sample_id = ? AND ' + var + ' IS NULL',
                new_data)
        else:
            print('did not understand', v)

    con.commit()

    # caco3_sd
    vars = var_interp[var_interp.column_name == var + '_sd']
    print(vars)
    var_names = set(vars.name)
    print(var + '_sd names', str(var_names)[1:-1])

    cur.execute(
        "SELECT data.file_id as file_id, sample_id, value, units FROM data JOIN variables v using (var_id) WHERE v.name IN (" + str(
            var_names)[1:-1] + ")")
    insert = con.cursor()

    for row in cur:
        d = dict(row)
        #    print(d)
        v = d['units']
        # print(v)
        if v in u_mg:
            # print('inserted', d['value'], 'with unit ', v)
            new_data = (d['value'], d['file_id'], d['sample_id'])
            insert.execute('UPDATE processed_data set ' + var + '_sd = ? WHERE file_id = ? AND sample_id = ?', new_data)
        elif v in u_ga:
            try:
                val_in = d['value'] * 1000 / 365
            except TypeError:
                val_in = 'NA'
            new_data = (val_in, d['file_id'], d['sample_id'])
            insert.execute(
                'UPDATE processed_data set ' + var + '_sd = ? WHERE file_id = ? AND sample_id = ? AND ' + var + ' IS NULL',
                new_data)
        elif v in u_mmol:
            try:
                val_in = d['value'] * 100.0869
            except TypeError:
                val_in = 'NA'
            new_data = (val_in, d['file_id'], d['sample_id'])
            insert.execute(
                'UPDATE processed_data set ' + var + '_sd = ? WHERE file_id = ? AND sample_id = ?',
                new_data)
        elif v in u_pc:
            new_data = (d['value'], d['file_id'], d['sample_id'])
            insert.execute(
                'UPDATE processed_data set ' + var + '_sd = mass_total * ?/100 WHERE file_id = ? AND sample_id = ? AND ' + var + ' IS NULL',
                new_data)
        else:
            print('did not understand', v)

    con.commit()


# to be deleted
def add_ca_var(var_interp, dbname):
    var = 'ca'
    con = sqlite3.connect(dbname, detect_types=sqlite3.PARSE_DECLTYPES)
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    u_mg = {'mg m-2 d-1', 'mg m-2 day-1', 'mg C m-2 d-1', 'mg/m2/d', 'mg/m2/day', 'mg m**2 d*1', 'mg m**2 day*1',
            'mg/m**2/day', 'mg/m**2/d'}

    u_pc = {'%', 'percent'}

    # ca
    vars = var_interp[var_interp.column_name == var]
    print(vars)
    var_names = set(vars.name)
    print(var + ' names', str(var_names)[1:-1])

    # update the processed data table db for the variable
    cur.execute(
        "SELECT data.file_id as file_id, sample_id, value, units FROM data JOIN variables v using (var_id) WHERE v.name IN (" + str(
            var_names)[1:-1] + ")")
    insert = con.cursor()

    for row in cur:
        d = dict(row)
        #    print(d)
        v = d['units']
        # print(v)
        if v in u_mg:
            # print('inserted', d['value'], 'with unit ', v)
            new_data = (d['value'], d['units'], d['file_id'], d['sample_id'])
            insert.execute(
                'UPDATE processed_data set ' + var + ' = ?,' + var + '_units = ? WHERE file_id = ? AND sample_id = ?',
                new_data)
        elif v in u_pc:
            conv_unit = 'mg m-2 d-1'
            new_data = (d['value'], conv_unit, d['file_id'], d['sample_id'])
            insert.execute(
                'UPDATE processed_data set ' + var + ' = mass_total * ?/100, ' + var + '_units = ? WHERE file_id = ? AND sample_id = ? AND ' + var + ' IS NULL',
                new_data)
        else:
            print('did not understand', v)

    con.commit()

    # ca_sd
    vars = var_interp[var_interp.column_name == var + '_sd']
    print(vars)
    var_names = set(vars.name)
    print(var + '_sd names', str(var_names)[1:-1])

    cur.execute(
        "SELECT data.file_id as file_id, sample_id, value, units FROM data JOIN variables v using (var_id) WHERE v.name IN (" + str(
            var_names)[1:-1] + ")")
    insert = con.cursor()

    for row in cur:
        d = dict(row)
        #    print(d)
        v = d['units']
        # print(v)
        if v in u_mg:
            # print('inserted', d['value'], 'with unit ', v)
            new_data = (d['value'], d['file_id'], d['sample_id'])
            insert.execute('UPDATE processed_data set ' + var + '_sd = ? WHERE file_id = ? AND sample_id = ?', new_data)
        elif v in u_pc:
            new_data = (d['value'], d['file_id'], d['sample_id'])
            insert.execute(
                'UPDATE processed_data set ' + var + '_sd = mass_total * ?/100 WHERE file_id = ? AND sample_id = ? AND ' + var + ' IS NULL',
                new_data)
        else:
            print('did not understand', v)

    con.commit()

    # ca_qc

    vars = var_interp[var_interp.column_name == var + '_qc']
    print(vars)
    var_names = set(vars.name)
    print(var + '_qc names', str(var_names)[1:-1])

    cur.execute(
        "SELECT data.file_id as file_id, sample_id, value FROM data JOIN variables v using (var_id) WHERE v.name IN (" + str(
            var_names)[1:-1] + ")")
    insert = con.cursor()
    for row in cur:
        d = dict(row)
        new_data = (d['value'], d['file_id'], d['sample_id'])
        insert.execute('UPDATE processed_data set ' + var + '_qc = ? WHERE file_id = ? AND sample_id = ?', new_data)
    con.commit()


# to be deleted
def add_fe_var(var_interp, dbname):
    var = 'fe'
    con = sqlite3.connect(dbname, detect_types=sqlite3.PARSE_DECLTYPES)
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    u_mg = {'mg m-2 d-1', 'mg Chl a m-2 d-1', 'mg m-2 day-1', 'mg/m2/d', 'mg/m2/day', 'mg m**2 d*1', 'mg m**2 day*1',
            'mg/m**2/day', 'mg/m**2/d', 'milligram/m2/day'}

    u_umola = {'µmol/cm**2/a'}

    u_pc = {'%', 'percent'}

    # fe
    vars = var_interp[var_interp.column_name == var]
    print(vars)
    var_names = set(vars.name)
    print(var + ' names', str(var_names)[1:-1])

    # update the processed data table db for the variable
    cur.execute(
        "SELECT data.file_id as file_id, sample_id, value, units FROM data JOIN variables v using (var_id) WHERE v.name IN (" + str(
            var_names)[1:-1] + ")")
    insert = con.cursor()

    for row in cur:
        d = dict(row)
        #    print(d)
        v = d['units']
        # print(v)
        if v in u_mg:
            # print('inserted', d['value'], 'with unit ', v)
            new_data = (d['value'], d['units'], d['file_id'], d['sample_id'])
            insert.execute(
                'UPDATE processed_data set ' + var + ' = ?, ' + var + '_units = ? WHERE file_id = ? AND sample_id = ?',
                new_data)
        elif v in u_umola:
            try:
                val_in = d['value'] * 55.845 / 1000 / 10000 / 365
            except TypeError:
                val_in = 'NA'
            conv_unit = 'mg m-2 d-1'
            new_data = (val_in, conv_unit, d['file_id'], d['sample_id'])
            insert.execute(
                'UPDATE processed_data set ' + var + ' = ?, ' + var + '_units = ? WHERE file_id = ? AND sample_id = ? AND ' + var + ' IS NULL',
                new_data)
        elif v in u_pc:
            conv_unit = 'mg m-2 d-1'
            new_data = (d['value'], conv_unit, d['file_id'], d['sample_id'])
            insert.execute(
                'UPDATE processed_data set ' + var + ' = mass_total * ?/100, ' + var + '_units = ? WHERE file_id = ? AND sample_id = ? AND ' + var + ' IS NULL',
                new_data)
        else:
            print('did not understand', v)

    con.commit()

    # fe_sd
    vars = var_interp[var_interp.column_name == var + '_sd']
    print(vars)
    var_names = set(vars.name)
    print(var + '_sd names', str(var_names)[1:-1])

    cur.execute(
        "SELECT data.file_id as file_id, sample_id, value, units FROM data JOIN variables v using (var_id) WHERE v.name IN (" + str(
            var_names)[1:-1] + ")")
    insert = con.cursor()

    for row in cur:
        d = dict(row)
        #    print(d)
        v = d['units']
        # print(v)
        if v in u_mg:
            # print('inserted', d['value'], 'with unit ', v)
            new_data = (d['value'], d['file_id'], d['sample_id'])
            insert.execute('UPDATE processed_data set ' + var + '_sd = ? WHERE file_id = ? AND sample_id = ?', new_data)
        elif v in u_umola:
            try:
                val_in = d['value'] * 55.845 / 1000 / 10000 / 365
            except TypeError:
                val_in = 'NA'
            new_data = (val_in, d['file_id'], d['sample_id'])
            insert.execute(
                'UPDATE processed_data set ' + var + '_sd = ? WHERE file_id = ? AND sample_id = ? AND ' + var + ' IS NULL',
                new_data)
        elif v in u_pc:
            new_data = (d['value'], d['file_id'], d['sample_id'])
            insert.execute(
                'UPDATE processed_data set ' + var + '_sd = mass_total * ?/100 WHERE file_id = ? AND sample_id = ? AND ' + var + ' IS NULL',
                new_data)
        else:
            print('did not understand', v)

    con.commit()

    # fe_qc

    vars = var_interp[var_interp.column_name == var + '_qc']
    print(vars)
    var_names = set(vars.name)
    print(var + '_qc names', str(var_names)[1:-1])

    cur.execute(
        "SELECT data.file_id as file_id, sample_id, value FROM data JOIN variables v using (var_id) WHERE v.name IN (" + str(
            var_names)[1:-1] + ")")
    insert = con.cursor()
    for row in cur:
        d = dict(row)
        new_data = (d['value'], d['file_id'], d['sample_id'])
        insert.execute('UPDATE processed_data set ' + var + '_qc = ? WHERE file_id = ? AND sample_id = ?', new_data)
    con.commit()


# to be deleted
def add_mn_var(var_interp, dbname):
    var = 'mn'
    con = sqlite3.connect(dbname, detect_types=sqlite3.PARSE_DECLTYPES)
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    u_ug = {'µg/m**2/day'}

    u_pc = {'%', 'percent'}

    # mn
    vars = var_interp[var_interp.column_name == var]
    print(vars)
    var_names = set(vars.name)
    print(var + ' names', str(var_names)[1:-1])

    # update the processed data table db for the variable
    cur.execute(
        "SELECT data.file_id as file_id, sample_id, value, units FROM data JOIN variables v using (var_id) WHERE v.name IN (" + str(
            var_names)[1:-1] + ")")
    insert = con.cursor()

    for row in cur:
        d = dict(row)
        #    print(d)
        v = d['units']
        # print(v)
        if v in u_ug:
            try:
                val_in = d['value'] / 1000
            except TypeError:
                val_in = 'NA'
            conv_unit = 'mg m-2 d-1'
            new_data = (val_in, conv_unit, d['file_id'], d['sample_id'])
            insert.execute(
                'UPDATE processed_data set ' + var + ' = ?, ' + var + '_units = ? WHERE file_id = ? AND sample_id = ? AND ' + var + ' IS NULL',
                new_data)
        elif v in u_pc:
            conv_unit = 'mg m-2 d-1'
            new_data = (d['value'], conv_unit, d['file_id'], d['sample_id'])
            insert.execute(
                'UPDATE processed_data set ' + var + ' = mass_total * ?/100, ' + var + '_units = ? WHERE file_id = ? AND sample_id = ? AND ' + var + ' IS NULL',
                new_data)
        else:
            print('did not understand', v)

    con.commit()

    # mn_sd
    vars = var_interp[var_interp.column_name == var + '_sd']
    print(vars)
    var_names = set(vars.name)
    print(var + '_sd names', str(var_names)[1:-1])

    cur.execute(
        "SELECT data.file_id as file_id, sample_id, value, units FROM data JOIN variables v using (var_id) WHERE v.name IN (" + str(
            var_names)[1:-1] + ")")
    insert = con.cursor()

    for row in cur:
        d = dict(row)
        #    print(d)
        v = d['units']
        # print(v)
        if v in u_ug:
            try:
                val_in = d['value'] / 1000
            except TypeError:
                val_in = 'NA'
            new_data = (val_in, d['file_id'], d['sample_id'])
            insert.execute(
                'UPDATE processed_data set ' + var + '_sd = ? WHERE file_id = ? AND sample_id = ? AND ' + var + ' IS NULL',
                new_data)
        elif v in u_pc:
            new_data = (d['value'], d['file_id'], d['sample_id'])
            insert.execute(
                'UPDATE processed_data set ' + var + '_sd = mass_total * ?/100 WHERE file_id = ? AND sample_id = ? AND ' + var + ' IS NULL',
                new_data)
        else:
            print('did not understand', v)

    con.commit()

    # mn_qc

    vars = var_interp[var_interp.column_name == var + '_qc']
    print(vars)
    var_names = set(vars.name)
    print(var + '_qc names', str(var_names)[1:-1])

    cur.execute(
        "SELECT data.file_id as file_id, sample_id, value FROM data JOIN variables v using (var_id) WHERE v.name IN (" + str(
            var_names)[1:-1] + ")")
    insert = con.cursor()
    for row in cur:
        d = dict(row)
        new_data = (d['value'], d['file_id'], d['sample_id'])
        insert.execute('UPDATE processed_data set ' + var + '_qc = ? WHERE file_id = ? AND sample_id = ?', new_data)
    con.commit()


# to be deleted
def add_ba_var(var_interp, dbname):
    var = 'ba'
    con = sqlite3.connect(dbname, detect_types=sqlite3.PARSE_DECLTYPES)
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    u_ug = {'µg/m**2/day'}

    u_mmol = {'mmol_m^-2_d^-1'}

    u_pc = {'%', 'percent'}

    # ba
    vars = var_interp[var_interp.column_name == var]
    print(vars)
    var_names = set(vars.name)
    print(var + ' names', str(var_names)[1:-1])

    # update the processed data table db for the variable
    cur.execute(
        "SELECT data.file_id as file_id, sample_id, value, units FROM data JOIN variables v using (var_id) WHERE v.name IN (" + str(
            var_names)[1:-1] + ")")
    insert = con.cursor()

    for row in cur:
        d = dict(row)
        #    print(d)
        v = d['units']
        # print(v)
        if v in u_ug:
            try:
                val_in = d['value'] / 1000
            except TypeError:
                val_in = 'NA'
            conv_unit = 'mg m-2 d-1'
            new_data = (val_in, conv_unit, d['file_id'], d['sample_id'])
            insert.execute(
                'UPDATE processed_data set ' + var + ' = ?, ' + var + '_units = ? WHERE file_id = ? AND sample_id = ? AND ' + var + ' IS NULL',
                new_data)
        elif v in u_mmol:
            try:
                val_in = d['value'] * 137.327
            except TypeError:
                val_in = 'NA'
            conv_unit = 'mg m-2 d-1'
            new_data = (val_in, conv_unit, d['file_id'], d['sample_id'])
            insert.execute(
                'UPDATE processed_data set ' + var + ' = ?, ' + var + '_units = ? WHERE file_id = ? AND sample_id = ? AND ' + var + ' IS NULL',
                new_data)
        elif v in u_pc:
            conv_unit = 'mg m-2 d-1'
            new_data = (d['value'], conv_unit, d['file_id'], d['sample_id'])
            insert.execute(
                'UPDATE processed_data set ' + var + ' = mass_total * ?/100, ' + var + '_units = ? WHERE file_id = ? AND sample_id = ? AND ' + var + ' IS NULL',
                new_data)
        else:
            print('did not understand', v)

    con.commit()

    # ba_sd
    vars = var_interp[var_interp.column_name == var + '_sd']
    print(vars)
    var_names = set(vars.name)
    print(var + '_sd names', str(var_names)[1:-1])

    cur.execute(
        "SELECT data.file_id as file_id, sample_id, value, units FROM data JOIN variables v using (var_id) WHERE v.name IN (" + str(
            var_names)[1:-1] + ")")
    insert = con.cursor()

    for row in cur:
        d = dict(row)
        #    print(d)
        v = d['units']
        # print(v)
        if v in u_ug:
            try:
                val_in = d['value'] / 1000
            except TypeError:
                val_in = 'NA'
            new_data = (val_in, d['file_id'], d['sample_id'])
            insert.execute(
                'UPDATE processed_data set ' + var + ' = ? WHERE file_id = ? AND sample_id = ? AND ' + var + ' IS NULL',
                new_data)
        elif v in u_mmol:
            try:
                val_in = d['value'] * 137.327
            except TypeError:
                val_in = 'NA'
            new_data = (val_in, d['file_id'], d['sample_id'])
            insert.execute(
                'UPDATE processed_data set ' + var + ' = ? WHERE file_id = ? AND sample_id = ? AND ' + var + ' IS NULL',
                new_data)
        elif v in u_pc:
            new_data = (d['value'], d['file_id'], d['sample_id'])
            insert.execute(
                'UPDATE processed_data set ' + var + ' = mass_total * ?/100 WHERE file_id = ? AND sample_id = ? AND ' + var + ' IS NULL',
                new_data)
        else:
            print('did not understand', v)

    con.commit()

    # ba_qc

    vars = var_interp[var_interp.column_name == var + '_qc']
    print(vars)
    var_names = set(vars.name)
    print(var + '_qc names', str(var_names)[1:-1])

    cur.execute(
        "SELECT data.file_id as file_id, sample_id, value FROM data JOIN variables v using (var_id) WHERE v.name IN (" + str(
            var_names)[1:-1] + ")")
    insert = con.cursor()
    for row in cur:
        d = dict(row)
        new_data = (d['value'], d['file_id'], d['sample_id'])
        insert.execute('UPDATE processed_data set ' + var + '_qc = ? WHERE file_id = ? AND sample_id = ?', new_data)
    con.commit()


# to be deleted
def add_lithogenics_var(var_interp, dbname):
    var = 'lithogenic'
    con = sqlite3.connect(dbname, detect_types=sqlite3.PARSE_DECLTYPES)
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    u_mg = {'mg m-2 d-1', 'mg m-2 day-1', 'mg C m-2 d-1', 'mg/m2/d', 'mg/m2/day', 'mg m**2 d*1', 'mg m**2 day*1',
            'mg/m**2/day', 'mg/m**2/d'}

    u_ga = {'g/m**2/a'}

    u_pc = {'%', 'percent'}

    # lithogenic
    vars = var_interp[var_interp.column_name == var]
    print(vars)
    var_names = set(vars.name)
    print(var + ' names', str(var_names)[1:-1])

    # update the processed data table db for the variable
    cur.execute(
        "SELECT data.file_id as file_id, sample_id, value, units FROM data JOIN variables v using (var_id) WHERE v.name IN (" + str(
            var_names)[1:-1] + ")")
    insert = con.cursor()

    for row in cur:
        d = dict(row)
        #    print(d)
        v = d['units']
        # print(v)
        if v in u_mg:
            # print('inserted', d['value'], 'with unit ', v)
            new_data = (d['value'], d['units'], d['file_id'], d['sample_id'])
            insert.execute(
                'UPDATE processed_data set ' + var + ' = ?,' + var + '_units = ? WHERE file_id = ? AND sample_id = ?',
                new_data)
        elif v in u_ga:
            try:
                val_in = d['value'] * 1000 / 365
            except TypeError:
                val_in = 'NA'
            conv_unit = 'mg m-2 d-1'
            new_data = (val_in, conv_unit, d['file_id'], d['sample_id'])
            insert.execute(
                'UPDATE processed_data set ' + var + ' = ?,' + var + '_units = ? WHERE file_id = ? AND sample_id = ? AND ' + var + ' IS NULL',
                new_data)
        elif v in u_pc:
            conv_unit = 'mg m-2 d-1'
            new_data = (d['value'], conv_unit, d['file_id'], d['sample_id'])
            insert.execute(
                'UPDATE processed_data set ' + var + ' = mass_total * ?/100, ' + var + '_units = ? WHERE file_id = ? AND sample_id = ? AND ' + var + ' IS NULL',
                new_data)
        else:
            print('did not understand', v)

    con.commit()

    # lithogenic_sd
    vars = var_interp[var_interp.column_name == var + '_sd']
    print(vars)
    var_names = set(vars.name)
    print(var + '_sd names', str(var_names)[1:-1])

    cur.execute(
        "SELECT data.file_id as file_id, sample_id, value, units FROM data JOIN variables v using (var_id) WHERE v.name IN (" + str(
            var_names)[1:-1] + ")")
    insert = con.cursor()

    for row in cur:
        d = dict(row)
        #    print(d)
        v = d['units']
        # print(v)
        if v in u_mg:
            # print('inserted', d['value'], 'with unit ', v)
            new_data = (d['value'], d['file_id'], d['sample_id'])
            insert.execute('UPDATE processed_data set ' + var + '_sd = ? WHERE file_id = ? AND sample_id = ?', new_data)
        elif v in u_ga:
            try:
                val_in = d['value'] * 1000 / 365
            except TypeError:
                val_in = 'NA'
            new_data = (val_in, d['file_id'], d['sample_id'])
            insert.execute(
                'UPDATE processed_data set ' + var + '_sd = ? WHERE file_id = ? AND sample_id = ? AND ' + var + ' IS NULL',
                new_data)
        elif v in u_pc:
            new_data = (d['value'], d['file_id'], d['sample_id'])
            insert.execute(
                'UPDATE processed_data set ' + var + '_sd = mass_total * ?/100 WHERE file_id = ? AND sample_id = ? AND ' + var + ' IS NULL',
                new_data)
        else:
            print('did not understand', v)

    con.commit()


# to be deleted
def add_detrital_var(var_interp, dbname):
    var = 'detrital'
    con = sqlite3.connect(dbname, detect_types=sqlite3.PARSE_DECLTYPES)
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    u_mg = {'mg m-2 d-1', 'mg m-2 day-1', 'mg C m-2 d-1', 'mg/m2/d', 'mg/m2/day', 'mg m**2 d*1', 'mg m**2 day*1',
            'mg/m**2/day', 'mg/m**2/d'}

    u_ga = {'g/m**2/a'}

    u_pc = {'%', 'percent'}

    # detrital
    vars = var_interp[var_interp.column_name == var]
    print(vars)
    var_names = set(vars.name)
    print(var + ' names', str(var_names)[1:-1])

    # update the processed data table db for the variable
    cur.execute(
        "SELECT data.file_id as file_id, sample_id, value, units FROM data JOIN variables v using (var_id) WHERE v.name IN (" + str(
            var_names)[1:-1] + ")")
    insert = con.cursor()

    for row in cur:
        d = dict(row)
        #    print(d)
        v = d['units']
        # print(v)
        if v in u_mg:
            # print('inserted', d['value'], 'with unit ', v)
            new_data = (d['value'], d['units'], d['file_id'], d['sample_id'])
            insert.execute(
                'UPDATE processed_data set ' + var + ' = ?,' + var + '_units = ? WHERE file_id = ? AND sample_id = ?',
                new_data)
        elif v in u_ga:
            try:
                val_in = d['value'] * 1000 / 365
            except TypeError:
                val_in = 'NA'
            conv_unit = 'mg m-2 d-1'
            new_data = (val_in, conv_unit, d['file_id'], d['sample_id'])
            insert.execute(
                'UPDATE processed_data set ' + var + ' = ?,' + var + '_units = ? WHERE file_id = ? AND sample_id = ? AND ' + var + ' IS NULL',
                new_data)
        elif v in u_pc:
            conv_unit = 'mg m-2 d-1'
            new_data = (d['value'], conv_unit, d['file_id'], d['sample_id'])
            insert.execute(
                'UPDATE processed_data set ' + var + ' = mass_total * ?/100, ' + var + '_units = ? WHERE file_id = ? AND sample_id = ? AND ' + var + ' IS NULL',
                new_data)
        else:
            print('did not understand', v)

    con.commit()

    # detrital_sd
    vars = var_interp[var_interp.column_name == var + '_sd']
    print(vars)
    var_names = set(vars.name)
    print(var + '_sd names', str(var_names)[1:-1])

    cur.execute(
        "SELECT data.file_id as file_id, sample_id, value, units FROM data JOIN variables v using (var_id) WHERE v.name IN (" + str(
            var_names)[1:-1] + ")")
    insert = con.cursor()

    for row in cur:
        d = dict(row)
        #    print(d)
        v = d['units']
        # print(v)
        if v in u_mg:
            # print('inserted', d['value'], 'with unit ', v)
            new_data = (d['value'], d['file_id'], d['sample_id'])
            insert.execute('UPDATE processed_data set ' + var + '_sd = ? WHERE file_id = ? AND sample_id = ?', new_data)
        elif v in u_ga:
            try:
                val_in = d['value'] * 1000 / 365
            except TypeError:
                val_in = 'NA'
            new_data = (val_in, d['file_id'], d['sample_id'])
            insert.execute(
                'UPDATE processed_data set ' + var + '_sd = ? WHERE file_id = ? AND sample_id = ? AND ' + var + ' IS NULL',
                new_data)
        elif v in u_pc:
            new_data = (d['value'], d['file_id'], d['sample_id'])
            insert.execute(
                'UPDATE processed_data set ' + var + '_sd = mass_total * ?/100 WHERE file_id = ? AND sample_id = ? AND ' + var + ' IS NULL',
                new_data)
        else:
            print('did not understand', v)

    con.commit()


# to be deleted
def add_ti_var(var_interp, dbname):
    var = 'ti'
    con = sqlite3.connect(dbname, detect_types=sqlite3.PARSE_DECLTYPES)
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    u_ug = {'µg/m**2/day'}

    u_uga = {'µg/cm**2/a'}

    u_pc = {'%', 'percent'}

    # ti
    vars = var_interp[var_interp.column_name == var]
    print(vars)
    var_names = set(vars.name)
    print(var + ' names', str(var_names)[1:-1])

    # update the processed data table db for the variable
    cur.execute(
        "SELECT data.file_id as file_id, sample_id, value, units FROM data JOIN variables v using (var_id) WHERE v.name IN (" + str(
            var_names)[1:-1] + ")")
    insert = con.cursor()

    for row in cur:
        d = dict(row)
        #    print(d)
        v = d['units']
        # print(v)
        if v in u_ug:
            try:
                val_in = d['value'] / 1000
            except TypeError:
                val_in = 'NA'
            conv_unit = 'mg m-2 d-1'
            new_data = (val_in, conv_unit, d['file_id'], d['sample_id'])
            insert.execute(
                'UPDATE processed_data set ' + var + ' = ?, ' + var + '_units = ? WHERE file_id = ? AND sample_id = ? AND ' + var + ' IS NULL',
                new_data)
        elif v in u_uga:
            try:
                val_in = d['value'] / 1000 / 10000 / 365
            except TypeError:
                val_in = 'NA'
            conv_unit = 'mg m-2 d-1'
            new_data = (val_in, conv_unit, d['file_id'], d['sample_id'])
            insert.execute(
                'UPDATE processed_data set ' + var + ' = ?, ' + var + '_units = ? WHERE file_id = ? AND sample_id = ? AND ' + var + ' IS NULL',
                new_data)
        elif v in u_pc:
            conv_unit = 'mg m-2 d-1'
            new_data = (d['value'], conv_unit, d['file_id'], d['sample_id'])
            insert.execute(
                'UPDATE processed_data set ' + var + ' = mass_total * ?/100, ' + var + '_units = ? WHERE file_id = ? AND sample_id = ? AND ' + var + ' IS NULL',
                new_data)
        else:
            print('did not understand', v)

    con.commit()

    # ti_sd
    vars = var_interp[var_interp.column_name == var + '_sd']
    print(vars)
    var_names = set(vars.name)
    print(var + '_sd names', str(var_names)[1:-1])

    cur.execute(
        "SELECT data.file_id as file_id, sample_id, value, units FROM data JOIN variables v using (var_id) WHERE v.name IN (" + str(
            var_names)[1:-1] + ")")
    insert = con.cursor()

    for row in cur:
        d = dict(row)
        #    print(d)
        v = d['units']
        # print(v)
        if v in u_ug:
            try:
                val_in = d['value'] / 1000
            except TypeError:
                val_in = 'NA'
            new_data = (val_in, d['file_id'], d['sample_id'])
            insert.execute(
                'UPDATE processed_data set ' + var + ' = ? WHERE file_id = ? AND sample_id = ? AND ' + var + ' IS NULL',
                new_data)
        elif v in u_uga:
            try:
                val_in = d['value'] / 1000 / 10000 / 365
            except TypeError:
                val_in = 'NA'
            new_data = (val_in, d['file_id'], d['sample_id'])
            insert.execute(
                'UPDATE processed_data set ' + var + ' = ?, ' + var + '_units = ? WHERE file_id = ? AND sample_id = ? AND ' + var + ' IS NULL',
                new_data)
        elif v in u_pc:
            new_data = (d['value'], d['file_id'], d['sample_id'])
            insert.execute(
                'UPDATE processed_data set ' + var + ' = mass_total * ?/100 WHERE file_id = ? AND sample_id = ? AND ' + var + ' IS NULL',
                new_data)
        else:
            print('did not understand', v)

    con.commit()

    # ti_qc

    vars = var_interp[var_interp.column_name == var + '_qc']
    print(vars)
    var_names = set(vars.name)
    print(var + '_qc names', str(var_names)[1:-1])

    cur.execute(
        "SELECT data.file_id as file_id, sample_id, value FROM data JOIN variables v using (var_id) WHERE v.name IN (" + str(
            var_names)[1:-1] + ")")
    insert = con.cursor()
    for row in cur:
        d = dict(row)
        new_data = (d['value'], d['file_id'], d['sample_id'])
        insert.execute('UPDATE processed_data set ' + var + '_qc = ? WHERE file_id = ? AND sample_id = ?', new_data)
    con.commit()


def add_reference_var(dbname):
    var = 'Reference'
    con = sqlite3.connect(dbname, detect_types=sqlite3.PARSE_DECLTYPES)
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    # reference
    # update the processed data table db for the variable
    cur.execute(
        "SELECT data.file_id as file_id, sample_id, value FROM data JOIN variables v using (var_id) WHERE v.name = '" + var + "'")
    insert = con.cursor()

    for row in cur:
        d = dict(row)
        new_data = (d['value'], d['file_id'], d['sample_id'])
        insert.execute('UPDATE processed_data set ' + var + '= ? WHERE file_id = ? AND sample_id = ?',
                       new_data)

    con.commit()


def add_comments_var(var_interp, dbname):
    var = 'comments'
    con = sqlite3.connect(dbname, detect_types=sqlite3.PARSE_DECLTYPES)
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    # comments
    vars = var_interp[var_interp.column_name == var]
    print(vars)
    var_names = set(vars.name)
    print(var + ' names', str(var_names)[1:-1])

    # update the processed data table db for the variable
    cur.execute(
        "SELECT data.file_id as file_id, sample_id, value, units FROM data JOIN variables v using (var_id) WHERE v.name IN (" + str(
            var_names)[1:-1] + ")")
    insert = con.cursor()

    for row in cur:
        d = dict(row)
        #    print(d)
        new_data = (d['value'], d['file_id'], d['sample_id'])
        insert.execute(
            'UPDATE processed_data set ' + var + '= ? WHERE file_id = ? AND sample_id = ? AND ' + var + ' IS NULL',
            new_data)
    con.commit()


def add_doi(dbname):
    var = 'doi'
    con = sqlite3.connect(dbname, detect_types=sqlite3.PARSE_DECLTYPES)
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    # doi
    # update the processed data table db for the variable
    cur.execute(
        "SELECT file_metadata.file_id as file_id, name, value FROM file_metadata WHERE file_metadata.name = '" + var + "'")
    insert = con.cursor()

    for row in cur:
        d = dict(row)
        new_data = (d['value'], d['file_id'])
        insert.execute('UPDATE processed_data set ' + var + '= ? WHERE file_id = ?', new_data)

    con.commit()


def add_url(dbname):
    var = 'url'
    con = sqlite3.connect(dbname, detect_types=sqlite3.PARSE_DECLTYPES)
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    # url
    # update the processed data table db for the variable
    cur.execute(
        "SELECT file_metadata.file_id as file_id, name, value FROM file_metadata WHERE file_metadata.name = '" + var + "'")
    insert = con.cursor()

    for row in cur:
        d = dict(row)
        new_data = (d['value'], d['file_id'])
        insert.execute('UPDATE processed_data set ' + var + '= ? WHERE file_id = ?', new_data)

    con.commit()


def add_citation(dbname):
    var = 'citation'
    con = sqlite3.connect(dbname, detect_types=sqlite3.PARSE_DECLTYPES)
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    # update the processed data table db for the variable
    cur.execute(
        "SELECT file_metadata.file_id as file_id, name, value FROM file_metadata WHERE file_metadata.name = '" + var + "'")
    insert = con.cursor()

    for row in cur:
        d = dict(row)
        new_data = (d['value'], d['file_id'])
        insert.execute('UPDATE processed_data set ' + var + '= ? WHERE file_id = ?', new_data)

    con.commit()


def add_date_downloaded(dbname):
    var = 'date_downloaded'
    con = sqlite3.connect(dbname, detect_types=sqlite3.PARSE_DECLTYPES)
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    # update the processed data table db for the variable
    cur.execute(
        "SELECT file_metadata.file_id as file_id, name, value FROM file_metadata WHERE file_metadata.name = '" + var + "'")
    insert = con.cursor()

    for row in cur:
        d = dict(row)
        new_data = (d['value'], d['file_id'])
        insert.execute('UPDATE processed_data set ' + var + '= ? WHERE file_id = ?', new_data)

    con.commit()


if __name__ == "__main__":
#    fn = 'Pangaea_wanted_variables_2.csv'
#    var_interp = pd.read_csv(fn, encoding="ISO-8859-1")
#    filename = 'conversion_factors.csv'
#    var_dict = pd.read_csv(filename).to_dict()

    u_mg = {'mg m-2 d-1', 'mg m-2 day-1', 'mg C m-2 d-1', 'mg N m-2 d-1', 'mg Chl a m-2 d-1',
            'mg Chl equivalents a m-2 d-1', 'mg/m2/d', 'mg/m2/day', 'mg m**2 d*1', 'mg m**2 day*1',
            'mg/m**2/day', 'mg/m**2/d', 'milligram/m2/day', 'mg_m^-2_d^-1'}

    u_ug = {'µg/m**2/day', 'ug/m2/day', 'ug chl a m-2 d-1'}

    u_pmol = {'pmol/m**2/day'}

    u_uga = {'µg/cm**2/a'}

    u_mmol = {'mmol m-2 d-1', 'mmol/m**2/day', 'mmol/m2/day', 'mmol/m**2/d', 'mmol_m^-2_d^-1'}

    u_umol = {'umol/m**2/d'}

    u_ga = {'g/m**2/a'}

    u_gd = {'g/m**2/day'}

    u_ugcma = {'µg/cm**2/a'}

    u_mola = {'mol/m**2/a'}

    u_umolcma = {'µmol/cm**2/a'}

    u_pc = {'%', 'percent'}

    u_plus_minus = {'±'}

    u_day = {'days', 'day', 'unitless', 'number of days', 'd'}

    u_hours = {'h', 'hours'}

    u_m = {'m', 'Meters'}

    u_lat = {'degrees', 'deg', 'unitless', 'decimal degrees', 'degrees_north', 'degrees North'}

    u_lon = {'degrees', 'deg', 'unitless', 'decimal degrees', 'degrees_east', 'degrees East'}

    vars_4_inclusion = {'duration', 'latitude', 'longitude', 'depth', 'mass_total', 'carbon_total', 'poc', 'poc', 'pon',
                        'pop', 'opa', 'psi', 'psio2', 'psi_oh_4', 'pal', 'chl', 'pheop', 'caco3', 'ca', 'fe', 'mn',
                        'ba', 'lithogenic', 'detrital', 'ti'}

    # print(dict_from_csv)

    create_processed_data_db()
#    add_timestamp_var(var_interp, dbname)
    # add_duration_var(var_interp, dbname)
#    add_time_recovered_var(var_interp, dbname)
#    add_time_mid_var(var_interp, dbname)
    # add_lat_var(var_interp, dbname)
    # add_lon_var(var_interp, dbname)
    # add_depth_var(var_interp, dbname)
    # add_mass_total_var(var_interp, dbname)
    # add_carbon_total_var(var_interp, dbname)
    # add_poc_var(var_interp, dbname)
    # add_pic_var(var_interp, dbname)
    # add_pon_var(var_interp, dbname)
    # add_pop_var(var_interp, dbname)
    # add_opal_var(var_interp, dbname)
    # add_psi_var(var_interp, dbname)
    # add_psio2_var(var_interp, dbname)
    # add_psioh4_var(var_interp, dbname)
    # add_pal_var(var_interp, dbname)
    # add_chl_var(var_interp, dbname)
    # add_pheop_var(var_interp, dbname)
    # add_caco3_var(var_interp, dbname)
    # add_ca_var(var_interp, dbname)
    # add_fe_var(var_interp, dbname)
    # add_mn_var(var_interp, dbname)
    # add_ba_var(var_interp, dbname)
    # add_lithogenics_var(var_interp, dbname)
    # add_detrital_var(var_interp, dbname)
    # add_ti_var(var_interp, dbname)
#    add_reference_var(dbname)
#    add_comments_var(var_interp, dbname)
#    add_doi(dbname)
#    add_url(dbname)
#    add_citation(dbname)
#    add_date_downloaded(dbname)



