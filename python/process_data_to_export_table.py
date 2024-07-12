import sqlite3
import pandas as pd


def create_processed_data_db(dbname):
    con = sqlite3.connect(dbname, detect_types=sqlite3.PARSE_DECLTYPES)
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    # create the database table to take the data
    cur.execute("CREATE TABLE IF NOT EXISTS processed_data"
                "(file_id integer, sample_id integer, "
                "timestamp REAL, timestamp_units REAL, duration REAL, duration_units REAL, time_deployed REAL, time_deployed_units REAL,"
                "time_recovered REAL, time_recovered_units REAL, time_mid REAL, time_mid_units REAL, latitude REAL, "
                "latitude_units REAL, longitude REAL, longitude_units REAL, data_type TEXT, instrument_descriptor TEXT, instrument_collector_size REAL,"
                "bathymetry_depth_ETOPO1 REAL, depth REAL, depth_units REAL, mass_total REAL, mass_total_units_raw TEXT, "
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
                "detrital_qc TEXT, ti REAL, ti_units_raw TEXT, ti_units TEXT, ti_sd REAL, ti_qc TEXT, source TEXT, "
                "comments TEXT, doi TEXT, citation TEXT, date_downloaded REAL, sst REAL, "
                "mixed_layer_depth REAL, chl_tot_gsm REAL, npp_c REAL, kd490 REAL, z_eu REAL, par REAL, sfm REAL, "
                "CONSTRAINT FK_data_var_id CONSTRAINT FK_data_file_id "
                "FOREIGN KEY(file_id) REFERENCES file(file_id))")

    cur.execute("CREATE INDEX processed_data_file_id_IDX ON 'processed_data' (file_id)")
#    cur.execute("CREATE INDEX processed_data_var_id_IDX ON 'processed_data' (var_id)")
    cur.execute("CREATE INDEX processed_data_sample_IDX ON 'processed_data' (sample_id)")
    con.commit()

def populate_file_id(dbname):
    con = sqlite3.connect(dbname, detect_types=sqlite3.PARSE_DECLTYPES)
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    # start populating the table with file_id and sample_id to get started
    # cur.execute(
    #     "SELECT data.file_id as file_id, sample_id, var_id FROM data GROUP BY file_id, sample_id, var_id ORDER BY file_id, sample_id, var_id")
    cur.execute(
        "SELECT data.file_id as file_id, sample_id FROM data GROUP BY file_id, sample_id ORDER BY file_id, sample_id")

    insert = con.cursor()

    for row in cur:
        d = dict(row)
        print(d)

        # new_data = (d['file_id'], d['sample_id'], d['var_id'])
        new_data = (d['file_id'], d['sample_id'])
        # insert.execute('INSERT INTO processed_data (file_id, sample_id, var_id) VALUES (?,?,?)', new_data)
        insert.execute('INSERT INTO processed_data (file_id, sample_id) VALUES (?,?)', new_data)
    con.commit()

def convert_units_mg_p_day(value, units_in, conversion_factor):
    if units_in in u_mg or units_in in u_lat or units_in in u_lon or units_in in u_day or units_in in u_m:
        val_in = value
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
    elif units_in in u_plus_minus:
        try:
            val_in = value
        except TypeError:
            val_in = 'NA'
    else:
        print('did not understand : ', units_in, ' unit')
        val_in = value

    return val_in

def add_times_var(var_interp, dbname):
    time_vars = {'timestamp', 'time_mid', 'time_recovered', 'time_deployed'}
    for var in time_vars:
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
                # cur.execute("SELECT data.file_id as file_id, sample_id, value, units, var_id FROM data JOIN variables using (var_id) JOIN file_variables using (var_id) WHERE variables.name = '" + v + "'")
                cur.execute(
                    "SELECT data.file_id as file_id, data.sample_id as sample_id, data.value as value, data.var_id as var_id, "
                    "file_variables.units as units FROM data JOIN file_variables using (var_id) JOIN variables using (var_id) WHERE variables.name = '" + v + "'")
                insert = con.cursor()

                for row in cur:
                    d = dict(row)
                    vu = d['units']
                    print(v, d)
                    # continue

                    new_data = (d['value'], d['units'], d['file_id'], d['sample_id'])
                    insert.execute(
                        "UPDATE processed_data set '" + var + "'= ?,'" + var + "_units' = ? WHERE file_id = ? AND sample_id = ?",
                        new_data)

                con.commit()
            except sqlite3.OperationalError as e:
                print(e)

def add_lat_lon(var_interp, dbname):
    geo_vars = {'latitude', 'longitude'}
    con = sqlite3.connect(dbname, detect_types=sqlite3.PARSE_DECLTYPES)
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    for var in geo_vars:

        vars = var_interp[var_interp.column_name == var]
        print(vars)
        var_names = set(vars.name)
        print(var + ' names', str(var_names)[1:-1])

        # # update the processed data table db
        for v in var_names:
            print('working on ', v)
            # update the processed data table db
            try:
                # cur.execute("SELECT data.file_id as file_id, sample_id, value, units, var_id FROM data JOIN variables using (var_id) JOIN file_variables using (var_id) WHERE variables.name = '" + v + "'")
                cur.execute(
                    "SELECT data.file_id as file_id, data.sample_id as sample_id, data.value as value, data.var_id as var_id, "
                    "file_variables.units as units FROM data JOIN file_variables using (var_id) JOIN variables using (var_id) WHERE variables.name = '" + v + "'")
                insert = con.cursor()

                for row in cur:
                    d = dict(row)
                    vu = d['units']
                    print(v, d)
                    # continue

                    new_data = (d['value'], d['units'], d['file_id'], d['sample_id'])
                    insert.execute(
                        "UPDATE processed_data set '" + var + "'= ?,'" + var + "_units' = ? WHERE file_id = ? AND sample_id = ?",
                        new_data)

                con.commit()
            except sqlite3.OperationalError as e:
                print(e)

def add_variables(var, var_interp, dbname, var_calculations):
    var_dict = var_calculations.loc[var_calculations['varname'] == var]
    conv_factor = var_dict['conversion_factor'].values[0]
    conv_unit = var_dict['final_unit'].values[0]

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
            # cur.execute("SELECT data.file_id as file_id, sample_id, value, units, var_id FROM data JOIN variables using (var_id) JOIN file_variables using (var_id) WHERE variables.name = '" + v + "'")
            cur.execute(
                "SELECT data.file_id as file_id, data.sample_id as sample_id, data.value as value, data.var_id as var_id, "
                "file_variables.units as units FROM data JOIN file_variables using (var_id) JOIN variables using (var_id) WHERE variables.name = '" + v + "'")
            insert = con.cursor()

            for row in cur:
                d = dict(row)
                vu = d['units']
                print(v, d)
                #continue
                try:
                    if conv_unit is None:
                        conv_unit = vu
                except AttributeError:
                    pass
                try:
                    if conv_factor is None:
                        conv_factor = 1
                except AttributeError:
                    pass

                if vu in u_pc:
                    new_data = (d['value'], conv_unit, vu, d['file_id'], d['sample_id'])
                    # insert.execute(
                    #     "UPDATE processed_data set '" + var + "' = mass_total * ?/100, '" + var + "_units' = ?, '" + var + "_units_raw' = ? WHERE file_id = ? AND sample_id = ? AND var_id = ? AND '" + var + "' IS NULL",
                    #     new_data)
                    insert.execute(
                        "UPDATE processed_data set '" + var + "' = mass_total * ?/100, '" + var + "_units' = ?, '" + var + "_units_raw' = ? WHERE file_id = ? AND sample_id = ? AND '" + var + "'",
                        new_data)

                else:
                    val_in = convert_units_mg_p_day(d['value'], d['units'], conv_factor)
                    new_data = (val_in, conv_unit, vu, d['file_id'], d['sample_id'])
                    # insert.execute(
                    #     "UPDATE processed_data set '" + var + "'= ?,'" + var + "_units' = ?, '" + var + "_units_raw' = ? WHERE file_id = ? AND sample_id = ? AND var_id = ? AND '" + var + "' IS NULL",
                    #     new_data)
                    insert.execute(
                        "UPDATE processed_data set '" + var + "'= ?,'" + var + "_units' = ?, '" + var + "_units_raw' = ? WHERE file_id = ? AND sample_id = ?",
                        new_data)

            con.commit()
        except sqlite3.OperationalError as e:
            print(e)

    # SD
    vars = var_interp[var_interp.column_name == var + '_sd']
    print(vars)
    if vars is not None:
        var_names = set(vars.name)
        print(var + '_sd names', str(var_names)[1:-1])
        # update the processed data table db for the variable
        for v in var_names:
            print('working on ', v)
            # update the processed data table db
            try:
                # cur.execute(
                #     "SELECT data.file_id as file_id, sample_id, value, units, var_id FROM data JOIN variables using (var_id) JOIN file_variables using (var_id) WHERE variables.name = '" + v + "'")
                cur.execute(
                    "SELECT data.file_id as file_id, data.sample_id as sample_id, data.value as value, data.var_id as var_id, "
                    "file_variables.units as units FROM data JOIN file_variables using (var_id) JOIN variables using (var_id) WHERE variables.name = '" + v + "'")
                insert = con.cursor()

                for row in cur:
                    d = dict(row)
                    vu = d['units']
                    print(v, d)
                    # get the sample units
                    get_units = con.cursor()
                    unit_file_id = (d['file_id'], d['var_id'])
                    get_units.execute("SELECT units FROM file_variables WHERE file_id = ? AND var_id = ?", unit_file_id)
                    units_raw = get_units.fetchone()
                    var_units = dict(units_raw)['units']
                    #print(vu)
                    if vu in u_pc:
                        new_data = (d['value'], d['file_id'], d['sample_id'])
                        insert.execute(
                            "UPDATE processed_data set '" + var + "_sd' = mass_total * ?/100 WHERE file_id = ? AND sample_id = ?",
                            new_data)

                    elif v in u_plus_minus:
                        val_in = convert_units_mg_p_day(d['value'], var_units, conv_factor)
                        new_data = (val_in, d['file_id'], d['sample_id'])
                        insert.execute(
                            "UPDATE processed_data set '" + var + "_sd' = mass_total * ?/100 WHERE file_id = ? AND sample_id = ?",
                            new_data)
                    else:
                        val_in = convert_units_mg_p_day(d['value'], d['units'], conv_factor)
                        new_data = (val_in, d['file_id'], d['sample_id'])
                        insert.execute(
                            "UPDATE processed_data set '" + var + "_sd'= ? WHERE file_id = ? AND sample_id = ?",
                            new_data)
                con.commit()
            except sqlite3.OperationalError as e:
                print(e)
                continue

    # QC

    vars = var_interp[var_interp.column_name == var + '_qc']
    print(vars)
    if vars is not None:
        var_names = set(vars.name)
        print(var + '_qc names', str(var_names)[1:-1])
        # update the processed data table db for the variable
        for v in var_names:
            print('working on ', v)
            # update the processed data table db
            try:
                # cur.execute(
                #     "SELECT data.file_id as file_id, sample_id, value, var_id FROM data JOIN variables using (var_id) JOIN file_variables using (var_id) WHERE variables.name = '" + v + "'")
                cur.execute(
                    "SELECT data.file_id as file_id, data.sample_id as sample_id, data.value as value, data.var_id as var_id FROM data JOIN variables using (var_id) WHERE variables.name = '" + v + "'")

                insert = con.cursor()

                for row in cur:
                    d = dict(row)
                    new_data = (d['value'], d['file_id'], d['sample_id'])
                    insert.execute(
                        "UPDATE processed_data set '" + var + "_qc' = ? WHERE file_id = ? AND sample_id = ?",
                        new_data)
                con.commit()
            except sqlite3.OperationalError as e:
                print(e)
                continue

def add_reference_var(dbname):
    con = sqlite3.connect(dbname, detect_types=sqlite3.PARSE_DECLTYPES)
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    # reference
    # update the processed data table db
    try:
        cur.execute(
            "SELECT file.file_id as file_id, file.citation as citation, file.doi as doi FROM file")
        insert = con.cursor()

        for row in cur:
            d = dict(row)
            new_data = (d['citation'], d['doi'], d['file_id'])
            insert.execute("UPDATE processed_data.citation = ?, processed_data.doi = ?"
                           "WHERE processed_data.file_id = ?", new_data)
        con.commit()
    except sqlite3.OperationalError as e:
        print(e)



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
    dbname = r'C:\Users\wyn028\OneDrive - CSIRO\Manuscripts\Particle_flux_database\DATA_Mining\test\test_sed_data.sqlite'
    fn = r'Pangaea_wanted_variables_2.csv'
    var_interp = pd.read_csv(fn, encoding="ISO-8859-1")
    filename = r'conversion_factors.csv'
    var_calculations = pd.read_csv(filename)

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

    u_unknown = ''

    vars_4_inclusion = {'duration', 'latitude', 'longitude', 'depth', 'mass_total', 'carbon_total', 'poc', 'poc', 'pon',
                        'pop', 'opa', 'psi', 'psio2', 'psi_oh_4', 'pal', 'chl', 'pheop', 'caco3', 'ca', 'fe', 'mn',
                        'ba', 'lithogenic', 'detrital', 'ti'}

#    create_processed_data_db(dbname)
    populate_file_id(dbname)
    add_times_var(var_interp, dbname)
    add_lat_lon(var_interp, dbname)
    var = ('mass_total', 'duration', 'carbon_total', 'poc', 'pic', 'pon', 'pop', 'opal', 'psi', 'psio2',
            'psi_oh_4', 'pal', 'chl', 'pheop', 'caco3', 'ca', 'fe', 'mn', 'ba', 'lithogenics', 'detrital',
            'ti', 'timestamp', 'time_deployed', 'time_recovered', 'time_mid')
    for vv in var:
        add_variables(vv, var_interp, dbname, var_calculations)

#    add_reference_var(dbname)
#    add_comments_var(var_interp, dbname)
#    add_doi(dbname)
#    add_url(dbname)
#    add_citation(dbname)
#    add_data_type
#    add_instrument_type
#    add_instrument_collector_size
#    add_bathymetry
#    add_sst
#    add_mixed_layer_depth
#    add_chl_tot_gsm
#    add_npp_c
#    add_kd490
#    add_z_eu
#    add_par
#    add_sfm
#    add_date_downloaded(dbname)



