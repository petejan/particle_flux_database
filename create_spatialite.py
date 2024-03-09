import sqlite3

SPATIALITE = "/opt/homebrew/lib/mod_spatialite.dylib"

db = sqlite3.connect("sediment_data.sqlite")
db.enable_load_extension(True)
db.execute("SELECT load_extension(?)", [SPATIALITE])
db.execute("SELECT InitSpatialMetadata(1)")
db.execute("CREATE VIEW places_spatialite (id integer primary key, name text)")
db.execute("SELECT AddGeometryColumn('places_spatialite', 'geometry', 4326, 'POINT', 'XY');")

# Then to add a spatial index:
db.execute("SELECT CreateSpatialIndex('places_spatialite', 'geometry');")