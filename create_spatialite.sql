SELECT load_extension('/opt/homebrew/lib/mod_spatialite.dylib')

drop view geom;
SELECT InitSpatialMetadata(1)

CREATE VIEW geom AS SELECT file.file_id, fm.value, file.mintimeextent, file.maxtimeextent, GeomFromText(concat('POINT(',file.meanLongitude, ' ', file.meanLatitude,')'), 4326) AS geometry FROM file JOIN file_metadata fm on(fm.file_id = file.file_id and fm.name = 'title');

SELECT file.file_id, fm.value, file.mintimeextent, file.maxtimeextent, concat('POINT(',file.meanLongitude, ' ', file.meanLatitude,')'), 4326 AS geometry FROM file JOIN file_metadata fm on(fm.file_id = file.file_id and fm.name = 'title');

INSERT INTO geometry_columns(f_table_name, f_geometry_column, geometry_type, coord_dimension, srid, spatial_index_enabled) VALUES('geom', 'geometry', 1, 2, 4326, 1);

SELECT CreateSpatialIndex('geom', 'geometry');

SELECT * from geom ;

drop table places_spatialite;

CREATE TABLE places_spatialite (id integer primary key, name text, timemin REAL, timemax REAL);

SELECT AddGeometryColumn('places_spatialite', 'geometry', 4326, 'POINT', 'XY')

delete from places_spatialite ;

INSERT INTO places_spatialite (id, name, timemin, timemax, geometry) SELECT * FROM geom;

SELECT * from places_spatialite ps ;
